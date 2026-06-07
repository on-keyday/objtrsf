//go:build js

package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"sync"
	"syscall/js"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
)

// WebSocketEndpoint constructs a WebSocket-backed objproto Endpoint in the
// mode specified by cfg.Mode. The wasm build supports Client mode only;
// Server / Mutual return an error because the browser environment cannot
// listen for incoming WS connections.
func WebSocketEndpoint(mux *http.ServeMux, cfg WebSocketConfig) (objproto.Endpoint, error) {
	rawSess := objproto.NewEndpoint(cfg.Logger, cfg.Mode)
	if err := WebSocketEndpointEx(rawSess, mux, cfg, nil); err != nil {
		return nil, err
	}
	return rawSess, nil
}

// WebSocketEndpointEx is the lower-level variant for callers that already
// own a RawEndpoint. wasm build supports Client mode only.
//
// sendTo mirrors the native variant: nil ⇒ rawSess.GetSenderChannel(),
// non-nil ⇒ dedicated channel (not used in WASM since dualstack with UDP
// is unreachable from the browser, but accepted for signature symmetry).
func WebSocketEndpointEx(rawSess objproto.RawEndpoint, mux *http.ServeMux, cfg WebSocketConfig, sendTo <-chan *objproto.PacketData) error {
	if cfg.Mode != objproto.EndpointModeClient {
		return fmt.Errorf("websocket_wasm: only Client mode is supported (got %v)", cfg.Mode)
	}
	if mux != nil {
		return errors.New("websocket_wasm: mux must be nil in wasm Client mode")
	}

	transportName := "ws"
	if cfg.TLS != nil {
		transportName = "wss"
	}

	var connsMu sync.Mutex
	conns := make(map[netip.AddrPort]*wasmWSConn)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// sender goroutine: drain sendTo (the dedicated channel or, when
	// caller passed nil, rawSess.GetSenderChannel()) and route packets to
	// the right connection. Handshake packets to unknown peers trigger a
	// fresh dial (only Client/Mutual reach this branch per upstream
	// SendHandshake invariant).
	if sendTo == nil {
		sendTo = rawSess.GetSenderChannel()
	}
	go func() {
		for pkt := range sendTo {
			connsMu.Lock()
			conn, ok := conns[pkt.To.Addr]
			connsMu.Unlock()

			if !ok {
				if pkt.Kind != packet.PacketKind_Handshake {
					logger.Error("no websocket connection for address",
						slog.String("address", pkt.To.String()))
					rawSess.CannotSend(pkt)
					continue
				}
				go dialAndSend(rawSess, transportName, &connsMu, conns, pkt, cfg, logger)
				continue
			}
			if err := conn.send(pkt.Data); err != nil {
				logger.Error("ws send failed",
					slog.String("to", pkt.To.String()),
					slog.String("err", err.Error()))
				rawSess.CannotSend(pkt)
				connsMu.Lock()
				delete(conns, pkt.To.Addr)
				connsMu.Unlock()
				conn.close()
			}
		}
	}()

	return nil
}

// wasmWSConn wraps a JS WebSocket value with the same Send / Receive shape
// used internally by the sender / receiver goroutines.
type wasmWSConn struct {
	ws         js.Value
	remoteAddr netip.AddrPort

	incoming chan []byte
	closed   chan struct{}

	cleanupMu sync.Mutex
	cleanedUp bool
	releases  []js.Func
}

func dialAndSend(
	rawSess objproto.RawEndpoint,
	transportName string,
	connsMu *sync.Mutex,
	conns map[netip.AddrPort]*wasmWSConn,
	pkt *objproto.PacketData,
	cfg WebSocketConfig,
	logger *slog.Logger,
) {
	scheme := "ws"
	if cfg.TLS != nil {
		scheme = "wss"
	}
	url := fmt.Sprintf("%s://%s%s", scheme, pkt.To.Addr.String(), cfg.Path)

	ws := js.Global().Get("WebSocket").New(url)
	ws.Set("binaryType", "arraybuffer")

	openCh := make(chan struct{})
	errCh := make(chan struct{}, 1)

	var releases []js.Func
	addListener := func(event string, fn func(this js.Value, args []js.Value) any) js.Func {
		f := js.FuncOf(fn)
		ws.Call("addEventListener", event, f)
		releases = append(releases, f)
		return f
	}

	addListener("open", func(this js.Value, args []js.Value) any {
		select {
		case <-openCh:
		default:
			close(openCh)
		}
		return nil
	})
	addListener("error", func(this js.Value, args []js.Value) any {
		select {
		case errCh <- struct{}{}:
		default:
		}
		return nil
	})

	select {
	case <-openCh:
	case <-errCh:
		logger.Error("ws dial failed", slog.String("addr", pkt.To.Addr.String()))
		for _, f := range releases {
			f.Release()
		}
		rawSess.CannotSend(pkt)
		return
	}

	conn := &wasmWSConn{
		ws:         ws,
		remoteAddr: pkt.To.Addr,
		incoming:   make(chan []byte, 16),
		closed:     make(chan struct{}),
		releases:   releases,
	}

	addListener("message", func(this js.Value, args []js.Value) any {
		evt := args[0]
		data := evt.Get("data") // ArrayBuffer (binaryType = "arraybuffer")
		u8 := js.Global().Get("Uint8Array").New(data)
		buf := make([]byte, u8.Length())
		js.CopyBytesToGo(buf, u8)
		select {
		case conn.incoming <- buf:
		case <-conn.closed:
		}
		return nil
	})
	addListener("close", func(this js.Value, args []js.Value) any {
		conn.markClosed()
		return nil
	})

	connsMu.Lock()
	conns[pkt.To.Addr] = conn
	connsMu.Unlock()

	if err := conn.send(pkt.Data); err != nil {
		logger.Error("ws handshake send failed",
			slog.String("addr", pkt.To.Addr.String()),
			slog.String("err", err.Error()))
		rawSess.CannotSend(pkt)
		connsMu.Lock()
		delete(conns, pkt.To.Addr)
		connsMu.Unlock()
		conn.close()
		return
	}

	go func() {
		for {
			select {
			case data := <-conn.incoming:
				rawSess.Receive(transportName, conn.remoteAddr, data)
			case <-conn.closed:
				connsMu.Lock()
				delete(conns, conn.remoteAddr)
				connsMu.Unlock()
				conn.close()
				return
			}
		}
	}()
}

func (c *wasmWSConn) send(data []byte) error {
	if c.ws.Get("readyState").Int() != 1 { // 1 = OPEN
		return errors.New("websocket not open")
	}
	arr := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(arr, data)
	c.ws.Call("send", arr)
	return nil
}

func (c *wasmWSConn) markClosed() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

func (c *wasmWSConn) close() {
	c.cleanupMu.Lock()
	if c.cleanedUp {
		c.cleanupMu.Unlock()
		return
	}
	c.cleanedUp = true
	c.cleanupMu.Unlock()

	c.markClosed()
	c.ws.Call("close")
	for _, f := range c.releases {
		f.Release()
	}
}
