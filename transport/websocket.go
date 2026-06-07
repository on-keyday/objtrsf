//go:build !js

package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"sync"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
	"golang.org/x/net/websocket"
)

// WebSocketConn is a connection that uses a WebSocket for communication.
type WebSocketConn struct {
	conn       *websocket.Conn
	remoteAddr netip.AddrPort
	cancel     context.CancelFunc
}

// newWebSocketConn creates a new WebSocketConn.
func newWebSocketConn(conn *websocket.Conn, remoteAddr netip.AddrPort, cancel context.CancelFunc) *WebSocketConn {
	return &WebSocketConn{
		conn:       conn,
		remoteAddr: remoteAddr,
		cancel:     cancel,
	}
}

// Send writes a message to the WebSocket connection.
func (c *WebSocketConn) Send(p []byte) error {
	return websocket.Message.Send(c.conn, p)
}

// Receive reads a message from the WebSocket connection.
func (c *WebSocketConn) Receive() ([]byte, error) {
	var p []byte
	err := websocket.Message.Receive(c.conn, &p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Close closes the WebSocket connection.
func (c *WebSocketConn) Close() error {
	c.cancel()
	return c.conn.Close()
}

// RemoteAddr returns the remote network address.
func (c *WebSocketConn) RemoteAddr() string {
	return c.remoteAddr.String()
}

type connectionMap struct {
	connMapLock sync.RWMutex
	connMap     map[netip.AddrPort]*WebSocketConn
}

func (m *connectionMap) Get(addr netip.AddrPort) (*WebSocketConn, bool) {
	m.connMapLock.RLock()
	defer m.connMapLock.RUnlock()
	conn, ok := m.connMap[addr]
	return conn, ok
}

func (m *connectionMap) Set(addr netip.AddrPort, conn *WebSocketConn) {
	m.connMapLock.Lock()
	defer m.connMapLock.Unlock()
	m.connMap[addr] = conn
}

func (m *connectionMap) Delete(addr netip.AddrPort) {
	m.connMapLock.Lock()
	defer m.connMapLock.Unlock()
	delete(m.connMap, addr)
}

// startTransportLoops runs two goroutines on top of rawSess:
//   - recv: pumps frames from each accepted/dialed *WebSocketConn into rawSess.Receive
//   - send: drains rawSess.GetSenderChannel and writes to the mapped conn,
//     dialing a fresh outbound connection (using dialPath) when a Handshake
//     packet targets an unknown peer.
//
// The dial branch in the send loop relies on an upstream invariant:
// objproto.endpoint.SendHandshake (objproto/objproto.go:639-641) returns an
// error for EndpointModeServer. So Handshake packets only reach this loop
// from Client or Mutual endpoints; for pure Server callers dialPath being
// empty is safe because no Handshake is ever observed.
func startTransportLoops(rawSess objproto.RawEndpoint, transportName string,
	connChan chan *WebSocketConn, connMap *connectionMap,
	senderChannel <-chan *objproto.PacketData,
	tlsConf *tls.Config, dialPath string, logger *slog.Logger) {

	go func() {
		for conn := range connChan {
			go func(c *WebSocketConn) {
				for {
					recv, err := c.Receive()
					if err != nil {
						connMap.Delete(c.remoteAddr)
						if errors.Is(err, io.EOF) {
							logger.Info("websocket connection closed by remote", slog.String("address", c.remoteAddr.String()))
						} else {
							logger.Error("failed to receive websocket message", slog.String("address", c.remoteAddr.String()), slog.String("error", err.Error()))
						}
						return
					}
					rawSess.Receive(transportName, c.remoteAddr, recv)
				}
			}(conn)
		}
	}()

	go func() {
		for pkt := range senderChannel {
			conn, ok := connMap.Get(pkt.To.Addr)
			if !ok {
				if pkt.Kind == packet.PacketKind_Handshake {
					go func() {
						wsScheme := "ws"
						httpScheme := "http"
						if tlsConf != nil {
							wsScheme = "wss"
							httpScheme = "https"
						}
						conf := &websocket.Config{
							Location: &url.URL{
								Scheme: wsScheme,
								Host:   pkt.To.Addr.String(),
								Path:   dialPath,
							},
							Origin: &url.URL{
								Scheme: httpScheme,
								Host:   pkt.To.Addr.String(),
							},
							TlsConfig: tlsConf,
							Version:   websocket.ProtocolVersionHybi13,
						}
						ws, err := websocket.DialConfig(conf)
						if err != nil {
							logger.Error("failed to dial websocket", slog.String("address", pkt.To.Addr.String()), slog.String("error", err.Error()))
							rawSess.CannotSend(pkt)
							return
						}
						conn := newWebSocketConn(ws, pkt.To.Addr, func() {})
						connMap.Set(pkt.To.Addr, conn)
						err = conn.Send(pkt.Data)
						if err != nil {
							logger.Error("failed to send websocket handshake message", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
							rawSess.CannotSend(pkt)
							connMap.Delete(pkt.To.Addr)
							return
						}
						connChan <- conn
					}()
					continue
				}
				logger.Error("no websocket connection for address", slog.String("address", pkt.To.String()))
				rawSess.CannotSend(pkt)
				continue
			}
			err := conn.Send(pkt.Data)
			if err != nil {
				logger.Error("failed to send websocket message", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
				rawSess.CannotSend(pkt)
				connMap.Delete(pkt.To.Addr)
			}
		}
	}()
}

// newAcceptHandler builds the http.Handler that upgrades incoming WS
// connections, registers them in connMap, and feeds them into connChan
// for the recv loop to pick up.
func newAcceptHandler(connChan chan<- *WebSocketConn, connMap *connectionMap, tlsConf *tls.Config, logger *slog.Logger) http.Handler {
	return &websocket.Server{
		Config: websocket.Config{
			TlsConfig: tlsConf,
		},
		Handshake: func(c *websocket.Config, r *http.Request) error {
			var err error
			c.Origin, err = websocket.Origin(c, r)
			if err == nil && c.Origin == nil {
				return fmt.Errorf("null origin")
			}
			return err
		},
		Handler: func(ws *websocket.Conn) {
			ctx, cancel := context.WithCancel(ws.Request().Context())
			remoteAddr, err := netip.ParseAddrPort(ws.Request().RemoteAddr)
			if err != nil {
				logger.Error("invalid remote address", slog.String("address", ws.Request().RemoteAddr))
				ws.Close()
				cancel()
				return
			}
			conn := newWebSocketConn(ws, remoteAddr, cancel)
			connMap.Set(remoteAddr, conn)
			connChan <- conn
			<-ctx.Done()
		},
	}
}

// transportName returns "wss" if a TLS config is supplied, "ws" otherwise.
// Used to tag PacketData with the right transport identifier.
func transportName(tlsConf *tls.Config) string {
	if tlsConf != nil {
		return "wss"
	}
	return "ws"
}

// WebSocketEndpoint constructs a WebSocket-backed objproto Endpoint in the
// mode specified by cfg.Mode.
//
// mux contract:
//   - Client:        mux must be nil (no accept handler is registered)
//   - Server/Mutual: mux must be non-nil; the accept handler is registered
//     via mux.Handle(cfg.Path, handler)
//
// The returned Endpoint is rawSess directly (objproto.RawEndpoint embeds
// objproto.Endpoint). The listen-side http.Server lifecycle is owned by
// the caller; for Server / Mutual the caller must run http.ListenAndServe
// (or equivalent) on a *http.Server bound to mux.
func WebSocketEndpoint(mux *http.ServeMux, cfg WebSocketConfig) (objproto.Endpoint, error) {
	rawSess := objproto.NewEndpoint(cfg.Logger, cfg.Mode)
	if err := WebSocketEndpointEx(rawSess, mux, cfg, nil); err != nil {
		return nil, err
	}
	return rawSess, nil
}

// WebSocketEndpointEx is the lower-level variant for callers that already
// own a RawEndpoint (e.g. dualstack). It enforces the same mux contract
// as WebSocketEndpoint.
//
// sendTo is the channel from which this endpoint's WS sender loop pulls
// outbound PacketData. Pass nil to use rawSess.GetSenderChannel() (the
// default — single-stack callers want this). Pass a dedicated channel
// when sharing rawSess with another transport leg (UDP), so dualstack
// can fan-out by pkt.To.Transport without two readers racing on the
// same source channel; see transport/dualstack.go.
func WebSocketEndpointEx(rawSess objproto.RawEndpoint, mux *http.ServeMux, cfg WebSocketConfig, sendTo <-chan *objproto.PacketData) error {
	// Transport mux contract is now independent of cfg.Mode (objproto mode).
	// mux non-nil  → register WS accept handler on cfg.Path (incoming TCP→WS).
	// mux nil      → dial-only at transport layer; we still accept incoming
	//                objproto Handshake packets on existing connMap entries if
	//                cfg.Mode permits (Mutual). This lets a runner that dials
	//                out to the server still serve as a Phase C relay proxy:
	//                the server can SendHandshake at a new connection_id over
	//                the existing reused WS conn, and the runner's endpoint
	//                will create a new activeConn instead of dropping the
	//                Handshake (which is what EndpointModeClient does).
	switch cfg.Mode {
	case objproto.EndpointModeClient, objproto.EndpointModeServer, objproto.EndpointModeMutual:
		// all three accepted
	default:
		return fmt.Errorf("unknown EndpointMode: %v", cfg.Mode)
	}
	if cfg.Mode == objproto.EndpointModeClient && mux != nil {
		return errors.New("mux must be nil for Client mode (Client cannot accept incoming handshakes)")
	}
	if cfg.Mode == objproto.EndpointModeServer && mux == nil {
		return errors.New("mux is required for Server mode (Server only accepts incoming, never dials)")
	}

	connChan := make(chan *WebSocketConn, 10)
	connMap := &connectionMap{
		connMap: make(map[netip.AddrPort]*WebSocketConn),
	}

	// Register accept handler only when caller provided a mux. Mutual mode with
	// no mux is now a valid "dial-only transport + bidirectional objproto"
	// configuration used by dial-mode runners.
	if mux != nil {
		mux.Handle(cfg.Path, newAcceptHandler(connChan, connMap, cfg.TLS, cfg.Logger))
	}

	// dialPath: Client/Mutual will dial outbound, Server will not.
	// Per upstream invariant (objproto.SendHandshake rejects Server mode),
	// no Handshake packet reaches the sender loop in Server mode anyway.
	dialPath := ""
	if cfg.Mode != objproto.EndpointModeServer {
		dialPath = cfg.Path
	}

	if sendTo == nil {
		sendTo = rawSess.GetSenderChannel()
	}
	startTransportLoops(rawSess, transportName(cfg.TLS), connChan, connMap,
		sendTo, cfg.TLS, dialPath, cfg.Logger)
	return nil
}
