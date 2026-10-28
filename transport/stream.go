package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
)

// StreamConfig configures a leg that carries objproto packets over any
// byte stream (vsock, a Unix socket, TCP) with no framing of its own.
//
// None is needed because a packet delimits itself: PacketHeader's len is
// sent in the clear and counts the bytes that follow, so a reader takes one
// header, then len bytes, and has exactly the one packet Receive expects.
// That makes this leg depend on len staying unmasked. If objproto ever
// protects it, the stream needs its own length prefix.
//
// A stream cannot skip a bad packet the way a datagram socket can: once one
// header is misread every later boundary is wrong. So a packet that does not
// decode ends its connection.
type StreamConfig struct {
	Logger *slog.Logger
	// Transport labels every connection this leg carries. It must equal the
	// transport of the ConnectionIDs the leg is addressed by, because objproto
	// keys a connection by its whole ConnectionID.
	Transport string
	// Dial opens a stream to addr when a handshake goes to a peer the leg
	// has no stream to. nil means the leg never dials.
	Dial func(ctx context.Context, addr netip.AddrPort) (net.Conn, error)
	// Listener, when non-nil, is accepted from until it is closed. The caller
	// owns it.
	Listener net.Listener
	// AddrOf maps an accepted stream to the address its packets are keyed by.
	// Required with Listener.
	AddrOf func(net.Conn) (netip.AddrPort, error)
}

// streamDialTimeout bounds one Dial; a handshake that waits longer has been
// given up on by objproto anyway.
const streamDialTimeout = 10 * time.Second

// packetHeaderLen is PacketHeader's encoded size, taken from the generated
// encoder so the schema stays the one place that says how long it is.
var packetHeaderLen = func() int {
	b, err := (&packet.PacketHeader{}).Append(nil)
	if err != nil {
		panic(err)
	}
	return len(b)
}()

// readPacket reads one whole packet: its header, then header.len bytes.
func readPacket(r io.Reader) ([]byte, error) {
	hdr := make([]byte, packetHeaderLen)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	var h packet.PacketHeader
	off := 0
	if err := h.DecodeSlice(hdr, &off); err != nil {
		return nil, fmt.Errorf("decode packet header: %w", err)
	}
	buf := make([]byte, packetHeaderLen+int(h.Len))
	copy(buf, hdr)
	if _, err := io.ReadFull(r, buf[packetHeaderLen:]); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}

type streamConns struct {
	mu sync.Mutex
	m  map[netip.AddrPort]net.Conn
}

func (s *streamConns) get(addr netip.AddrPort) (net.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.m[addr]
	return c, ok
}

func (s *streamConns) set(addr netip.AddrPort, c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[addr] = c
}

// remove deletes addr only while it still maps to c, so a stream that ends
// after a newer one replaced it does not take the newer one's entry along.
func (s *streamConns) remove(addr netip.AddrPort, c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[addr] == c {
		delete(s.m, addr)
	}
}

// StreamLeg is a MultiLegEndpoint leg over cfg.
func StreamLeg(cfg StreamConfig) Leg {
	return Leg{
		transports: []string{cfg.Transport},
		start: func(rawSess objproto.RawEndpoint, sendTo <-chan *objproto.PacketData) error {
			return StreamEndpointEx(rawSess, cfg, sendTo)
		},
	}
}

// StreamEndpointEx attaches a stream leg to rawSess, the stream counterpart of
// WebSocketEndpointEx, with the same contract between mode and direction: a
// Client endpoint must not accept (no Listener), a Server endpoint never dials
// and must accept. sendTo nil means rawSess.GetSenderChannel().
func StreamEndpointEx(rawSess objproto.RawEndpoint, cfg StreamConfig, sendTo <-chan *objproto.PacketData) error {
	if cfg.Transport == "" {
		return errors.New("stream leg: Transport is required")
	}
	switch rawSess.EndpointMode() {
	case objproto.EndpointModeClient:
		if cfg.Listener != nil {
			return errors.New("stream leg: Listener must be nil for Client mode (Client cannot accept incoming handshakes)")
		}
	case objproto.EndpointModeServer:
		if cfg.Listener == nil {
			return errors.New("stream leg: Listener is required for Server mode (Server only accepts incoming, never dials)")
		}
		cfg.Dial = nil
	}
	if cfg.Listener != nil && cfg.AddrOf == nil {
		return errors.New("stream leg: AddrOf is required with Listener")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if sendTo == nil {
		sendTo = rawSess.GetSenderChannel()
	}
	conns := &streamConns{m: make(map[netip.AddrPort]net.Conn)}

	recv := func(c net.Conn, addr netip.AddrPort) {
		r := bufio.NewReader(c)
		for {
			data, err := readPacket(r)
			if err != nil {
				conns.remove(addr, c)
				c.Close()
				if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
					logger.Info("stream closed", slog.String("transport", cfg.Transport), slog.String("address", addr.String()))
				} else {
					logger.Error("stream read failed", slog.String("transport", cfg.Transport), slog.String("address", addr.String()), slog.String("error", err.Error()))
				}
				CloseConnectionsAt(rawSess, addr, logger)
				return
			}
			rawSess.Receive(cfg.Transport, addr, data)
		}
	}

	if cfg.Listener != nil {
		go func() {
			for {
				c, err := cfg.Listener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					logger.Error("stream accept failed", slog.String("transport", cfg.Transport), slog.String("error", err.Error()))
					time.Sleep(50 * time.Millisecond)
					continue
				}
				addr, err := cfg.AddrOf(c)
				if err != nil {
					logger.Error("stream peer has no address", slog.String("transport", cfg.Transport), slog.String("error", err.Error()))
					c.Close()
					continue
				}
				conns.set(addr, c)
				go recv(c, addr)
			}
		}()
	}

	go func() {
		for pkt := range sendTo {
			if pkt.To.Transport != cfg.Transport {
				logger.Error("unsupported transport for stream leg", slog.String("leg", cfg.Transport), slog.String("transport", pkt.To.Transport))
				rawSess.CannotSend(pkt)
				continue
			}
			c, ok := conns.get(pkt.To.Addr)
			if !ok {
				if pkt.Kind == packet.PacketKind_Handshake && cfg.Dial != nil {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), streamDialTimeout)
						defer cancel()
						c, err := cfg.Dial(ctx, pkt.To.Addr)
						if err != nil {
							logger.Error("stream dial failed", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
							rawSess.CannotSend(pkt)
							return
						}
						// Written before the stream is published, so the send loop
						// cannot write to it concurrently with this first packet.
						if _, err := c.Write(pkt.Data); err != nil {
							logger.Error("stream handshake write failed", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
							c.Close()
							rawSess.CannotSend(pkt)
							return
						}
						conns.set(pkt.To.Addr, c)
						recv(c, pkt.To.Addr)
					}()
					continue
				}
				logger.Error("no stream for address", slog.String("to", pkt.To.String()))
				rawSess.CannotSend(pkt)
				continue
			}
			if _, err := c.Write(pkt.Data); err != nil {
				logger.Error("stream write failed", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
				rawSess.CannotSend(pkt)
				conns.remove(pkt.To.Addr, c)
				c.Close()
			}
		}
	}()
	return nil
}
