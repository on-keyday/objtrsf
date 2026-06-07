package transport

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/on-keyday/objtrsf/objproto"
)

// ClientEndpoint is a convenience constructor that resolves a server CID
// and wires up the appropriate transport (ws/wss/udp) for a Client-mode
// objproto Endpoint. Currently caller-zero in this repo; preserved as
// template alongside UDPWebsocketDualStackEndpoint for future use.
func ClientEndpoint(logger *slog.Logger, addr string, udpPort uint16) (objproto.ConnectionID, objproto.Endpoint, error) {
	cid, err := objproto.ParseConnectionID(addr, objproto.ParseOption_AllowRandomID|objproto.ParseOption_ResolveAddr)
	if err != nil {
		return objproto.ConnectionID{}, nil, err
	}
	sess := objproto.NewEndpoint(logger, objproto.EndpointModeClient)
	switch cid.Transport {
	case "ws", "wss":
		err = WebSocketEndpointEx(sess, nil, WebSocketConfig{
			Logger: logger,
			Mode:   objproto.EndpointModeClient,
		}, nil)
		if err != nil {
			return objproto.ConnectionID{}, nil, err
		}
	case "udp":
		_, err = UDPEndpointEx(sess, logger, udpPort, sess.GetSenderChannel())
		if err != nil {
			return objproto.ConnectionID{}, nil, err
		}
	default:
		return objproto.ConnectionID{}, nil, errors.New("unsupported transport: " + cid.Transport)
	}
	return cid, sess, nil
}

// UDPWebsocketDualStackConfig configures a UDP+WebSocket dual stack
// Endpoint that shares a single objproto RawEndpoint across both
// transports. The fan-out goroutine is the *single* reader of
// rawSess.GetSenderChannel(); UDP / WS legs each read from a dedicated
// bounded channel that the fan-out feeds based on pkt.To.Transport.
//
// WS.Mode selects the WS leg behaviour (Client / Server / Mutual). Mux
// must be non-nil for Server / Mutual; nil for Client.
type UDPWebsocketDualStackConfig struct {
	Logger  *slog.Logger
	UDPPort uint16
	Mux     *http.ServeMux  // required when WS.Mode is Server or Mutual; nil for Client
	WS      WebSocketConfig // Mode / Path / TLS / Logger drive the WS leg
}

type UDPWebsocketDualStack struct {
	Endpoint objproto.Endpoint
}

func UDPWebsocketDualStackEndpoint(cfg UDPWebsocketDualStackConfig) (UDPWebsocketDualStack, error) {
	rawSess := objproto.NewEndpoint(cfg.Logger, cfg.WS.Mode)
	udpChan := make(chan *objproto.PacketData, 100)
	wsChan := make(chan *objproto.PacketData, 100)

	if _, err := UDPEndpointEx(rawSess, cfg.Logger, cfg.UDPPort, udpChan); err != nil {
		return UDPWebsocketDualStack{}, err
	}

	if err := WebSocketEndpointEx(rawSess, cfg.Mux, cfg.WS, wsChan); err != nil {
		return UDPWebsocketDualStack{}, err
	}

	// Fan-out: the single reader of rawSess.GetSenderChannel() routes
	// packets to the per-transport sender channel. Both UDP and WS legs
	// were constructed with explicit sendTo, so they do not race against
	// this loop on the source channel.
	go fanOutByTransport(rawSess, rawSess.GetSenderChannel(), udpChan, wsChan, cfg.Logger)

	return UDPWebsocketDualStack{Endpoint: rawSess}, nil
}

// fanOutByTransport reads packets from src and routes them by
// pkt.To.Transport. Extracted so dualstack_test.go can drive it without
// binding a real UDP port. cancelSink is invoked for packets whose
// transport is unknown so the upper layer can mark them as undeliverable.
func fanOutByTransport(
	cancelSink interface {
		CannotSend(*objproto.PacketData)
	},
	src <-chan *objproto.PacketData,
	udpDst, wsDst chan<- *objproto.PacketData,
	logger *slog.Logger,
) {
	for pkt := range src {
		switch pkt.To.Transport {
		case "udp":
			udpDst <- pkt
		case "ws", "wss":
			wsDst <- pkt
		default:
			if logger != nil {
				logger.Error("unsupported transport for udp-websocket session",
					slog.String("transport", pkt.To.Transport))
			}
			if cancelSink != nil {
				cancelSink.CannotSend(pkt)
			}
		}
	}
}
