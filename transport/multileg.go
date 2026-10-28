package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/on-keyday/objtrsf/objproto"
)

// Leg is one transport of an endpoint that MultiLegEndpoint builds from
// several. Make one with UDPLeg, WebSocketLeg, StreamLeg or VsockLeg.
type Leg struct {
	// transports are the pkt.To.Transport values this leg sends.
	transports []string
	start      func(rawSess objproto.RawEndpoint, sendTo <-chan *objproto.PacketData) error
}

// UDPLeg listens on port (0 = OS-assigned), as UDPEndpoint does.
func UDPLeg(logger *slog.Logger, port uint16) Leg {
	return Leg{
		transports: []string{"udp"},
		start: func(rawSess objproto.RawEndpoint, sendTo <-chan *objproto.PacketData) error {
			_, err := UDPEndpointEx(rawSess, logger, port, sendTo)
			return err
		},
	}
}

// WebSocketLeg carries ws and wss under WebSocketEndpointEx's mux contract.
// cfg.Mode is ignored: the leg takes the mode MultiLegEndpoint is given.
func WebSocketLeg(mux *http.ServeMux, cfg WebSocketConfig) Leg {
	return Leg{
		transports: []string{"ws", "wss"},
		start: func(rawSess objproto.RawEndpoint, sendTo <-chan *objproto.PacketData) error {
			cfg.Mode = rawSess.EndpointMode()
			return WebSocketEndpointEx(rawSess, mux, cfg, sendTo)
		},
	}
}

// MultiLegEndpoint builds one objproto endpoint that sends and receives over
// every leg, so a peer reachable over any of them is the same endpoint's
// connection. Each outbound packet goes to the leg that carries its
// destination's transport; one whose transport no leg carries is reported to
// CannotSend.
func MultiLegEndpoint(logger *slog.Logger, mode objproto.EndpointMode, legs ...Leg) (objproto.Endpoint, error) {
	if len(legs) == 0 {
		return nil, errors.New("MultiLegEndpoint: no legs")
	}
	rawSess := objproto.NewEndpoint(logger, mode)
	routes := make(map[string]chan<- *objproto.PacketData)
	for _, leg := range legs {
		ch := make(chan *objproto.PacketData, 100)
		for _, t := range leg.transports {
			if _, dup := routes[t]; dup {
				return nil, fmt.Errorf("MultiLegEndpoint: two legs carry transport %q", t)
			}
			routes[t] = ch
		}
		if err := leg.start(rawSess, ch); err != nil {
			return nil, err
		}
	}
	// The single reader of rawSess.GetSenderChannel(): each leg reads only its
	// own channel, so no two legs race on the source.
	go fanOutByTransport(rawSess, rawSess.GetSenderChannel(), routes, logger)
	return rawSess, nil
}

// fanOutByTransport reads packets from src and routes them by
// pkt.To.Transport. Extracted so dualstack_test.go can drive it without
// binding a real socket. cancelSink is invoked for packets whose transport
// has no route so the upper layer can mark them as undeliverable.
func fanOutByTransport(
	cancelSink interface {
		CannotSend(*objproto.PacketData)
	},
	src <-chan *objproto.PacketData,
	routes map[string]chan<- *objproto.PacketData,
	logger *slog.Logger,
) {
	for pkt := range src {
		if dst, ok := routes[pkt.To.Transport]; ok {
			dst <- pkt
			continue
		}
		if logger != nil {
			logger.Error("no transport leg for packet", slog.String("transport", pkt.To.Transport))
		}
		if cancelSink != nil {
			cancelSink.CannotSend(pkt)
		}
	}
}
