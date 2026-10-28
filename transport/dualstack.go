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
// Endpoint: MultiLegEndpoint with a UDPLeg and a WebSocketLeg.
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
	ep, err := MultiLegEndpoint(cfg.Logger, cfg.WS.Mode,
		UDPLeg(cfg.Logger, cfg.UDPPort),
		WebSocketLeg(cfg.Mux, cfg.WS))
	if err != nil {
		return UDPWebsocketDualStack{}, err
	}
	return UDPWebsocketDualStack{Endpoint: ep}, nil
}
