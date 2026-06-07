package transport

import (
	"log/slog"

	"github.com/on-keyday/objtrsf/objproto"
)

func InMemoryPipeSession(logger *slog.Logger) (objproto.Endpoint, objproto.Endpoint) {
	selfSess := objproto.NewEndpoint(logger, objproto.EndpointModeMutual)
	peerSess := objproto.NewEndpoint(logger, objproto.EndpointModeMutual)

	fromSelf := selfSess.GetSenderChannel()
	fromPeer := peerSess.GetSenderChannel()

	go func() {
		for pkt := range fromSelf {
			peerSess.Receive(pkt.To.Transport, pkt.To.Addr, pkt.Data)
		}
	}()

	go func() {
		for pkt := range fromPeer {
			selfSess.Receive(pkt.To.Transport, pkt.To.Addr, pkt.Data)
		}
	}()

	return selfSess, peerSess
}
