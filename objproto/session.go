package objproto

import (
	"context"
	"net/netip"
	"time"
)

type ChanWithTimeout[T any] struct {
	C <-chan T
}

func (c *ChanWithTimeout[T]) WaitWithTimeout(ctx context.Context, timeout time.Duration) (T, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case v, ok := <-c.C:
		if !ok {
			var zero T
			return zero, ErrChannelClosed
		}
		return v, nil
	case <-timeoutCtx.Done():
		var zero T
		return zero, ErrTimeout
	}
}

// Endpoint is the process-scoped container that owns every Connection this
// host has with its peers, plus the bookkeeping (handshake table, probe
// table, proxy table) that lives across individual connections. There is
// intentionally no Close on this interface: an Endpoint's lifetime is meant
// to match the owning process, with the underlying transport sockets held
// open until exit. To tear down a single peer relationship, call Close on
// the corresponding Connection — not on the Endpoint.
//
// Naming note: in common networking vocabulary "Session" usually means the
// per-peer logical unit, but this package previously used Session for the
// container concept. The container is now Endpoint and the per-peer object
// is Connection, which is the layering most readers will expect.
type Endpoint interface {
	SendHandshake(cid ConnectionID, priv []byte, hs *Handshake) (*ChanWithTimeout[Connection], error)
	SendProbe(cid ConnectionID, macAddr [6]byte, ipAddr netip.AddrPort) error
	GetNewActiveConnectionChannel() <-chan Connection
	WaitNewActiveConnection(timeout time.Duration) (Connection, error)
	GetProbeInfoChannel() <-chan *ProbeInfo
	WaitProbeInfo(timeout time.Duration) (*ProbeInfo, error)
	GetConnection(cid ConnectionID) (Connection, bool)
	ListHandshakes() []HandshakeInfo
	DeleteHandshakeBefore(limit time.Time) []HandshakeInfo
	ListActiveConnections() []Connection
	DeleteInactiveConnectionsBefore(limit time.Time) []Connection
	ListProxies() []ProxyInfo
	DeleteProxyBefore(limit time.Time) []ProxyInfo
	EndpointMode() EndpointMode
	SetProxy(owned, allocate ConnectionID) error
	DeleteProxy(peer ConnectionID) error
}

func AutoGarbageCollect(s Endpoint, interval time.Duration, handshakeTimeout, connectionTimeout, proxyTimeout time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		now := time.Now()
		s.DeleteHandshakeBefore(now.Add(-handshakeTimeout))
		s.DeleteInactiveConnectionsBefore(now.Add(-connectionTimeout))
		s.DeleteProxyBefore(now.Add(-proxyTimeout))
	}
}

func AutoRespondProbes(s Endpoint, macAddr [6]byte, ipAddr netip.AddrPort) {
	probes := s.GetProbeInfoChannel()
	for probe := range probes {
		s.SendProbe(probe.Sender, macAddr, ipAddr)
	}
}

// RawEndpoint extends Endpoint with the byte-level seam used by transport
// packages (transport/udp.go, transport/websocket.go) to feed datagrams in
// and out. Outbound packets arrive on GetSenderChannel; inbound bytes are
// pushed in via Receive; CannotSend signals that a queued send was rejected
// by the transport (e.g. EMSGSIZE, dial failure) so the upper layer can
// react instead of treating it as silently delivered.
type RawEndpoint interface {
	Endpoint
	GetSenderChannel() <-chan *PacketData
	Receive(transport string, from netip.AddrPort, data []byte) error
	CannotSend(*PacketData)
}

type PacketNumber = uint64

// Connection is one peer relationship inside an Endpoint: a single
// ConnectionID, a derived ECDH key, a packet-number space, and a Message
// stream. Close terminates exactly this peer relationship; the owning
// Endpoint and any other Connections it holds are unaffected. Done returns
// a channel closed when the connection ends (peer Close, error, or local
// Close), letting callers wait without polling IsActive.
type Connection interface {
	SetName(name string)
	Name() string
	ConnectionID() ConnectionID
	ConsumePacketNumber() PacketNumber
	SendMessageWithPacketNumber(obj []byte, pn PacketNumber) (sentSize int, _ PacketNumber, _ error)
	SendMessage(obj []byte) (sentSize int, _ PacketNumber, _ error)
	ReceiveMessage() (*Message, error)
	ReceiveMessageTimeout(ctx context.Context, timeout time.Duration) (*Message, error)
	ReceiveMessageContext(ctx context.Context) (*Message, error)
	GetTranscript() []byte
	ConnectedAt() time.Time
	LastTime() time.Time
	Close() error
	IsActive() bool
	Done() <-chan struct{}
	// for proxy connections
	// proxy is established by upper layer negotiation and using Endpoint.SetProxy.
	// Then rehandshake is performed to switch the connection to proxied peer.
	// Connection returned by RehandshakeForProxy is as same as the original connection but proxied
	RehandshakeForProxy(priv []byte, hs *Handshake) (*ChanWithTimeout[Connection], error)
	IsProxied() bool
}
