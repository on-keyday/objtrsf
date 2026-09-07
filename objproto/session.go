package objproto

import (
	"context"
	"net/netip"
	"time"
)

type ChanWithTimeout[T any] struct {
	C <-chan T
	// onTick, when set, runs every tickAfter that the wait goes unanswered,
	// with tickAfter doubling after each one. The handshake paths set it to
	// retransmit, and that is why retransmission costs no goroutine: whoever
	// dialed is already blocked in WaitWithTimeout below. Unset means the wait
	// behaves exactly as it always did.
	onTick    func()
	tickAfter time.Duration
}

func (c *ChanWithTimeout[T]) WaitWithTimeout(ctx context.Context, timeout time.Duration) (T, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if c.onTick == nil || c.tickAfter <= 0 {
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
	// One timer, Reset rather than a fresh time.After per turn: a timer per
	// iteration was 17.5% of this project's allocations once already. Go >= 1.23
	// discards a stale value on Reset, so there is no drain to do.
	delay := c.tickAfter
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
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
		case <-timer.C:
			c.onTick()
			delay *= 2
			timer.Reset(delay)
		}
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

// DefaultKeyUpdateInterval is the recommended maxKeyAge for AutoKeyUpdate.
const DefaultKeyUpdateInterval = 10 * time.Minute

// AutoKeyUpdate rekeys connections whose current send phase has been in use
// longer than maxKeyAge. The packet-count trigger inside the send path covers
// busy connections; this covers idle ones, which would otherwise hold a single
// key for the whole life of the connection.
func AutoKeyUpdate(s Endpoint, interval, maxKeyAge time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		for _, conn := range s.ListActiveConnections() {
			if time.Since(conn.KeyPhaseAt()) < maxKeyAge {
				continue
			}
			// A refusal is the anti-double-advance floor talking; the next
			// tick retries.
			_ = conn.UpdateKey()
		}
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
// pushed in via Receive; CannotSend reports that a queued send was rejected by
// the transport.
//
// CannotSend is a TERMINAL report, not a per-packet one: the only
// implementation closes the connection the packet belonged to. An oversized
// datagram is therefore NOT one of its cases, whatever an earlier version of
// this comment claimed -- transport/udp.go deliberately routes EMSGSIZE past
// it and drops the datagram, because handing a too-large PLPMTUD probe to
// CannotSend would tear the connection down. Nothing carries "this one
// datagram did not fit" up to trsf; see the comment at that drop for why not.
// If a size signal is ever wanted here it needs its own path, and the reason
// to hesitate is that EMSGSIZE does not mean the same thing on every OS --
// probe-mode Linux raises it only above the INTERFACE MTU, while the plain
// DF-bit platforms let their kernel's own path-MTU knowledge in.
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
	// UpdateKey advances this connection's send key. The peer follows the key
	// phase bit on the next packet it receives.
	UpdateKey() error
	// KeyPhaseAt reports when the current send phase began.
	KeyPhaseAt() time.Time
	IsActive() bool
	Done() <-chan struct{}
	// for proxy connections
	// proxy is established by upper layer negotiation and using Endpoint.SetProxy.
	// Then rehandshake is performed to switch the connection to proxied peer.
	// Connection returned by RehandshakeForProxy is as same as the original connection but proxied
	RehandshakeForProxy(priv []byte, hs *Handshake) (*ChanWithTimeout[Connection], error)
	IsProxied() bool
}
