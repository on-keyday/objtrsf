package trsf

import (
	"context"
	"io"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

type StreamID uint64

func (s StreamID) Next() StreamID {
	return s + 4
}

const (
	ServerBidirectionalStart  StreamID = 0
	ClientBidirectionalStart  StreamID = 1
	ServerUnidirectionalStart StreamID = 2
	ClientUnidirectionalStart StreamID = 3
)

func (s StreamID) IsServerInitiated() bool {
	return s%2 == 0
}

func (s StreamID) IsClientInitiated() bool {
	return s%2 == 1
}

func (s StreamID) IsBidirectional() bool {
	return s%4 < 2
}

func (s StreamID) IsUnidirectional() bool {
	return s%4 >= 2
}

type SendStream interface {
	ID() StreamID
	io.WriteCloser
	WriteContext(ctx context.Context, data []byte) (n int, err error)
	HasSendData() bool
	Completed() bool
	AppendData(eof bool, data ...[]byte) error
	AppendDataContext(ctx context.Context, eof bool, data ...[]byte) error
}

type ReceiveStream interface {
	ID() StreamID
	io.Reader
	ReadContext(ctx context.Context, p []byte) (n int, err error)
	ReadDirectContext(ctx context.Context, maxN uint64) ([]byte, bool, error)
	ReadDirect(maxN uint64) ([]byte, bool, error)
	HasRecvData() bool
	EOF() bool
	Cancel()
}

type BidirectionalStream interface {
	SendStream
	ReceiveStream
	CloseBoth() error
}

type Multiplexer interface {
	CreateBidirectionalStream() BidirectionalStream
	CreateSendStream() SendStream
	AcceptBidirectionalStream(ctx context.Context) (BidirectionalStream, error)
	AcceptReceiveStream(ctx context.Context) (ReceiveStream, error)
	GetInternalState() *InternalState
	GetSendStream(id StreamID) SendStream
	GetReceiveStream(id StreamID) ReceiveStream
	GetBidirectionalStream(id StreamID) BidirectionalStream
}

type Transport interface {
	Multiplexer
	Send(msg *objproto.Message)
	Recv(ctx context.Context) *SendAction
}

func AutoSend(ctx context.Context, p Transport, conn UnderlayingSendTransport, onEnd func(err error)) {
	for {
		action := p.Recv(ctx)
		if action == nil {
			if onEnd != nil {
				onEnd(nil)
			}
			return
		}
		err := action.Send(ctx, conn)
		if err != nil {
			if onEnd != nil {
				onEnd(err)
			}
			return
		}
	}
}

type UnderlayingReceiveTransport interface {
	ReceiveMessageContext(ctx context.Context) (*objproto.Message, error)
}

type UnderlayingBidirectionalTransport interface {
	UnderlayingSendTransport
	UnderlayingReceiveTransport
}

func AutoPing(ctx context.Context, conn UnderlayingSendTransport, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			conn.SendMessage([]byte{byte(wire.ApplicationPayloadKind_Ping)})
		case <-ctx.Done():
			return
		}
	}
}

// SendClose tells the peer that we are going away. The peer's AutoReceive
// will return after dispatching a final (nil, io.EOF) event so its caller
// can clean up immediately instead of waiting for the connection's idle
// timeout. Best-effort: any send error is returned to the caller.
func SendClose(conn UnderlayingSendTransport) error {
	_, _, err := conn.SendMessage([]byte{byte(wire.ApplicationPayloadKind_Close)})
	return err
}

// SendCloseBody is SendClose with a diagnostic body (status + message). The
// peer's AutoReceive surfaces the body to its onEvent callback alongside the
// io.EOF. status is logging-only; a bare SendClose is equally valid.
func SendCloseBody(conn UnderlayingSendTransport, status wire.CloseStatus, message []byte) error {
	_, _, err := conn.SendMessage(EncodeClose(status, message))
	return err
}

// recvConfig holds the opt-in knobs for AutoReceive. The zero value preserves
// the historical behavior (auto-pong on Ping, drop Pong), so existing callers
// that pass no options are unaffected.
type recvConfig struct {
	deliverPong bool
	manualPing  bool
}

// Option configures AutoReceive. Options are additive and opt-in; with none
// supplied, AutoReceive behaves exactly as before they were introduced.
type Option func(*recvConfig)

// WithDeliverPong surfaces received Pong frames to onEvent — as (msg, nil) with
// the Pong kind byte intact — instead of silently dropping them. Use it when
// the application computes RTT itself rather than relying on the transport.
func WithDeliverPong() Option { return func(c *recvConfig) { c.deliverPong = true } }

// WithManualPing suppresses the automatic Pong reply and instead surfaces the
// received Ping to onEvent — as (msg, nil) with the Ping kind byte intact — so
// the application can answer (or not) on its own terms. Callers that take this
// own the keepalive contract: a peer that never gets a Pong will eventually
// drop the connection on its ping timeout.
func WithManualPing() Option { return func(c *recvConfig) { c.manualPing = true } }

func AutoReceive(ctx context.Context, p Transport, conn UnderlayingBidirectionalTransport, onEvent func(msg *objproto.Message, err error), opts ...Option) {
	var cfg recvConfig
	for _, o := range opts {
		o(&cfg)
	}
	for {
		data, err := conn.ReceiveMessageContext(ctx)
		if err != nil {
			if onEvent != nil {
				onEvent(nil, err)
			}
			return
		}
		if len(data.Data) == 0 {
			continue
		}
		kind := wire.ApplicationPayloadKind(data.Data[0])
		if wire.IsStreamRelated(kind) {
			p.Send(data)
			continue
		}
		if kind == wire.ApplicationPayloadKind_Ping {
			if cfg.manualPing {
				// Caller handles the ping; deliver it verbatim (kind byte
				// intact) and send no automatic Pong.
				if onEvent != nil {
					onEvent(data, nil)
				}
				continue
			}
			// Respond with a Pong, echoing the ping body verbatim so the
			// initiator can compute RTT. A bare ping echoes a bare pong.
			conn.SendMessage(EncodePong(data.Data[1:]))
			continue
		}
		if kind == wire.ApplicationPayloadKind_Pong {
			if cfg.deliverPong {
				// Surface verbatim (kind byte intact); the caller correlates
				// the body against its outstanding ping for RTT.
				if onEvent != nil {
					onEvent(data, nil)
				}
			}
			// Otherwise ignore.
			continue
		}
		if kind == wire.ApplicationPayloadKind_Close {
			// Peer signalled it is going away. Dispatch a final EOF event so
			// the caller can run its cleanup, then exit the loop. The message
			// is delivered verbatim (kind byte intact), so a caller that wants
			// the diagnostic close body strips the leading kind byte itself;
			// callers that only care about EOF check err first and ignore msg.
			if onEvent != nil {
				onEvent(data, io.EOF)
			}
			return
		}
		if onEvent != nil {
			onEvent(data, nil)
		}
	}
}
