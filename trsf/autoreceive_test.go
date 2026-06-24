package trsf

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// errStop terminates the AutoReceive loop once the scripted inbound messages
// are exhausted, standing in for a closed underlying connection.
var errStop = errors.New("stop")

// mockConn is a minimal UnderlayingBidirectionalTransport: it replays a fixed
// queue of inbound messages and records every outbound SendMessage payload.
type mockConn struct {
	inbound []*objproto.Message
	idx     int
	sent    [][]byte
}

func (m *mockConn) ReceiveMessageContext(ctx context.Context) (*objproto.Message, error) {
	if m.idx >= len(m.inbound) {
		return nil, errStop
	}
	msg := m.inbound[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockConn) SendMessage(msg []byte) (int, objproto.PacketNumber, error) {
	cp := make([]byte, len(msg))
	copy(cp, msg)
	m.sent = append(m.sent, cp)
	return len(msg), 0, nil
}

func (m *mockConn) SendMessageWithPacketNumber(msg []byte, pn objproto.PacketNumber) (int, objproto.PacketNumber, error) {
	return m.SendMessage(msg)
}

type recvEvent struct {
	msg *objproto.Message
	err error
}

func msgOf(b ...byte) *objproto.Message { return &objproto.Message{Data: b} }

// runAutoReceive drives AutoReceive over the scripted inbound queue and returns
// the captured onEvent calls plus the mock's outbound payloads. The terminal
// (nil, errStop) event — emitted when the mock queue drains and stands in for a
// closed connection — is stripped, since it is test scaffolding rather than a
// behavior under test. (The genuine receive-error path is covered separately.)
func runAutoReceive(inbound []*objproto.Message, opts ...Option) (events []recvEvent, sent [][]byte) {
	conn := &mockConn{inbound: inbound}
	AutoReceive(context.Background(), nil, conn, func(msg *objproto.Message, err error) {
		if err == errStop {
			return
		}
		events = append(events, recvEvent{msg, err})
	}, opts...)
	return events, conn.sent
}

func TestAutoReceive_PongDroppedByDefault(t *testing.T) {
	events, sent := runAutoReceive([]*objproto.Message{
		msgOf(byte(wire.ApplicationPayloadKind_Pong), 0xAA),
	})
	if len(events) != 0 {
		t.Fatalf("pong should not surface by default, got %d events", len(events))
	}
	if len(sent) != 0 {
		t.Fatalf("pong must not trigger any send, got %d", len(sent))
	}
}

func TestAutoReceive_DeliverPong(t *testing.T) {
	events, _ := runAutoReceive([]*objproto.Message{
		msgOf(byte(wire.ApplicationPayloadKind_Pong), 0xAA, 0xBB),
	}, WithDeliverPong())
	if len(events) != 1 {
		t.Fatalf("want 1 surfaced pong, got %d", len(events))
	}
	e := events[0]
	if e.err != nil {
		t.Fatalf("pong err = %v, want nil", e.err)
	}
	// Delivered verbatim, kind byte intact.
	want := []byte{byte(wire.ApplicationPayloadKind_Pong), 0xAA, 0xBB}
	if string(e.msg.Data) != string(want) {
		t.Fatalf("pong data = %v, want %v", e.msg.Data, want)
	}
}

func TestAutoReceive_PingAutoPongByDefault(t *testing.T) {
	events, sent := runAutoReceive([]*objproto.Message{
		msgOf(byte(wire.ApplicationPayloadKind_Ping), 0x01, 0x02),
	})
	if len(events) != 0 {
		t.Fatalf("ping should be auto-handled, got %d events", len(events))
	}
	if len(sent) != 1 {
		t.Fatalf("want 1 auto-pong send, got %d", len(sent))
	}
	// Auto-pong echoes the ping body verbatim.
	want := EncodePong([]byte{0x01, 0x02})
	if string(sent[0]) != string(want) {
		t.Fatalf("auto-pong = %v, want %v", sent[0], want)
	}
}

func TestAutoReceive_ManualPing(t *testing.T) {
	events, sent := runAutoReceive([]*objproto.Message{
		msgOf(byte(wire.ApplicationPayloadKind_Ping), 0x01, 0x02),
	}, WithManualPing())
	if len(sent) != 0 {
		t.Fatalf("manual ping must suppress auto-pong, got %d sends", len(sent))
	}
	if len(events) != 1 {
		t.Fatalf("want 1 surfaced ping, got %d", len(events))
	}
	e := events[0]
	if e.err != nil {
		t.Fatalf("ping err = %v, want nil", e.err)
	}
	want := []byte{byte(wire.ApplicationPayloadKind_Ping), 0x01, 0x02}
	if string(e.msg.Data) != string(want) {
		t.Fatalf("ping data = %v, want %v", e.msg.Data, want)
	}
}

func TestAutoReceive_CloseSurfacesVerbatimWithEOF(t *testing.T) {
	closeMsg := EncodeClose(wire.CloseStatus(0), []byte("bye"))
	events, _ := runAutoReceive([]*objproto.Message{
		{Data: closeMsg},
		// A message after Close must never be read — Close returns the loop.
		msgOf(byte(wire.ApplicationPayloadKind_Pong)),
	}, WithDeliverPong())
	if len(events) != 1 {
		t.Fatalf("want exactly 1 event (close), got %d", len(events))
	}
	e := events[0]
	if e.err != io.EOF {
		t.Fatalf("close err = %v, want io.EOF", e.err)
	}
	// Delivered verbatim (kind byte intact), not the kind-stripped body.
	if string(e.msg.Data) != string(closeMsg) {
		t.Fatalf("close data = %v, want verbatim %v", e.msg.Data, closeMsg)
	}
}

func TestAutoReceive_ReceiveErrorSurfacesNilEvent(t *testing.T) {
	// A receive error must surface as (nil, err) and stop the loop. Uses a
	// distinct sentinel (not errStop) so it survives the helper's strip; here
	// we drive AutoReceive directly to assert the terminal event verbatim.
	recvErr := errors.New("connection reset")
	var events []recvEvent
	AutoReceive(context.Background(), nil, &errConn{err: recvErr}, func(msg *objproto.Message, err error) {
		events = append(events, recvEvent{msg, err})
	})
	if len(events) != 1 {
		t.Fatalf("want 1 event for receive error, got %d", len(events))
	}
	if events[0].msg != nil || events[0].err != recvErr {
		t.Fatalf("want (nil, %v), got (%v, %v)", recvErr, events[0].msg, events[0].err)
	}
}

// errConn is an UnderlayingBidirectionalTransport whose every receive fails.
type errConn struct{ err error }

func (c *errConn) ReceiveMessageContext(ctx context.Context) (*objproto.Message, error) {
	return nil, c.err
}
func (c *errConn) SendMessage(msg []byte) (int, objproto.PacketNumber, error) {
	return len(msg), 0, nil
}
func (c *errConn) SendMessageWithPacketNumber(msg []byte, pn objproto.PacketNumber) (int, objproto.PacketNumber, error) {
	return len(msg), 0, nil
}
