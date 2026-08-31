package mock

import (
	"log/slog"
	"os"
	"sync/atomic"
	"testing"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf"
)

type MockUnderlayingTransport struct {
	Peer trsf.Transport
}

func (m *MockUnderlayingTransport) SendMessage(data []byte) (int, objproto.PacketNumber, error) {
	return m.SendMessageWithPacketNumber(data, 0)
}

type MockPacketNumberIssuer struct {
	current atomic.Uint64
}

func (m *MockPacketNumberIssuer) ConsumePacketNumber() objproto.PacketNumber {
	pn := m.current.Add(1) - 1
	return pn
}

func (m *MockUnderlayingTransport) SendMessageWithPacketNumber(data []byte, pn objproto.PacketNumber) (int, objproto.PacketNumber, error) {
	if m.Peer == nil {
		return 0, 0, nil
	}
	m.Peer.Send(&objproto.Message{
		Data:         data,
		PacketNumber: pn,
	})
	return len(data), pn, nil
}

func SetupClientServer(t testing.TB) (trsf.Transport, trsf.Transport) {
	return SetupClientServerEx(t, slog.LevelDebug)
}

func SetupClientServerEx(t testing.TB, logLevel slog.Level) (trsf.Transport, trsf.Transport) {
	debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})).With("test", t.Name())
	ctx := t.Context()
	client := trsf.NewStreams(ctx, false, 1200, 1500, &MockPacketNumberIssuer{}, debugLogger.With("role", "client"))
	if client == nil {
		t.Fatal("failed to create client streams")
	}
	server := trsf.NewStreams(ctx, true, 1200, 1500, &MockPacketNumberIssuer{}, debugLogger.With("role", "server"))
	if server == nil {
		t.Fatal("failed to create server streams")
	}
	return client, server
}

func BackgroundIO(t testing.TB, peer1, peer2 trsf.Transport) {
	ctx := t.Context()
	go func() {
		for {
			pkt := peer1.Recv(ctx)
			if pkt == nil {
				return
			}
			pkt.Send(ctx, &MockUnderlayingTransport{Peer: peer2})
		}
	}()
	go func() {
		for {
			pkt := peer2.Recv(ctx)
			if pkt == nil {
				return
			}
			pkt.Send(ctx, &MockUnderlayingTransport{Peer: peer1})
		}
	}()
}
