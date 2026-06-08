package trsf_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf"
	"github.com/on-keyday/objtrsf/trsf/mock"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// --- SendAction inspection helpers --------------------------------------
//
// objtrsf's SendAction does not expose the ksdk typed fields
// (action.Packet/ACK/Window/Cancel). It carries raw wire bytes in
// action.Data (a stream-related app payload whose first byte is the
// ApplicationPayloadKind) and a separately-encoded ACK in action.ACK.
// These helpers decode action.Data to recover the same predicates the ksdk
// tests relied on.

// streamData decodes action.Data as a StreamData app packet. It returns nil
// when action.Data is empty or is not a StreamData frame (e.g. cancel,
// window-update, or an ACK-only action).
func streamData(action *trsf.SendAction) *wire.StreamPacket {
	if action == nil || len(action.Data) == 0 {
		return nil
	}
	if wire.ApplicationPayloadKind(action.Data[0]) != wire.ApplicationPayloadKind_StreamData {
		return nil
	}
	app := &wire.StreamAppPacket{}
	if err := app.DecodeExact(action.Data); err != nil {
		return nil
	}
	return app.StreamData()
}

// recvSkipProbe blocks until a SendAction is available whose data portion
// carries user payload — i.e. it is not an MTU probe and not a pendingOpen
// advertise frame (a 0-byte non-EOF STREAM frame that only materialises the
// stream entry on the peer). Probes/advertises are dropped silently; the trsf
// state machine emits each at most once per condition, so the loop terminates
// as soon as real data is queued.
func recvSkipProbe(ctx context.Context, t trsf.Transport) *trsf.SendAction {
	for {
		action := t.Recv(ctx)
		if action == nil {
			return nil
		}
		sd := streamData(action)
		if sd != nil && action.ACK == nil {
			if sd.IsProbe() {
				continue
			}
			if len(sd.Data) == 0 && !sd.IsEof() {
				continue // pendingOpen advertise frame
			}
		}
		return action
	}
}

func TestConn(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream.AppendData(true, []byte("Hello, World!"))
	pkt := recvSkipProbe(ctx, client)
	if pkt == nil {
		t.Fatal("client failed to receive packet")
	}
	if streamData(pkt) == nil {
		t.Fatal("client received non-stream-data packet")
	}
	if pkt.ACK != nil {
		t.Fatal("unexpected ACK packet received by client")
	}
	pkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: server})
	recvStream, err := server.AcceptReceiveStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept receive stream: %v", err)
	}
	if recvStream == nil {
		t.Fatal("received nil receive stream")
	}
	recvData := make([]byte, 13)
	n, err := recvStream.Read(recvData)
	if err != nil {
		t.Fatalf("failed to read from receive stream: %v", err)
	}
	if n != 13 {
		t.Fatalf("expected to read 13 bytes, got %d", n)
	}
	if string(recvData) != "Hello, World!" {
		t.Fatalf("data mismatch: expected 'Hello, World!', got '%s'", string(recvData))
	}
}

func TestBidirectionalStream(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	// 手動 pkt drain だと MTU probe / ACK / データの delivery 順を test 側で
	// 完全に追えないので BackgroundIO で双方向に流す。これで MTU probe が
	// データ packet として届いて test の終了条件を誤判定するバグも回避できる。
	mock.BackgroundIO(t, client, server)
	bidiStream := client.CreateBidirectionalStream()
	if bidiStream == nil {
		t.Fatal("failed to create bidirectional stream")
	}
	if err := bidiStream.AppendData(true, []byte("Hello from client!")); err != nil {
		t.Fatalf("failed to append data to client stream: %v", err)
	}
	acceptedBidiStream, err := server.AcceptBidirectionalStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept bidirectional stream: %v", err)
	}
	if acceptedBidiStream == nil {
		t.Fatal("received nil bidirectional stream")
	}
	recvData := make([]byte, 18)
	n, err := acceptedBidiStream.Read(recvData)
	if err != nil {
		t.Fatalf("failed to read from bidirectional stream: %v", err)
	}
	if n != 18 {
		t.Fatalf("expected to read 18 bytes, got %d", n)
	}
	if string(recvData) != "Hello from client!" {
		t.Fatalf("data mismatch: expected 'Hello from client!', got '%s'", string(recvData))
	}
	// send response from server to client
	if err := acceptedBidiStream.AppendData(true, []byte("Hello from server!")); err != nil {
		t.Fatalf("failed to append data to server stream: %v", err)
	}
	recvData = make([]byte, 18)
	n, err = bidiStream.Read(recvData)
	if err != nil {
		t.Fatalf("failed to read from bidirectional stream: %v", err)
	}
	if n != 18 {
		t.Fatalf("expected to read 18 bytes, got %d", n)
	}
	if string(recvData) != "Hello from server!" {
		t.Fatalf("data mismatch: expected 'Hello from server!', got '%s'", string(recvData))
	}
}

// simulate loss and retransmission
func TestLossAndRetransmission(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream.AppendData(true, []byte("Data that will be lost"))
	pkt := recvSkipProbe(ctx, client)
	if pkt == nil {
		t.Fatal("client failed to receive packet")
	}
	if streamData(pkt) == nil {
		t.Fatal("client received non-stream-data packet")
	}
	if pkt.ACK != nil {
		t.Fatal("unexpected ACK packet received by client")
	}
	// simulate sending but losing the packet
	pkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: nil})
	// retransmit the packet
	pkt = recvSkipProbe(ctx, client)
	if pkt == nil {
		t.Fatal("client failed to receive retransmitted packet")
	}
	if streamData(pkt) == nil {
		t.Fatal("client received non-stream-data retransmitted packet")
	}
	if pkt.ACK != nil {
		t.Fatal("unexpected ACK packet received by client on retransmission")
	}
	pkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: server})
	recvStream, err := server.AcceptReceiveStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept receive stream: %v", err)
	}
	if recvStream == nil {
		t.Fatal("received nil receive stream")
	}
	recvData := make([]byte, 22)
	n, err := recvStream.Read(recvData)
	if err != nil {
		t.Fatalf("failed to read from receive stream: %v", err)
	}
	if n != 22 {
		t.Fatalf("expected to read 22 bytes, got %d", n)
	}
	if string(recvData) != "Data that will be lost" {
		t.Fatalf("data mismatch: expected 'Data that will be lost', got '%s'", string(recvData))
	}
}

func TestLossTimer(t *testing.T) {
	// second send will success, first send will fail then retransmit
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream.AppendData(false, []byte("First packet data"))
	time.Sleep(1 * time.Second)
	sendStream.AppendData(true, []byte("Second packet data"))
	// first packet (to be lost)
	pkt1 := recvSkipProbe(ctx, client)
	if pkt1 == nil {
		t.Fatal("client failed to receive first packet")
	}
	// send but lose the first packet
	pkt1.Send(ctx, &mock.MockUnderlayingTransport{Peer: nil})
	// second packet (to be received)
	pkt2 := recvSkipProbe(ctx, client)
	if pkt2 == nil {
		t.Fatal("client failed to receive second packet")
	}
	pkt2.Send(ctx, &mock.MockUnderlayingTransport{Peer: server})
	recvStream, err := server.AcceptReceiveStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept receive stream: %v", err)
	}
	if recvStream == nil {
		t.Fatal("received nil receive stream")
	}
	if recvStream.HasRecvData() {
		t.Fatal("unexpected data available before retransmission")
	}
	// ack second packet to trigger loss detection
	servPkt := recvSkipProbe(ctx, server)
	if servPkt == nil {
		t.Fatal("server failed to receive packet")
	}
	servPkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: client})
	retrPkt := recvSkipProbe(ctx, client)
	if retrPkt == nil {
		t.Fatal("client failed to receive retransmitted packet")
	}
	retrPkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: server})
	recvData := make([]byte, 35)
	n, err := recvStream.Read(recvData)
	if err != nil {
		t.Fatalf("failed to read from receive stream: %v", err)
	}
	if n != 35 {
		t.Fatalf("expected to read 35 bytes, got %d", n)
	}
	if string(recvData) != "First packet dataSecond packet data" {
		t.Fatalf("data mismatch: expected 'First packet dataSecond packet data', got '%s'", string(recvData))
	}
	ackPkt := server.Recv(ctx)
	if ackPkt == nil {
		t.Fatal("server failed to receive final ACK packet")
	}
	if ackPkt.ACK == nil {
		t.Fatal("expected ACK packet not received by server")
	}
	ackPkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: client})
}

// send large data
func testLargeDataInLoss(t *testing.T, lossRate float64, dataSize int, logLevel slog.Level) {
	ctx := t.Context()
	client, server := mock.SetupClientServerEx(t, logLevel)
	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream2 := client.CreateSendStream()
	if sendStream2 == nil {
		t.Fatal("failed to create second send stream")
	}
	go func() {
		recvStream, err := server.AcceptReceiveStream(ctx)
		if err != nil {
			return
		}
		recvStream2, err := server.AcceptReceiveStream(ctx)
		if err != nil {
			return
		}
		go func() {
			io.Copy(io.Discard, recvStream)
		}()
		go func() {
			io.Copy(io.Discard, recvStream2)
		}()
		for {
			r := server.Recv(ctx)
			if r == nil {
				return
			}
			if rand.Float64() < lossRate {
				continue // loss
			}
			r.Send(ctx, &mock.MockUnderlayingTransport{Peer: client})
		}
	}()
	largeData := bytes.Repeat([]byte("A"), dataSize)
	sendStream.AppendData(true, largeData)
	sendStream2.AppendData(true, largeData)
	for !sendStream.Completed() || !sendStream2.Completed() {
		timeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		pkt := client.Recv(timeCtx)
		cancel()
		if pkt == nil {
			continue
		}
		pkt.Send(ctx, &mock.MockUnderlayingTransport{Peer: server})
	}
}

// 30% loss + 100MB は congestion recovery が長引いて CI 単位 timeout に
// 収まらない。回帰検出には 1MB で十分。手動で実行する場合は data size を
// 引き上げる。
func TestLargeDataInLoss(t *testing.T) {
	testLargeDataInLoss(t, 0.3, 1*1024*1024, slog.LevelWarn)
}

func TestLargeDataNormalLoss(t *testing.T) {
	testLargeDataInLoss(t, 0.01, 100*1024*1024, slog.LevelInfo)
}

func TestLargeDataNoLoss(t *testing.T) {
	testLargeDataInLoss(t, 0.0, 100*1024*1024, slog.LevelInfo)
}

func TestCancel(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	mock.BackgroundIO(t, client, server)
	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream.AppendData(false, []byte("Data before cancel"))
	recvStream, err := server.AcceptReceiveStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept receive stream: %v", err)
	}
	if recvStream == nil {
		t.Fatal("received nil receive stream")
	}
	go func() {
		for {
			_, err := sendStream.Write([]byte("More data"))
			if err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(500 * time.Millisecond)
	buf := make([]byte, 1024)
	_, err = recvStream.Read(buf)
	if err != nil {
		t.Fatalf("expect to read data before cancel, got error: %v", err)
	}
	recvStream.Cancel()
	time.Sleep(500 * time.Millisecond)
	_, err = recvStream.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF after cancel, got: %v", err)
	}
	_, err = sendStream.Write([]byte("Data after cancel"))
	if err != io.EOF {
		t.Fatalf("expected EOF on send stream after cancel, got: %v", err)
	}
}
