package objproto

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// Drives enough traffic through a real pair to cross the update threshold many
// times, and checks every byte survives. Lowering the threshold is what makes
// this cheap enough to run in the normal suite.
func TestBulkTransferAcrossManyKeyUpdates(t *testing.T) {
	origPackets, origFloor, origTime := keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates
	keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates = 64, 8, 0
	t.Cleanup(func() {
		keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates = origPackets, origFloor, origTime
	})

	p := newConnectedPair(t)
	const chunks = 2000
	payload := make([]byte, 512)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	var sent, got bytes.Buffer
	for i := 0; i < chunks; i++ {
		if _, _, err := p.client.endpoint.sendApplication(p.client.cid, payload, p.client, nil); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		sent.Write(payload)
		p.pump()
		msg, err := p.server.ReceiveMessage()
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		got.Write(msg.Data)
	}
	if !bytes.Equal(sent.Bytes(), got.Bytes()) {
		t.Fatal("payload corrupted across key updates")
	}
	p.server.mu.Lock()
	defer p.server.mu.Unlock()
	if p.server.recvPhase < 10 {
		t.Fatalf("only %d key updates over %d packets", p.server.recvPhase, chunks)
	}
	t.Logf("%d key updates over %d packets, %d bytes intact", p.server.recvPhase, chunks, got.Len())
}
