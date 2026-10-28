package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"testing/iotest"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
)

func encodedPacket(t *testing.T, payload []byte) []byte {
	t.Helper()
	p := &packet.Packet{}
	p.Header.Len = uint16(len(payload))
	if !p.SetData(payload) {
		t.Fatal("SetData refused")
	}
	b, err := p.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Two packets written back to back, read one byte at a time, come out as the
// same two packets: the header's len alone delimits them.
func TestReadPacketSplitsBackToBackPackets(t *testing.T) {
	a := encodedPacket(t, []byte("first"))
	b := encodedPacket(t, bytes.Repeat([]byte{0xee}, 3000))
	r := iotest.OneByteReader(bytes.NewReader(append(append([]byte{}, a...), b...)))
	for i, want := range [][]byte{a, b} {
		got, err := readPacket(r)
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("packet %d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
	if _, err := readPacket(r); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last packet: err = %v, want EOF", err)
	}
}

func TestReadPacketTruncatedBodyIsUnexpectedEOF(t *testing.T) {
	p := encodedPacket(t, []byte("truncated"))
	if _, err := readPacket(bytes.NewReader(p[:len(p)-2])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}

// roundTrip handshakes cli to dst, then sends a message each way.
func roundTrip(t *testing.T, srv, cli objproto.Endpoint, dst objproto.ConnectionID) (cliConn, srvConn objproto.Connection) {
	t.Helper()
	cliConn, err := handshake(t, cli, dst, 3*time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	srvConn, err = srv.WaitNewActiveConnection(3 * time.Second)
	if err != nil {
		t.Fatalf("server saw no connection: %v", err)
	}
	if _, _, err := cliConn.SendMessage([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	m, err := srvConn.ReceiveMessageTimeout(t.Context(), 3*time.Second)
	if err != nil || string(m.Data) != "ping" {
		t.Fatalf("server received %v, %v", m, err)
	}
	if _, _, err := srvConn.SendMessage([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	m, err = cliConn.ReceiveMessageTimeout(t.Context(), 3*time.Second)
	if err != nil || string(m.Data) != "pong" {
		t.Fatalf("client received %v, %v", m, err)
	}
	return cliConn, srvConn
}

func tcpAddrOf(c net.Conn) (netip.AddrPort, error) {
	return netip.ParseAddrPort(c.RemoteAddr().String())
}

func tcpDial(ctx context.Context, addr netip.AddrPort) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr.String())
}

// The stream leg is not vsock-specific: over TCP it carries a handshake and
// messages both ways, with both ends labelling the connection by the leg.
func TestStreamLegOverTCP(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeServer,
		StreamLeg(StreamConfig{Logger: quietLogger(), Transport: "tcp", Listener: l, AddrOf: tcpAddrOf}))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeClient,
		StreamLeg(StreamConfig{Logger: quietLogger(), Transport: "tcp", Dial: tcpDial}))
	if err != nil {
		t.Fatal(err)
	}
	cliConn, srvConn := roundTrip(t, srv, cli,
		objproto.NewConnectionID("tcp", netip.MustParseAddrPort(l.Addr().String()), 0x4001))
	if cliConn.ConnectionID().Transport != "tcp" || srvConn.ConnectionID().Transport != "tcp" {
		t.Errorf("transports = %q / %q, want tcp on both ends",
			cliConn.ConnectionID().Transport, srvConn.ConnectionID().Transport)
	}
}

// A peer's stream ending is noticed as soon as it happens, not by the
// garbage collector's timeout.
func TestStreamLegClosesConnectionWhenStreamEnds(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeServer,
		StreamLeg(StreamConfig{Logger: quietLogger(), Transport: "tcp", Listener: l, AddrOf: tcpAddrOf}))
	if err != nil {
		t.Fatal(err)
	}
	var dialed net.Conn
	cli, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeClient,
		StreamLeg(StreamConfig{Logger: quietLogger(), Transport: "tcp", Dial: func(ctx context.Context, a netip.AddrPort) (net.Conn, error) {
			c, err := tcpDial(ctx, a)
			dialed = c
			return c, err
		}}))
	if err != nil {
		t.Fatal(err)
	}
	_, srvConn := roundTrip(t, srv, cli,
		objproto.NewConnectionID("tcp", netip.MustParseAddrPort(l.Addr().String()), 0x4002))
	dialed.Close()
	select {
	case <-srvConn.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("server connection outlived its stream")
	}
}

func TestStreamLegModeContract(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if _, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeClient,
		StreamLeg(StreamConfig{Transport: "tcp", Listener: l, AddrOf: tcpAddrOf})); err == nil {
		t.Error("a Client endpoint accepted a Listener")
	}
	if _, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeServer,
		StreamLeg(StreamConfig{Transport: "tcp", Dial: tcpDial})); err == nil {
		t.Error("a Server endpoint started with nothing to accept from")
	}
	if _, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeServer,
		StreamLeg(StreamConfig{Transport: "tcp", Listener: l})); err == nil {
		t.Error("a Listener without AddrOf was accepted")
	}
}

func TestMultiLegEndpointRefusesTwoLegsForOneTransport(t *testing.T) {
	_, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeClient,
		StreamLeg(StreamConfig{Transport: "tcp", Dial: tcpDial}),
		StreamLeg(StreamConfig{Transport: "tcp", Dial: tcpDial}))
	if err == nil {
		t.Fatal("two legs for tcp were accepted")
	}
}
