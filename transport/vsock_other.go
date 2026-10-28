//go:build !linux

package transport

import (
	"context"
	"errors"
	"net"
)

var errVsockUnsupported = errors.New("vsock is only supported on linux")

// DialVsock is unsupported off Linux.
func DialVsock(ctx context.Context, cid, port uint32) (net.Conn, error) {
	return nil, errVsockUnsupported
}

// ListenVsock is unsupported off Linux.
func ListenVsock(port uint32) (net.Listener, error) {
	return nil, errVsockUnsupported
}
