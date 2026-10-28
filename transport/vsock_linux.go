//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// The standard library has no AF_VSOCK, so a socket is made with x/sys/unix
// and wrapped in an *os.File: a non-blocking fd handed to os.NewFile joins the
// runtime poller, which gives Read/Write/deadlines without a goroutine per
// blocked call. The fd is registered before connect/accept runs on it, so a
// readiness edge cannot arrive before the poller is watching.

func vsockSocket() (*os.File, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	return os.NewFile(uintptr(fd), "vsock"), nil
}

func vsockAddrOf(sa unix.Sockaddr) VsockAddr {
	if vm, ok := sa.(*unix.SockaddrVM); ok {
		return VsockAddr{CID: vm.CID, Port: vm.Port}
	}
	return VsockAddr{}
}

func vsockLocalAddr(f *os.File) VsockAddr {
	var local VsockAddr
	if rc, err := f.SyscallConn(); err == nil {
		rc.Control(func(fd uintptr) {
			if sa, err := unix.Getsockname(int(fd)); err == nil {
				local = vsockAddrOf(sa)
			}
		})
	}
	return local
}

type vsockConn struct {
	*os.File
	local, remote VsockAddr
}

func (c *vsockConn) LocalAddr() net.Addr  { return c.local }
func (c *vsockConn) RemoteAddr() net.Addr { return c.remote }

// DialVsock connects to (cid, port), giving up when ctx is done.
func DialVsock(ctx context.Context, cid, port uint32) (net.Conn, error) {
	f, err := vsockSocket()
	if err != nil {
		return nil, err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return nil, err
	}
	var cerr error
	if err := rc.Control(func(fd uintptr) {
		cerr = unix.Connect(int(fd), &unix.SockaddrVM{CID: cid, Port: port})
	}); err != nil {
		f.Close()
		return nil, err
	}
	if errors.Is(cerr, unix.EINPROGRESS) {
		if dl, ok := ctx.Deadline(); ok {
			f.SetWriteDeadline(dl)
		}
		stop := context.AfterFunc(ctx, func() { f.SetWriteDeadline(time.Unix(1, 0)) })
		waited := false
		werr := rc.Write(func(fd uintptr) bool {
			if !waited {
				// The first call only asks the poller to wait for writability,
				// which is how a non-blocking connect reports completion.
				waited = true
				return false
			}
			v, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ERROR)
			switch {
			case err != nil:
				cerr = err
			case v != 0:
				cerr = unix.Errno(v)
			default:
				cerr = nil
			}
			return true
		})
		stop()
		if werr != nil {
			f.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, werr
		}
		f.SetWriteDeadline(time.Time{})
	}
	if cerr != nil {
		f.Close()
		return nil, os.NewSyscallError("connect", cerr)
	}
	return &vsockConn{File: f, local: vsockLocalAddr(f), remote: VsockAddr{CID: cid, Port: port}}, nil
}

type vsockListener struct {
	f      *os.File
	addr   VsockAddr
	closed atomic.Bool
}

// ListenVsock accepts on port from any CID.
func ListenVsock(port uint32) (net.Listener, error) {
	f, err := vsockSocket()
	if err != nil {
		return nil, err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return nil, err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		if err := unix.Bind(int(fd), &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: port}); err != nil {
			serr = os.NewSyscallError("bind", err)
			return
		}
		if err := unix.Listen(int(fd), unix.SOMAXCONN); err != nil {
			serr = os.NewSyscallError("listen", err)
		}
	}); err != nil {
		f.Close()
		return nil, err
	}
	if serr != nil {
		f.Close()
		return nil, serr
	}
	return &vsockListener{f: f, addr: vsockLocalAddr(f)}, nil
}

func (l *vsockListener) Accept() (net.Conn, error) {
	rc, err := l.f.SyscallConn()
	if err != nil {
		return nil, l.mapErr(err)
	}
	var nfd int
	var sa unix.Sockaddr
	var aerr error
	if err := rc.Read(func(fd uintptr) bool {
		nfd, sa, aerr = unix.Accept4(int(fd), unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		return !errors.Is(aerr, unix.EAGAIN)
	}); err != nil {
		return nil, l.mapErr(err)
	}
	if aerr != nil {
		return nil, os.NewSyscallError("accept4", aerr)
	}
	f := os.NewFile(uintptr(nfd), "vsock")
	return &vsockConn{File: f, local: vsockLocalAddr(f), remote: vsockAddrOf(sa)}, nil
}

// mapErr reports net.ErrClosed once Close has run, which is what an accept
// loop waits for; os does not use that error for a closed file.
func (l *vsockListener) mapErr(err error) error {
	if l.closed.Load() {
		return net.ErrClosed
	}
	return err
}

func (l *vsockListener) Close() error {
	l.closed.Store(true)
	return l.f.Close()
}

func (l *vsockListener) Addr() net.Addr { return l.addr }
