//go:build windows

package transport

import (
	"net"

	"golang.org/x/sys/windows"
)

// Windows has no probe-mode equivalent of Linux's IP_PMTUDISC_PROBE; it only
// exposes a plain DF-bit toggle. IP_DONTFRAGMENT / IPV6_DONTFRAG are not
// exported by x/sys/windows, so the raw option values from ws2ipdef.h are used.
const (
	winIP_DONTFRAGMENT = 14
	winIPV6_DONTFRAG   = 14
)

// tunePMTUDProbe sets the DF bit on the UDP socket so the kernel emits
// non-fragmentable datagrams and surfaces WSAEMSGSIZE (mapped to
// syscall.EMSGSIZE by the net layer) instead of silently fragmenting,
// letting the upper layer drive its own PLPMTUD search. This is the DF-only
// counterpart of the Linux probe-mode tuning — ICMP-driven PMTU clamping is
// left to the OS default.
//
// Best-effort: SetsockoptInt errors are dropped; a socket that rejects the
// option just falls back to the OS default path-MTU behavior.
func tunePMTUDProbe(conn *net.UDPConn) error {
	sys, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return sys.Control(func(fd uintptr) {
		windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IP, winIP_DONTFRAGMENT, 1)
		windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IPV6, winIPV6_DONTFRAG, 1)
	})
}
