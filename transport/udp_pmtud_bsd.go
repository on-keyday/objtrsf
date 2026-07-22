//go:build darwin || freebsd

package transport

import (
	"net"

	"golang.org/x/sys/unix"
)

// Darwin and FreeBSD have no probe-mode equivalent of Linux's
// IP_PMTUDISC_PROBE; they only expose a plain DF-bit toggle via
// IP_DONTFRAG / IPV6_DONTFRAG.
//
// tunePMTUDProbe sets the DF bit so the kernel emits non-fragmentable
// datagrams and surfaces EMSGSIZE instead of silently fragmenting, letting
// the upper layer drive its own PLPMTUD search. DF-only counterpart of the
// Linux probe-mode tuning — ICMP-driven PMTU clamping is left to the OS
// default.
//
// Best-effort: SetsockoptInt errors are dropped; a socket that rejects the
// option just falls back to the OS default path-MTU behavior.
func tunePMTUDProbe(conn *net.UDPConn) error {
	sys, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return sys.Control(func(fd uintptr) {
		unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_DONTFRAG, 1)
		unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_DONTFRAG, 1)
	})
}
