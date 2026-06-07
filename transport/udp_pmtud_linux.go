//go:build linux

package transport

import (
	"net"
	"syscall"
)

// tunePMTUDProbe sets the PLPMTUD probe-mode socket options
// (IP_MTU_DISCOVER / IPV6_MTU_DISCOVER = IP_PMTUDISC_PROBE) so the kernel
// emits packets with DF set and surfaces EMSGSIZE instead of black-holing,
// letting the upper layer drive its own PLPMTUD search.
//
// Best-effort: SetsockoptInt errors are dropped — older kernels or
// restricted environments that reject these options just fall back to the
// kernel default (IP_PMTUDISC_WANT on most distros).
func tunePMTUDProbe(conn *net.UDPConn) error {
	sys, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return sys.Control(func(fd uintptr) {
		syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE)
		syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE)
	})
}
