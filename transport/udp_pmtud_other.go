//go:build !linux

package transport

import "net"

// tunePMTUDProbe is a no-op outside Linux. The probe-mode IP_MTU_DISCOVER /
// IPV6_MTU_DISCOVER options used by the Linux variant are not portable;
// other platforms fall back to the OS default path-MTU behavior.
func tunePMTUDProbe(_ *net.UDPConn) error { return nil }
