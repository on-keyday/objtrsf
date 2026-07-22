//go:build !linux && !windows && !darwin && !freebsd

package transport

import "net"

// tunePMTUDProbe is a no-op on platforms without a portable DF-bit knob.
// Linux uses probe-mode IP_MTU_DISCOVER; Windows and Darwin/FreeBSD set the
// DF bit directly (see the respective udp_pmtud_*.go files). Everything else
// falls back to the OS default path-MTU behavior.
func tunePMTUDProbe(_ *net.UDPConn) error { return nil }
