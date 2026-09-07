//go:build !linux && !windows && !darwin && !freebsd

package transport

import "net"

// tunePMTUDProbe is a no-op on platforms without a portable DF-bit knob.
// Linux uses probe-mode IP_MTU_DISCOVER; Windows and Darwin/FreeBSD set the
// DF bit directly (see the respective udp_pmtud_*.go files). Everything else
// falls back to the OS default path-MTU behavior.
//
// That fallback breaks PLPMTUD in the direction that does not announce
// itself. With no DF bit the kernel is free to fragment, so an oversized probe
// is delivered and ACKED, and the tracker reads that as "the path carries this
// size" -- an estimate ABOVE the real path MTU, after which every data packet
// at that size is silently fragmented too. A too-small estimate costs
// throughput; this one costs it invisibly, and there is no signal here to
// detect it from. The build list above is the set of platforms where the
// search is trustworthy; js/wasm is in this bucket but never reaches it,
// because net.ListenUDP does not work there at all.
func tunePMTUDProbe(_ *net.UDPConn) error { return nil }
