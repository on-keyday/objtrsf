//go:build windows

package transport

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// isMessageTooBig reports whether a UDP send failed because the datagram
// exceeded the path/interface MTU while the DF bit was set.
//
// On Windows the socket returns WSAEMSGSIZE (syscall.Errno 10040). The Go
// syscall package's EMSGSIZE is an unrelated APPLICATION_ERROR (1<<29)+iota
// value there and never matches a real Winsock error, so WSAEMSGSIZE must be
// checked explicitly — otherwise the first oversized PLPMTUD probe falls
// through to the fatal-send path and tears down the connection. The EMSGSIZE
// arm is kept as a harmless belt-and-suspenders.
func isMessageTooBig(err error) bool {
	return errors.Is(err, windows.WSAEMSGSIZE) || errors.Is(err, syscall.EMSGSIZE)
}
