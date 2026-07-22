//go:build !windows

package transport

import (
	"errors"
	"syscall"
)

// isMessageTooBig reports whether a UDP send failed because the datagram
// exceeded the path/interface MTU while the DF bit was set (EMSGSIZE). The
// upper layer's PLPMTUD treats this as a probe result to shrink from, not a
// fatal send error.
func isMessageTooBig(err error) bool {
	return errors.Is(err, syscall.EMSGSIZE)
}
