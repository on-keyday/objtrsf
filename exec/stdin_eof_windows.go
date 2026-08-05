//go:build windows

package exec

import (
	"errors"
	"io"
	"os"

	"golang.org/x/term"
)

// platformSwallowLocalReadEOF recognises the Windows console's Ctrl+Z artefact.
//
// Go turns 0x1A on a console handle into a zero-length read (internal/poll
// readConsole stops there and consumes it), which os.File.Read reports as
// io.EOF because poll.FD.ZeroReadIsEOF is set for every Windows file. It is a
// Go-userspace translation, so no console mode flag suppresses it — one Ctrl+Z
// used to end the input pump outright.
//
// The keystroke is dropped, not re-sent as 0x1A: forwarded, it would SIGTSTP a
// cooked-mode remote agent that has no job-control shell to resume it. Narrowed
// to console handles because an EOF on a pipe is genuine.
func platformSwallowLocalReadEOF(in io.Reader, err error) bool {
	if !errors.Is(err, io.EOF) {
		return false
	}
	f, ok := in.(*os.File)
	if !ok {
		return false
	}
	// term.IsTerminal is GetConsoleMode here — the console-handle test.
	return term.IsTerminal(int(f.Fd()))
}
