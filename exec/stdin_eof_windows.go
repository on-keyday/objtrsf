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
// Go's runtime translates 0x1A on a console handle into a zero-length read
// (internal/poll readConsole stops at 0x1A and consumes it), which os.File.Read
// then reports as io.EOF because poll.FD.ZeroReadIsEOF is set for every Windows
// file, console included. term.MakeRaw cannot prevent it: the translation lives
// in Go userspace, not in the console mode flags, so no combination of
// ENABLE_* flags suppresses it. os.File.Read passes io.EOF through unwrapped,
// so the value is exactly io.EOF and not a *os.PathError.
//
// Without this, one Ctrl+Z ends the input pump. Confirmed with a debugger on a
// live session: the keystroke hit the pump's read-error branch while the trsf
// stream stayed pristine — no EOF, no cancel, the demux still parked waiting
// for frames — and the terminal went input-dead with output still scrolling.
//
// The keystroke is dropped, not re-synthesised as a 0x1A on the wire. A
// forwarded 0x1A reaches the remote PTY, and when the remote agent is the
// direct PTY child in cooked mode it takes SIGTSTP with no job-control shell
// able to resume it; the SendSignal control frame is the out-of-band path for
// callers that genuinely need to deliver a signal.
//
// Narrowed to console handles on purpose: on a pipe an EOF is genuine, and
// honouring it keeps redirected stdin working.
func platformSwallowLocalReadEOF(in io.Reader, err error) bool {
	if !errors.Is(err, io.EOF) {
		return false
	}
	f, ok := in.(*os.File)
	if !ok {
		return false
	}
	// On Windows term.IsTerminal is GetConsoleMode, i.e. exactly the
	// console-handle test that decides whether readConsole was used at all.
	return term.IsTerminal(int(f.Fd()))
}
