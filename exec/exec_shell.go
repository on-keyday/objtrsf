//go:build !js

package exec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"golang.org/x/term"
)

func (w *CommandExecutionStream) RemoteShell() error {
	old, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer term.Restore(int(os.Stdin.Fd()), old)

	// Hand the terminal back in the state a fresh shell expects. Which escapes
	// that takes, and the symptom each one answers, live on ScreenModeReset and
	// InputModeReset in terminal_modes.go — this call site only decides WHEN.
	//
	// LIFO order is load-bearing: this fires *before* the term.Restore above,
	// so the escapes go out while stdout is still flushing in raw mode without
	// line buffering.
	defer WriteTerminalReset(os.Stdout)

	// sendSize re-queries the local terminal dimensions and forwards them
	// over the control frame channel. Used both for the initial size and
	// for every SIGWINCH thereafter.

	if err := w.sendWindowSize(); err != nil {
		return err
	}

	// Window-size forwarding: when the local terminal resizes, push a
	// fresh TerminalWindowSize control frame so the runner-side PTY (and
	// claude inside it) sees the new dimensions and re-flows. Without
	// this, claude renders at the dimensions captured at attach time and
	// stays frozen for the rest of the session even if the user resizes
	// their terminal. Detection is platform-specific: SIGWINCH on Unix,
	// polling on Windows — see winsize_{unix,windows}.go.
	stopWinSize := startWindowSizeForwarder(w.sendWindowSize)
	defer stopWinSize()

	return w.PumpTerminalIO(os.Stdin, os.Stdout)
}

// stdinGoneGrace bounds how long the output direction may run on after the
// input pump has stopped. Detach waits for the server to close the stream back,
// which takes milliseconds; past this, no answer is coming. A var so tests can
// shorten it.
var stdinGoneGrace = 2 * time.Second

// swallowLocalReadEOF reports whether a local read error is a platform artefact
// that ate a keystroke rather than a genuine end of input. A var so tests can
// drive the path where the artefact does not exist.
var swallowLocalReadEOF = platformSwallowLocalReadEOF

// maxSwallowedReadEOF bounds consecutive swallows, so a source reporting the
// artefact without ever blocking ends the session instead of spinning.
var maxSwallowedReadEOF = 64

// ErrLocalInputLost reports that the input pump stopped while the remote side
// was still live, so the session was torn down rather than left input-dead.
var ErrLocalInputLost = errors.New("local input path closed")

// PumpTerminalIO splices in→runner and runner→out until either direction ends,
// intercepting the detach key on the way in.
//
// RemoteShell passes os.Stdin / os.Stdout. A front end that is NOT a local tty
// passes its own ends instead — the harness ssh gateway passes an ssh channel,
// and tests pass theirs. Nothing here touches a terminal: the caller owns raw
// mode, the window-size forwarder, and writing WriteTerminalReset on the way
// out, because each of those means something different when the terminal being
// served is at the far end of a network connection.
//
// Both directions must be able to end it: an input pump that dies alone leaves
// the caller blocked on the output copy with nothing reading the terminal, and
// raw mode means neither Ctrl+C nor the detach key can get out of that.
func (w *CommandExecutionStream) PumpTerminalIO(in io.Reader, out io.Writer) error {
	stdin := w.Stdin()
	stdout := w.Stdout()

	// Stdin → runner forward, with client-side detach key interception.
	//
	// detachByte = 0x1d (Ctrl+]) is swallowed at the client and triggers a
	// half-close of the bidi stream's send side via w.BidirectionalStream.Close().
	// The server's SessionMux.tuiPump sees ReadDirect return eof=true and
	// calls detachOnly, which CloseBoths the tui stream from the server side
	// but leaves the runner stream alive — for Detachable sessions the agent
	// (claude / bash / etc.) survives and is re-attachable. For
	// non-Detachable sessions the server has no SessionMux, so the half-close
	// cascades to runner teardown via the existing kill-on-disconnect path
	// — semantically equivalent to typing `exit` / Ctrl+D, which is fine.
	//
	// Why not stdinWrapper.Close()? That sends a 0-length Stdin frame, which
	// the runner forwards to the agent's stdin as EOF — bash exits, agent
	// dies even when the session was Detachable. The bidi-stream Close()
	// cuts at the transport layer instead.
	//
	// Choice of 0x1d: Ctrl+] is GS, used by telnet's escape and almost
	// nothing else in modern TUIs. In particular it is NOT 0x1b (Ctrl+[ =
	// ESC), which is the prefix of every terminal escape sequence and must
	// be passed through unmolested.
	//
	// Win32 Input Mode caveat: when the *runner* is Windows, ConPTY emits
	// `ESC [ ? 9001 h` to negotiate Win32 Input Mode with the connected
	// terminal. If the local terminal supports it (Windows Terminal,
	// conhost, recent mintty), Ctrl+] is then encoded as the multi-byte
	// CSI sequence `ESC [ <Vk> ; <Sc> ; <Uc> ; <Kd> ; <Cs> ; <Rc> _`
	// instead of raw 0x1d, where Uc is the resulting unicode codepoint
	// (29 for Ctrl+]) and Kd=1 is keydown. detachIndex below recognises
	// both encodings so the detach key works regardless of which side
	// of the WS the runner sits on. Spec:
	// https://github.com/microsoft/terminal/blob/main/doc/specs/%234999%20-%20Improved%20keyboard%20handling%20in%20Conpty.md
	const detachByte = 0x1d

	// stdinExit carries why the input pump stopped: nil for a detach, non-nil
	// for a failure. Buffered so the pump can always report and exit.
	stdinExit := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		swallowed := 0
		for {
			n, err := in.Read(buf)
			if n > 0 {
				swallowed = 0
				if start, _ := detachIndex(buf[:n]); start >= 0 {
					if start > 0 {
						_, _ = stdin.Write(buf[:start])
					}
					// Drop bytes [start, end) (the detach trigger itself);
					// any trailing bytes after `end` are also dropped — in
					// practice human input doesn't queue anything after a
					// dedicated detach keystroke.
					_ = w.BidirectionalStream.Close()
					stdinExit <- nil
					return
				}
				// On normal session termination the server CloseBoths the
				// stream; the next stdin.Write returns an error. Return so
				// this goroutine doesn't outlive RemoteShell and race
				// bubbletea (which reclaims stdin after tea.Exec) for
				// subsequent keystrokes — pre-f18919c the io.Copy form had
				// this exit on write error implicitly.
				if _, werr := stdin.Write(buf[:n]); werr != nil {
					stdinExit <- werr
					return
				}
			}
			if err != nil {
				// A Windows console reports Ctrl+Z as io.EOF; drop the
				// keystroke instead of ending the input path.
				if swallowLocalReadEOF(in, err) && swallowed < maxSwallowedReadEOF {
					swallowed++
					continue
				}
				stdinExit <- err
				return
			}
		}
	}()

	copyExit := make(chan error, 1)
	go func() {
		_, cerr := io.Copy(out, stdout)
		copyExit <- cerr
	}()

	select {
	case err := <-copyExit:
		return err
	case reason := <-stdinExit:
		select {
		case err := <-copyExit:
			return err
		case <-time.After(stdinGoneGrace):
		}
		// Unblock the output copy and join it: it writes to the caller's
		// terminal, which bubbletea reclaims as soon as tea.Exec sees us
		// return. Closing the read end does not touch the wire, so a detached
		// session stays alive and re-attachable.
		_ = w.stdoutPipe.Close()
		<-copyExit
		if reason != nil {
			return fmt.Errorf("%w: %w", ErrLocalInputLost, reason)
		}
		return nil
	}
}

// detachIndex scans buf for the first detach trigger and returns the
// [start, end) byte range covering the trigger, or (-1, -1) if none is
// present. Two encodings are recognised:
//
//  1. The raw byte 0x1d (GS = Ctrl+]), which is the default delivery in
//     every line-editing-disabled terminal mode (POSIX termios raw,
//     Windows console with ENABLE_VIRTUAL_TERMINAL_INPUT but no Win32
//     Input Mode).
//
//  2. A Win32 Input Mode keydown sequence whose Uc field is 29 (0x1d).
//     Format: `ESC [ <Vk> ; <Sc> ; <Uc> ; <Kd> ; <Cs> ; <Rc> _`. Win32
//     Input Mode is enabled when a runner-side Windows ConPTY emits the
//     `ESC [ ? 9001 h` request and the local terminal honours it (e.g.
//     Windows Terminal). Without case 2, Ctrl+] would be silently
//     forwarded as the multi-byte CSI to the runner, defeating detach.
//
// The earliest matching trigger wins. The (start, end) range is consumed
// (i.e., not forwarded to the runner); the prefix [0, start) is forwarded
// before the half-close.
func detachIndex(buf []byte) (start, end int) {
	rawIdx := bytes.IndexByte(buf, 0x1d)
	winStart, winEnd := scanWin32InputDetach(buf)
	switch {
	case rawIdx < 0 && winStart < 0:
		return -1, -1
	case rawIdx < 0:
		return winStart, winEnd
	case winStart < 0:
		return rawIdx, rawIdx + 1
	case rawIdx <= winStart:
		return rawIdx, rawIdx + 1
	default:
		return winStart, winEnd
	}
}

// scanWin32InputDetach finds the first Win32 Input Mode keydown sequence
// in buf with Uc=29 (Ctrl+]). Returns the byte range of the whole CSI
// sequence (including the leading ESC [ and trailing _), or (-1, -1).
//
// The scanner is conservative: it only consumes a candidate sequence if it
// matches the strict Win32 Input Mode shape (six decimal fields separated
// by ';' terminated by '_'). Any other byte aborts the candidate so that
// regular ANSI sequences from the runner-side stdout (which transit through
// the agent's stdin only when a TUI agent re-echoes them, an unusual case)
// are not misinterpreted as detach triggers.
func scanWin32InputDetach(buf []byte) (start, end int) {
	for i := 0; i+2 < len(buf); i++ {
		if buf[i] != 0x1b || buf[i+1] != '[' {
			continue
		}
		// Look ahead for the '_' terminator. Bound the scan so we don't
		// chew through a long unrelated CSI (the longest realistic Win32
		// Input Mode payload is on the order of 24 bytes).
		const maxFieldsBytes = 64
		j := i + 2
		limit := j + maxFieldsBytes
		if limit > len(buf) {
			limit = len(buf)
		}
		ok := false
		for ; j < limit; j++ {
			c := buf[j]
			if c == '_' {
				ok = true
				break
			}
			if c != ';' && (c < '0' || c > '9') {
				break // not a Win32 Input Mode payload — bail
			}
		}
		if !ok {
			continue
		}
		// Parse "Vk;Sc;Uc;Kd;Cs;Rc" — exactly 6 decimal fields.
		fields := bytes.Split(buf[i+2:j], []byte{';'})
		if len(fields) != 6 {
			continue
		}
		uc, errU := strconv.Atoi(string(fields[2]))
		kd, errK := strconv.Atoi(string(fields[3]))
		if errU != nil || errK != nil {
			continue
		}
		if uc == 0x1d && kd == 1 {
			return i, j + 1
		}
	}
	return -1, -1
}
