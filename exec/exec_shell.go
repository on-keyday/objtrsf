//go:build !js

package exec

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/term"
)

func (w *CommandExecutionStream) RemoteShell() error {
	old, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer term.Restore(int(os.Stdin.Fd()), old)

	// Restore terminal-emulator-level state that the runner-side agent (or its
	// ConPTY) may have left enabled, which term.Restore above does NOT cover:
	// term.Restore only resets the kernel termios line discipline (echo,
	// canonical mode, signals), not the emulator's screen/cursor/input modes,
	// which are driven purely by escape sequences. Two groups:
	//
	//   1. Input modes the ConPTY negotiated at attach: `\x1b[?9001h` Win32
	//      Input Mode and `\x1b[>4;1m` modifyOtherKeys. When the runner is
	//      Windows and the local terminal honours them (Windows Terminal,
	//      conhost, recent mintty), without this a *detach* leaves every
	//      subsequent keystroke encoded as a multi-byte CSI — so a later
	//      attach to a Linux runner whose bash readline can't parse them makes
	//      lowercase input "vanish".
	//
	//   2. Screen state a full-screen TUI (htop, less, vim, man …) set and
	//      never got to tear down: alternate screen buffer (`\x1b[?1049h`),
	//      hidden cursor (`\x1b[?25l`), mouse reporting (`\x1b[?1000h` /
	//      1002 / 1003 / 1006), bracketed paste (`\x1b[?2004h`), and stray
	//      SGR colour. If the user hits Ctrl+] while such an app is still
	//      running, the app is detached before its atexit cleanup runs, so the
	//      LOCAL terminal is left with those modes set. Two callers, two
	//      symptoms:
	//        - bare CLI attach (no host TUI): the terminal is stranded on the
	//          alternate screen with the cursor hidden — it goes blank
	//          ("nothing displayed").
	//        - the bubbletea host TUI (tea.Exec): bubbletea exits its OWN alt
	//          screen before running us and re-enters it after (ReleaseTerminal
	//          / RestoreTerminal). htop's un-torn-down `\x1b[?1049h` means
	//          bubbletea's re-enter `\x1b[?1049h` fires while the terminal is
	//          already on an alt buffer, so on some emulators (notably Windows
	//          conhost / Windows Terminal) the repaint doesn't start from a
	//          clean buffer and stale panel lines survive. Emitting `?1049l`
	//          here restores primary-screen parity so bubbletea's re-enter is a
	//          clean primary→alt toggle; it also clears the leaked mouse
	//          reporting that bubbletea's RestoreTerminal does not re-disable.
	//      (On reattach the server's modeTracker deliberately does NOT replay
	//      alt-screen, so folding it on detach keeps the two paths consistent.)
	//
	// The natural-`exit` path is unaffected: closing the agent/ConPTY emits
	// these resets itself, and re-emitting them is idempotent on a terminal
	// already in the default state — so emitting unconditionally is safe.
	// LIFO order: this fires *before* term.Restore so the escape goes out
	// while stdout is still flushing in raw mode without line buffering.
	// \x1b[r resets the scroll region (DECSTBM) to the full window and \x1b[?6l
	// resets origin mode (DECOM). A full-screen app (htop) sets a partial
	// scroll region while running; if it is detached before tearing down, that
	// region persists on the LOCAL terminal and confines/mis-positions all
	// subsequent output — for a bubbletea host TUI this looks like panels
	// shifted up with a blank lower half (NOT a size bug; verified on the
	// Windows ConPTY repro — the reported size stays correct throughout, only
	// the scroll region is wrong). Resetting both is idempotent on a terminal
	// already at defaults.
	defer fmt.Fprint(os.Stdout, "\x1b[?9001l\x1b[>4;0m"+
		"\x1b[?1049l\x1b[?25h\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l\x1b[?2004l\x1b[r\x1b[?6l\x1b[0m")

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

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				if start, _ := detachIndex(buf[:n]); start >= 0 {
					if start > 0 {
						_, _ = stdin.Write(buf[:start])
					}
					// Drop bytes [start, end) (the detach trigger itself);
					// any trailing bytes after `end` are also dropped — in
					// practice human input doesn't queue anything after a
					// dedicated detach keystroke.
					_ = w.BidirectionalStream.Close()
					return
				}
				// On normal session termination the server CloseBoths the
				// stream; the next stdin.Write returns an error. Return so
				// this goroutine doesn't outlive RemoteShell and race
				// bubbletea (which reclaims stdin after tea.Exec) for
				// subsequent keystrokes — pre-f18919c the io.Copy form had
				// this exit on write error implicitly.
				if _, werr := stdin.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	_, err = io.Copy(os.Stdout, stdout)
	return err
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
