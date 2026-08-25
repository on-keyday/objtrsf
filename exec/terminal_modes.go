//go:build !js

package exec

import "io"

// ScreenModeReset restores terminal-emulator-level state that the runner-side
// agent (or its ConPTY) may have left enabled, which term.Restore does NOT
// cover: term.Restore only resets the kernel termios line discipline (echo,
// canonical mode, signals), not the emulator's screen/cursor/input modes,
// which are driven purely by escape sequences. Two groups:
//
//  1. Input modes the ConPTY negotiated at attach: `\x1b[?9001h` Win32
//     Input Mode and `\x1b[>4;1m` modifyOtherKeys. When the runner is
//     Windows and the local terminal honours them (Windows Terminal,
//     conhost, recent mintty), without this a *detach* leaves every
//     subsequent keystroke encoded as a multi-byte CSI — so a later
//     attach to a Linux runner whose bash readline can't parse them makes
//     lowercase input "vanish".
//
//  2. Screen state a full-screen TUI (htop, less, vim, man …) set and
//     never got to tear down: alternate screen buffer (`\x1b[?1049h`),
//     hidden cursor (`\x1b[?25l`), mouse reporting (`\x1b[?1000h` /
//     1002 / 1003 / 1006), bracketed paste (`\x1b[?2004h`), and stray
//     SGR colour. If the user hits Ctrl+] while such an app is still
//     running, the app is detached before its atexit cleanup runs, so the
//     terminal is left with those modes set. Two callers, two symptoms:
//     - bare CLI attach (no host TUI): the terminal is stranded on the
//     alternate screen with the cursor hidden — it goes blank
//     ("nothing displayed").
//     - the bubbletea host TUI (tea.Exec): bubbletea exits its OWN alt
//     screen before running us and re-enters it after (ReleaseTerminal
//     / RestoreTerminal). htop's un-torn-down `\x1b[?1049h` means
//     bubbletea's re-enter `\x1b[?1049h` fires while the terminal is
//     already on an alt buffer, so on some emulators (notably Windows
//     conhost / Windows Terminal) the repaint doesn't start from a
//     clean buffer and stale panel lines survive. Emitting `?1049l`
//     here restores primary-screen parity so bubbletea's re-enter is a
//     clean primary→alt toggle; it also clears the leaked mouse
//     reporting that bubbletea's RestoreTerminal does not re-disable.
//     (On reattach the harness server's mode tracker deliberately does NOT
//     replay alt-screen, so folding it on detach keeps the two paths
//     consistent.)
//
// The natural-`exit` path is unaffected: closing the agent/ConPTY emits
// these resets itself, and re-emitting them is idempotent on a terminal
// already in the default state — so emitting unconditionally is safe.
//
// `\x1b[r` resets the scroll region (DECSTBM) to the full window and `\x1b[?6l`
// resets origin mode (DECOM). A full-screen app (htop) sets a partial scroll
// region while running; if it is detached before tearing down, that region
// persists on the terminal and confines/mis-positions all subsequent output —
// for a bubbletea host TUI this looks like panels shifted up with a blank lower
// half (NOT a size bug; verified on the Windows ConPTY repro — the reported
// size stays correct throughout, only the scroll region is wrong). Resetting
// both is idempotent on a terminal already at defaults.
const ScreenModeReset = "" +
	"\x1b[?9001l" + // Win32 Input Mode off
	"\x1b[>4;0m" + // modifyOtherKeys off
	"\x1b[?1049l" + // leave the alternate screen buffer
	"\x1b[?25h" + // DECTCEM: show the cursor
	"\x1b[?1000l" + // mouse: button press/release
	"\x1b[?1002l" + // mouse: button-event tracking
	"\x1b[?1003l" + // mouse: any-event tracking
	"\x1b[?1006l" + // mouse: SGR coordinates
	"\x1b[?2004l" + // bracketed paste
	"\x1b[r" + // DECSTBM: scroll region back to the full window
	"\x1b[?6l" + // DECOM: origin mode
	"\x1b[0m" // SGR reset

// InputModeReset stops the local terminal from SENDING things, and is written
// to it when a client hands the terminal back after an attach.
//
// Why a client has to do this at all. The harness server re-establishes the
// session's DEC private modes on every attach (its mode tracker emits a
// preamble), because a mode whose controlling sequence has scrolled out of the
// ring would otherwise be lost to the reattaching emulator. That replay does
// not distinguish modes that change how the screen LOOKS from modes that change
// what the terminal SENDS — so attaching to a session whose app enabled mouse
// tracking or bracketed paste turns those on in the operator's own terminal.
// Nothing turned them off again: leaving raw mode restores termios, which is
// the kernel line discipline and has no bearing on emulator state.
//
// The observed result, after detaching from a session that had run a
// mouse-using, palette-probing TUI: the terminal kept emitting SGR mouse
// reports at the operator's shell prompt, where readline consumed the `ESC[<`
// introducer as an unbound key sequence, rang the bell and self-inserted the
// remainder — `35;65;36M` — on every mouse movement. Reproduced by feeding
// those exact bytes to a bash pty.
//
// Reset unconditionally rather than from tracked state: the client does not
// know what was set, resetting an unset mode is a no-op, and the point is to
// leave the terminal in the state a fresh shell expects however it got here.
// This is what any full-screen program does on exit, and what `tmux detach`
// does — the harness was simply not doing it.
//
// Screen-affecting modes are deliberately NOT in THIS list. Cursor visibility,
// autowrap and the alternate screen decide what the operator SEES; clearing
// them here would be a display change nobody asked for, and the alternate
// screen in particular is content, not a flag (the harness server's mode
// tracker draws the same boundary, excluding it from its preamble). They are
// handled by ScreenModeReset instead, on the different grounds documented
// there: an app detached mid-run never got to tear them down.
const InputModeReset = "" +
	"\x1b[?1l" + // DECCKM: cursor keys back to normal (not application) encoding
	"\x1b[?9l" + // X10 mouse reporting
	"\x1b[?66l" + // DECNKM: numeric keypad
	"\x1b[?1000l" + // X11 mouse: button press/release
	"\x1b[?1001l" + // highlight mouse tracking
	"\x1b[?1002l" + // button-event tracking (motion while pressed)
	"\x1b[?1003l" + // any-event tracking (motion always) — the noisiest one
	"\x1b[?1004l" + // focus in/out reporting
	"\x1b[?1005l" + // UTF-8 mouse coordinates
	"\x1b[?1006l" + // SGR mouse coordinates
	"\x1b[?1015l" + // urxvt mouse coordinates
	"\x1b[?1016l" + // SGR-pixel mouse coordinates
	"\x1b[?2004l" + // bracketed paste
	"\x1b[?2031l" // colour-scheme change notifications

// WriteTerminalReset writes the full reset — both groups — to w.
//
// It exists so that "what a full reset is" has one definition. RemoteShell
// passes os.Stdout; a front end that is not a local tty (the harness ssh
// gateway) passes the channel that reaches its client's terminal, which is the
// only pipe it has to that terminal. A third group added here later reaches
// both without anyone having to remember the second call site.
//
// The two groups overlap — the mouse modes and bracketed paste are in each.
// Turning off a mode that is already off is a no-op, so the overlap needs no
// reconciling and the order within it does not matter.
//
// Errors are ignored on purpose: this runs on the way out, the terminal may
// already be gone, and there is nothing a caller could do about it that would
// not be worse than a terminal it no longer owns.
func WriteTerminalReset(w io.Writer) {
	_, _ = io.WriteString(w, ScreenModeReset)
	_, _ = io.WriteString(w, InputModeReset)
}
