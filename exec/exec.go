//go:build !js

package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	pty "github.com/aymanbagabas/go-pty"
	"github.com/on-keyday/objtrsf/exec/frame"
	"github.com/on-keyday/objtrsf/trsf"

	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
)

type outStreamWrapper struct {
	frameType frame.FrameType
	s         trsf.BidirectionalStream
	audit     Auditor // optional; taps stdout/stderr before framing
}

func (o *outStreamWrapper) Write(p []byte) (n int, err error) {
	if o.audit != nil {
		// Tap the raw payload before it is chunked/framed below. The loop only
		// reslices p (never mutates the bytes), so this view is valid here; the
		// Auditor must copy if it retains beyond the call.
		switch o.frameType {
		case frame.FrameType_Stdout:
			o.audit.Stdout(p)
		case frame.FrameType_Stderr:
			o.audit.Stderr(p)
		}
	}
	originLen := len(p)
	for len(p) > 0 {
		chunkSize := len(p)
		chunck := min(chunkSize, math.MaxUint32)
		dataToSend := p[:chunck]
		p = p[chunck:]
		// wrapping with frame
		hdr := frame.FrameHeader{
			Type: o.frameType,
			Len:  uint32(len(dataToSend)),
		}
		var dataCopy []byte // because p will be changed in next loop
		if len(dataToSend) > 0 {
			dataCopy = make([]byte, len(dataToSend))
			copy(dataCopy, dataToSend)
		}
		err = o.s.AppendData(false, hdr.MustAppend(nil), dataCopy)
		if err != nil {
			return 0, err
		}
	}
	return originLen, nil
}

func (c *outStreamWrapper) Close() error {
	hdr := frame.FrameHeader{
		Type: c.frameType,
		Len:  0,
	}
	return c.s.AppendData(true, hdr.MustAppend(nil))
}

// resizePty applies a window-size update to the given Pty. On Unix it uses
// the UnixPty extension to also propagate pixel dimensions (Xpixel/Ypixel),
// which some TUIs use for inline image / sixel sizing. On Windows ConPTY
// has no pixel concept and we fall back to the cell-only Resize.
func resizePty(p pty.Pty, rows, cols, width, height uint16) error {
	if up, ok := p.(pty.UnixPty); ok {
		return up.SetWinsize(&pty.Winsize{
			Row:    rows,
			Col:    cols,
			Xpixel: width,
			Ypixel: height,
		})
	}
	return p.Resize(int(cols), int(rows))
}

// Auditor observes a command-execution session on the runner side for audit /
// session-recording: the launched command, every stdin byte received from the
// client, every stdout/stderr byte sent back, and the final exit. It is opt-in
// (ExecuteOption.Audit); a nil Auditor disables auditing (the historical
// behaviour). Implementations must not block the exec hot path — record
// asynchronously if the sink is slow — and must copy any []byte they retain
// beyond the call (the buffers are reused). Methods may be called concurrently
// from the stdout, stderr, and stdin goroutines.
type Auditor interface {
	Start(command string, args []string, ptyEnabled bool)
	Stdin(data []byte)
	Stdout(data []byte)
	Stderr(data []byte)
	Exit(err error)
}

// ExecuteOption groups optional hooks for ExecuteCommand. Pass via
// ExecuteCommandWithOption. The original ExecuteCommand keeps its
// historical signature and forwards an empty option.
type ExecuteOption struct {
	// OnStdinWriter, if non-nil, is invoked exactly once shortly after the
	// child process's stdin pipe is wired up. The argument is a write fn
	// that the caller can stash and call any time before
	// ExecuteCommandWithOption returns to inject bytes directly into the
	// child's stdin. Writes after the process exits return io.ErrClosedPipe.
	//
	// Used by the runner to deliver agentboard wake markers without going
	// through the TUI/WebUI frame protocol.
	OnStdinWriter func(write func([]byte) (int, error))

	// Audit, if non-nil, receives a full session record (start / stdin /
	// stdout / stderr / exit) for the executed command — the remote-shell
	// audit trail. Nil disables auditing.
	Audit Auditor
}

// ExecuteCommandWithOption is the option-bearing form of ExecuteCommand.
func ExecuteCommandWithOption(ctx context.Context, stream trsf.BidirectionalStream, logger *slog.Logger, command string, args []string, cwd string, ptyEnabled bool, extraEnv []string, opt ExecuteOption) error {
	return executeCommandImpl(ctx, stream, logger, command, args, cwd, ptyEnabled, extraEnv, opt)
}

// ExecuteCommand runs command with its stdout/stderr forwarded over stream and
// stdin read from stream. It keeps its historical signature; use
// ExecuteCommandWithOption for additional hooks.
func ExecuteCommand(ctx context.Context, stream trsf.BidirectionalStream, logger *slog.Logger, command string, args []string, cwd string, ptyEnabled bool, extraEnv []string) error {
	return executeCommandImpl(ctx, stream, logger, command, args, cwd, ptyEnabled, extraEnv, ExecuteOption{})
}

func executeCommandImpl(ctx context.Context, stream trsf.BidirectionalStream, logger *slog.Logger, command string, args []string, cwd string, ptyEnabled bool, extraEnv []string, opt ExecuteOption) (retErr error) {
	defer stream.CloseBoth()
	logger.Info("Executing command", "command", command, "args", args, "cwd", cwd, "pty", ptyEnabled)
	if opt.Audit != nil {
		opt.Audit.Start(command, args, ptyEnabled)
		defer func() { opt.Audit.Exit(retErr) }()
	}
	gr, grCtx := errgroup.WithContext(ctx)
	gr.SetLimit(-1)
	stdout := &outStreamWrapper{
		frameType: frame.FrameType_Stdout,
		s:         stream,
		audit:     opt.Audit,
	}
	stderr := &outStreamWrapper{
		frameType: frame.FrameType_Stderr,
		s:         stream,
		audit:     opt.Audit,
	}
	pipeOut, pipeIn := io.Pipe()
	var ptyHandle pty.Pty
	var process *os.Process
	var waitFn func() error
	handleInput := func() error {
		defer pipeIn.Close()
		for {
			hdr := &frame.Frame{}
			err := hdr.Read(stream)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			if hdr.Header.Type == frame.FrameType_Stdin {
				if hdr.Header.Len == 0 { // close stdin
					pipeIn.Close()
					continue
				}
				data := *hdr.Data()
				if opt.Audit != nil {
					opt.Audit.Stdin(data)
				}
				_, err = pipeIn.Write(data)
				if err != nil {
					return err
				}
			} else if ctrl := hdr.Control(); ctrl != nil {
				switch ctrl.Type {
				case frame.ControlType_TerminalWindowSize:
					if ptyHandle == nil {
						logger.Warn("received terminal window size control frame, but pty is not enabled")
						continue
					}
					ws := ctrl.TerminalWindowSize()
					if err := resizePty(ptyHandle, ws.Rows, ws.Columns, ws.Width, ws.Height); err != nil {
						logger.Error("failed to set pty window size", "error", err)
					}
				case frame.ControlType_Signal:
					sig := ctrl.Signal()
					if process == nil {
						logger.Warn("received signal control frame before process start", "signal", sig.Signal)
						continue
					}
					if err := process.Signal(syscall.Signal(sig.Signal)); err != nil {
						logger.Error("failed to send signal to process", "error", err)
					}
				default:
					logger.Warn("unknown control frame received", "type", ctrl.Type)
				}
			} else {
				logger.Warn("unknown frame type received", "type", hdr.Header.Type)
			}
		}
	}
	var procExited atomic.Bool
	if ptyEnabled {
		p, err := pty.New()
		if err != nil {
			return err
		}
		ptyCmd := p.CommandContext(grCtx, command, args...)
		if cwd != "" {
			ptyCmd.Dir = cwd
		}
		if len(extraEnv) > 0 {
			ptyCmd.Env = append(os.Environ(), extraEnv...)
		}
		if err := ptyCmd.Start(); err != nil {
			// Only this early-error path closes p; once Start succeeds,
			// the wait goroutine becomes the sole owner of p.Close.
			// Pty.Close is non-idempotent on Windows: go-pty's conPty.Close
			// re-invokes ClosePseudoConsole on a closed handle, which
			// produces STATUS_HEAP_CORRUPTION (0xC0000374). A double-close
			// here would crash the runner immediately on the natural detach
			// path even though both calls are "expected".
			_ = p.Close()
			return err
		}
		ptyHandle = p
		process = ptyCmd.Process
		waitFn = ptyCmd.Wait
		gr.Go(func() error {
			// Don't close p here. On Windows, conPty.Close calls
			// ClosePseudoConsole, and doing so while the output goroutine is
			// still mid-Read on outPipe causes STATUS_HEAP_CORRUPTION
			// (0xC0000374). Pty.Close is centralized in the wait goroutine
			// below, after ptyCmd.Wait returns and the child is fully gone.
			_, err := io.Copy(p, pipeOut)
			// try SIGHUP to notify EOF
			if process != nil {
				process.Signal(syscall.SIGHUP)
				// try SIGTERM after 1 second if not exited
				time.AfterFunc(1*time.Second, func() {
					if !procExited.Load() && process != nil {
						process.Signal(syscall.SIGTERM)
						// finally try SIGKILL after another 1 second
						time.AfterFunc(1*time.Second, func() {
							if !procExited.Load() && process != nil {
								process.Kill()
							}
						})
					}
				})
			}
			return err
		})
		gr.Go(func() error {
			defer stdout.Close()
			_, err := io.Copy(stdout, p)
			return err
		})
	} else {
		cmd := exec.CommandContext(grCtx, command, args...)
		if cwd != "" {
			cmd.Dir = cwd
		}
		if len(extraEnv) > 0 {
			cmd.Env = append(os.Environ(), extraEnv...)
		}
		cmd.Stdin = pipeOut
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		process = cmd.Process
		waitFn = cmd.Wait
	}
	if opt.OnStdinWriter != nil {
		writeFn := func(p []byte) (int, error) {
			return pipeIn.Write(p)
		}
		gr.Go(func() error {
			opt.OnStdinWriter(writeFn)
			return nil
		})
	}
	gr.Go(handleInput)
	gr.Go(func() error {
		defer stream.Cancel() // terminate the input handler
		err := waitFn()
		procExited.Store(true)
		// Close the Pty here, AFTER the child has fully exited and been
		// reaped. This is the SOLE close site on the success path: go-pty's
		// conPty.Close on Windows is non-idempotent (re-invokes
		// ClosePseudoConsole on a closed handle, producing
		// STATUS_HEAP_CORRUPTION 0xC0000374), so the early-error path in
		// the PTY block above does its own explicit close instead of an
		// outer defer.
		if ptyHandle != nil {
			_ = ptyHandle.Close()
		}
		return err
	})
	err := gr.Wait()
	if err != nil {
		logger.Error("command execution stream ended with error", "error", err)
	} else {
		logger.Info("command execution stream ended")
	}
	return nil
}

type CommandExecutionStream struct {
	trsf.BidirectionalStream
	stdoutPipe *io.PipeReader
	stderrPipe *io.PipeReader
	// winSize holds the most recent TerminalWindowSize seen on the stream,
	// packed as (rows<<16)|columns; 0 means none seen yet. Set from the demux
	// goroutine, read via LastWindowSize. A view-attach snapshot needs the
	// session's PTY size to render the absolute-positioned output at the right
	// grid dimensions; the server replays the controlling client's last size as
	// a control frame ahead of the ring (see server.SessionMux.AttachViewer).
	winSize atomic.Uint32
}

// LastWindowSize returns the most recent terminal window size observed on the
// stream (e.g. replayed by the server to a view-attach). ok is false when no
// size frame has been seen yet.
func (w *CommandExecutionStream) LastWindowSize() (rows, cols uint16, ok bool) {
	v := w.winSize.Load()
	if v == 0 {
		return 0, 0, false
	}
	return uint16(v >> 16), uint16(v), true
}

func NewCommandExecutionStream(stream trsf.BidirectionalStream) *CommandExecutionStream {
	stdoutPipeR, stdoutPipeW := io.Pipe()
	stderrPipeR, stderrPipeW := io.Pipe()
	w := &CommandExecutionStream{
		BidirectionalStream: stream,
		stdoutPipe:          stdoutPipeR,
		stderrPipe:          stderrPipeR,
	}
	go func() {
		defer stdoutPipeW.Close()
		defer stderrPipeW.Close()
		for {
			hdr := &frame.Frame{}
			err := hdr.Read(stream)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				stdoutPipeW.CloseWithError(err)
				stderrPipeW.CloseWithError(err)
				return
			}
			switch hdr.Header.Type {
			case frame.FrameType_Stdout:
				if hdr.Header.Len == 0 {
					stdoutPipeW.Close()
					continue
				}
				data := *hdr.Data()
				_, err = stdoutPipeW.Write(data)
				if err != nil {
					stdoutPipeW.CloseWithError(err)
					return
				}
			case frame.FrameType_Stderr:
				if hdr.Header.Len == 0 {
					stderrPipeW.Close()
					continue
				}
				data := *hdr.Data()
				_, err = stderrPipeW.Write(data)
				if err != nil {
					stderrPipeW.CloseWithError(err)
					return
				}
			case frame.FrameType_Control:
				// A view-attach replay carries the session's PTY size as a
				// TerminalWindowSize control frame ahead of the ring so the
				// snapshot renderer can size its grid correctly.
				if ctrl := hdr.Control(); ctrl != nil && ctrl.Type == frame.ControlType_TerminalWindowSize {
					if ws := ctrl.TerminalWindowSize(); ws != nil {
						w.winSize.Store(uint32(ws.Rows)<<16 | uint32(ws.Columns))
					}
				}
			default:
				// ignore unknown frame types
			}
		}
	}()
	return w
}

func (w *CommandExecutionStream) Stdout() io.Reader {
	return w.stdoutPipe
}

func (w *CommandExecutionStream) Stderr() io.Reader {
	return w.stderrPipe
}

func (w *CommandExecutionStream) Stdin() io.Writer {
	return &stdinWrapper{
		s: w.BidirectionalStream,
	}
}

type stdinWrapper struct {
	s trsf.BidirectionalStream
}

func (w *stdinWrapper) Close() error {
	hdr := frame.FrameHeader{
		Type: frame.FrameType_Stdin,
		Len:  0,
	}
	return w.s.AppendData(false, hdr.MustAppend(nil))
}

func (w *stdinWrapper) Write(data []byte) (n int, err error) {
	hdr := frame.FrameHeader{
		Type: frame.FrameType_Stdin,
		Len:  uint32(len(data)),
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	err = w.s.AppendData(false, hdr.MustAppend(nil), copied)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

func (w *CommandExecutionStream) SendSignal(sig syscall.Signal) error {
	ctrl := frame.Control{
		Type: frame.ControlType_Signal,
	}
	ctrl.SetSignal(frame.Signal{
		Signal: int32(sig),
	})
	enc := ctrl.MustAppend(nil)
	fullCtrl := frame.FrameHeader{
		Type: frame.FrameType_Control,
		Len:  uint32(len(enc)),
	}
	return w.AppendData(false, fullCtrl.MustAppend(nil), enc)
}

func (w *CommandExecutionStream) SetTerminalWindowSize(rows, columns, width, height uint16) error {
	ctrl := frame.Control{
		Type: frame.ControlType_TerminalWindowSize,
	}
	ctrl.SetTerminalWindowSize(frame.TerminalWindowSize{
		Rows:    rows,
		Columns: columns,
		Width:   width,
		Height:  height,
	})
	enc := ctrl.MustAppend(nil)
	fullCtrl := frame.FrameHeader{
		Type: frame.FrameType_Control,
		Len:  uint32(len(enc)),
	}
	return w.AppendData(false, fullCtrl.MustAppend(nil), enc)
}

func (w *CommandExecutionStream) Close() error {
	w.stdoutPipe.Close()
	w.stderrPipe.Close()
	w.BidirectionalStream.Cancel()
	return w.BidirectionalStream.Close()
}

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
