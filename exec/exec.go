//go:build !js

package exec

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"time"

	pty "github.com/aymanbagabas/go-pty"
	"github.com/on-keyday/objtrsf/exec/frame"
	"github.com/on-keyday/objtrsf/trsf"

	"golang.org/x/sync/errgroup"
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

	// OnProcessExit, if non-nil, is called once with the child's own
	// os.ProcessState after it has been reaped, plus the wait error.
	//
	// This is the reliable way to learn how the child ended, and the reason it
	// exists rather than the caller reading an error type: the errgroup that
	// runs this session collects whatever fails first, and on Linux the pty
	// master returns EIO the instant the slave closes — so the incidental
	// teardown error routinely beats the exit status to the finish line.
	// ProcessState is not a race; it is set by the wait itself.
	//
	// state.ExitCode() is -1 when the child was killed by a signal, which for
	// these sessions is the ordinary end (a cancel), not a crash. Deciding
	// what an exit MEANS stays with the caller.
	OnProcessExit func(state *os.ProcessState, err error)

	// StdinDevNull gives the child /dev/null as its standard input instead of
	// a pipe fed from the stream. For a caller that will never send stdin --
	// an out-of-band command run from a UI with no keyboard attached to it --
	// this is what the child should have.
	//
	// A pipe that is closed shortly after the child starts is NOT the same
	// thing, which is why this is a mode rather than a convenience. It leaves a
	// window (measured at 6ms over a LAN) in which the child's stdin is open
	// and empty, so a program that distinguishes "no input yet" from "end of
	// input" gets a racing answer. /dev/null has no window and stays readable:
	// a child that reopens fd 0 still finds EOF.
	//
	// Stdin frames that arrive anyway are DROPPED, with one warning. The
	// alternative -- writing to the closed pipe -- returns ErrClosedPipe and
	// would fail the whole session for a caller's bookkeeping mistake.
	//
	// Refused with ptyEnabled, and refused alongside OnStdinWriter: a PTY
	// child's stdin IS the terminal, and OnStdinWriter has nothing to write
	// into. Silently ignoring either would be a typed option that does nothing.
	StdinDevNull bool
}

// ExecuteCommandWithOption is the option-bearing form of ExecuteCommand.
//
// The error reports how the SESSION ended, including the child's own: a child
// that exits non-zero comes back as an *exec.ExitError, so a caller can read
// the real code with errors.As rather than inventing one.
//
// Until 2026-08-21 it returned nil unconditionally -- the child's failure was
// logged and dropped. That made a whole class of outcome unobservable: in the
// harness that consumes this, every interactive task reported exit 0 and
// therefore "Succeeded", whatever the agent actually did.
//
// Whether a given exit MEANS failure is still the caller's policy, and the
// caller needs that judgement: killing the child is the ordinary way these
// sessions end, so a cancelled session arrives here as "signal: killed" and
// must not be reported as a crash. The same outcome is also delivered to
// ExecuteOption.Audit's Exit, for callers recording a session rather than
// branching on it.
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
	// sessionErr is what the session ended with. It is tracked separately from
	// the named return only because the early-return paths below never reach
	// gr.Wait; both end up in Audit.Exit. Reporting the named return here used
	// to mean every Auditor was told every session exited cleanly.
	var sessionErr error
	if opt.Audit != nil {
		opt.Audit.Start(command, args, ptyEnabled)
		defer func() {
			if sessionErr == nil {
				// The early-return paths below never reach gr.Wait; for those
				// the named return IS the outcome.
				sessionErr = retErr
			}
			opt.Audit.Exit(sessionErr)
		}()
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
	if opt.StdinDevNull && ptyEnabled {
		return errors.New("exec: StdinDevNull with ptyEnabled: a pty child's stdin is the terminal")
	}
	if opt.StdinDevNull && opt.OnStdinWriter != nil {
		return errors.New("exec: StdinDevNull with OnStdinWriter: there is no pipe to write into")
	}
	pipeOut, pipeIn := io.Pipe()
	var ptyHandle pty.Pty
	var process *os.Process
	var waitFn func() error
	// stateFn reads the child's ProcessState after waitFn returns. Both branches
	// populate one; go-pty copies the exec.Cmd's across.
	var stateFn func() *os.ProcessState
	warnedDroppedStdin := false
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
				if opt.StdinDevNull {
					// Dropped, not written: the child has /dev/null and this
					// pipe has no reader, so a write would block forever.
					// Warned once — the caller said it would send none.
					if !warnedDroppedStdin {
						warnedDroppedStdin = true
						logger.Warn("dropping stdin frames: this exec was started with StdinDevNull")
					}
					continue
				}
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
		stateFn = func() *os.ProcessState { return ptyCmd.ProcessState }
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
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		// Own the stdin copy rather than handing os/exec a non-*os.File
		// Stdin. When it owns the copier, Cmd.Wait waits for that goroutine
		// too, and the goroutine is parked in Read on a pipe that only closes
		// when the STREAM ends -- so a child that exits on its own leaves Wait
		// blocked indefinitely. Measured as a 45-second hang: the child was
		// gone, its exit status was sitting there, and Wait would not return.
		//
		// With the copy here, Wait returns as soon as the process does. This
		// goroutine outlives it, parked on the same read, until handleInput's
		// `defer pipeIn.Close()` fires when the stream ends -- which the
		// deferred stream.CloseBoth guarantees.
		if opt.StdinDevNull {
			// Handed to os/exec as an *os.File, so the child gets the fd
			// directly: no copier goroutine, and therefore none of the Wait
			// hazard the comment above describes.
			devNull, err := os.Open(os.DevNull)
			if err != nil {
				return err
			}
			defer devNull.Close()
			cmd.Stdin = devNull
			if err := cmd.Start(); err != nil {
				return err
			}
		} else {
			childIn, err := cmd.StdinPipe()
			if err != nil {
				return err
			}
			if err := cmd.Start(); err != nil {
				return err
			}
			go func() {
				defer childIn.Close()
				_, _ = io.Copy(childIn, pipeOut)
			}()
		}
		process = cmd.Process
		waitFn = cmd.Wait
		stateFn = func() *os.ProcessState { return cmd.ProcessState }
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
	// childErr is the CHILD's own outcome, kept apart from whatever else the
	// errgroup collects. Returning gr.Wait()'s error directly looks right and
	// is not: on Linux the pty master returns EIO the moment the slave closes,
	// so the stdout copy fails first and that incidental error arrives ahead
	// of the exit status. Measured -- a clean `exit 0` session came back
	// "read /dev/ptmx: input/output error", which is how a session that
	// succeeded gets reported as Failed. gr.Wait() synchronises the write
	// below with the read after it.
	var childErr error
	gr.Go(func() error {
		defer stream.Cancel() // terminate the input handler
		err := waitFn()
		childErr = err
		procExited.Store(true)
		if opt.OnProcessExit != nil {
			var st *os.ProcessState
			if stateFn != nil {
				st = stateFn()
			}
			opt.OnProcessExit(st, err)
		}
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
		// nil, not err: the child's status travels in childErr. Returning it
		// here would race the pty EIO for "first error in the group", and the
		// EIO usually wins.
		return nil
	})
	err := gr.Wait()
	sessionErr = childErr
	switch {
	case childErr != nil:
		logger.Error("command execution stream ended with a failing child", "error", childErr)
	case err != nil:
		// The child was fine; something else in the session was not. Logged,
		// not returned: after a clean child these are teardown artefacts (the
		// pty EIO above being the routine one), and reporting them would mark
		// successful sessions failed.
		logger.Info("command execution stream ended", "after_clean_child", err)
	default:
		logger.Info("command execution stream ended")
	}
	return childErr
}
