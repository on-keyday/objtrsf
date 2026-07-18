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
