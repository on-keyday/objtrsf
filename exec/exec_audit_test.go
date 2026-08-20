package exec

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingAuditor captures the Auditor callbacks for assertions.
type recordingAuditor struct {
	mu       sync.Mutex
	started  bool
	exited   bool
	startCmd string
	startArg []string
	stdout   bytes.Buffer
	exitErr  error
}

func (r *recordingAuditor) Start(command string, args []string, pty bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started, r.startCmd, r.startArg = true, command, args
}
func (r *recordingAuditor) Stdin([]byte) {}
func (r *recordingAuditor) Stdout(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stdout.Write(data)
}
func (r *recordingAuditor) Stderr([]byte) {}
func (r *recordingAuditor) Exit(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exited, r.exitErr = true, err
}

// TestExecuteCommandWithOption_Audit verifies the opt-in Auditor observes the
// session: Start with the command/args, Stdout with the process output (tapped
// before framing, so it is captured even though the stub stream discards frames),
// and Exit. /bin/echo needs no stdin; SignalEOF lets handleInput return so the
// errgroup completes after echo exits.
func TestExecuteCommandWithOption_Audit(t *testing.T) {
	stream := newEOFBidiStream()
	rec := &recordingAuditor{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.SignalEOF()
	}()
	err := ExecuteCommandWithOption(ctx, stream, slog.Default(), "/bin/echo", []string{"hi"}, "", false, nil,
		ExecuteOption{Audit: rec})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if !rec.started {
		t.Error("Start not called")
	}
	if rec.startCmd != "/bin/echo" || len(rec.startArg) != 1 || rec.startArg[0] != "hi" {
		t.Errorf("Start = %q %v, want /bin/echo [hi]", rec.startCmd, rec.startArg)
	}
	if !rec.exited {
		t.Error("Exit not called")
	}
	if got := rec.stdout.String(); !strings.Contains(got, "hi") {
		t.Errorf("Stdout audit = %q, want to contain \"hi\"", got)
	}
}

// TestAuditExitCarriesTheChildFailure pins the thing this hook is for. Until
// 2026-08-21 Exit was handed the function's named return, which is nil on every
// path that actually runs a child -- so an audit trail recorded that no session
// had ever failed, and a caller wanting the child's outcome had nowhere to read
// it. The return value still reports setup only, deliberately; this is the
// channel that carries the child.
func TestAuditExitCarriesTheChildFailure(t *testing.T) {
	stream := newEOFBidiStream()
	rec := &recordingAuditor{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.SignalEOF()
	}()
	// `false` exits 1 and needs no stdin.
	err := ExecuteCommandWithOption(ctx, stream, slog.Default(), "/bin/sh", []string{"-c", "exit 3"}, "", false, nil,
		ExecuteOption{Audit: rec})

	// The RETURN carries it too, and carries the real code -- a caller that
	// only had "something went wrong" would have to invent one.
	var retEE *exec.ExitError
	if !errors.As(err, &retEE) {
		t.Fatalf("return = %v (%T), want an *exec.ExitError for a child that exited 3", err, err)
	}
	if retEE.ExitCode() != 3 {
		t.Errorf("returned exit code = %d, want 3", retEE.ExitCode())
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if !rec.exited {
		t.Fatal("Exit not called")
	}
	if rec.exitErr == nil {
		t.Fatal("Exit was handed nil for a child that exited 3; the audit trail " +
			"cannot distinguish a clean session from a failed one")
	}
	var ee *exec.ExitError
	if !errors.As(rec.exitErr, &ee) {
		t.Fatalf("Exit err = %v (%T), want an *exec.ExitError", rec.exitErr, rec.exitErr)
	}
	if ee.ExitCode() != 3 {
		t.Errorf("exit code = %d, want 3", ee.ExitCode())
	}
}

// A clean child must still report nil, or "did it fail" becomes unanswerable in
// the other direction.
func TestAuditExitIsNilOnACleanChild(t *testing.T) {
	stream := newEOFBidiStream()
	rec := &recordingAuditor{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.SignalEOF()
	}()
	if err := ExecuteCommandWithOption(ctx, stream, slog.Default(), "/bin/echo", []string{"ok"}, "", false, nil,
		ExecuteOption{Audit: rec}); err != nil {
		t.Fatalf("exec: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.exitErr != nil {
		t.Errorf("Exit err = %v on a child that exited 0", rec.exitErr)
	}
}

// A child that exits on its own must let Wait return. When os/exec owned the
// stdin copy, that goroutine stayed parked on a pipe only the stream could
// close, so Wait blocked until the session was torn down from outside --
// measured as a 45-second hang with the child long gone and its status
// available. The stub stream here is never signalled EOF ON PURPOSE: that is
// what the hang needed.
// cancellableStream is eofBidiStream with a Cancel that actually unblocks the
// reader, which is what a real trsf stream does — the shared stub's Cancel is
// inert, and with it this test could only ever measure the stub.
type cancellableStream struct{ *eofBidiStream }

func (s *cancellableStream) Cancel() { s.SignalEOF() }

func TestNonPTYChildExitDoesNotWaitForTheStream(t *testing.T) {
	stream := &cancellableStream{newEOFBidiStream()} // never SignalEOF'd by the test
	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
			"/bin/sh", []string{"-c", "exit 4"}, "", false, nil, ExecuteOption{})
	}()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 4 {
			t.Fatalf("err = %v, want an ExitError carrying 4", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the child exited but Wait did not return; the stdin copier is " +
			"blocking it again")
	}
}

// OnProcessExit reports the child's own ProcessState, which is the reliable
// source: the errgroup returns whatever failed first, and on Linux a pty's EIO
// at teardown routinely beats the exit status to it. ProcessState is set by the
// wait itself and cannot lose that race.
func TestOnProcessExitCarriesTheChildState(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		code int
	}{
		{"clean", []string{"-c", "exit 0"}, 0},
		{"failing", []string{"-c", "exit 7"}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &cancellableStream{newEOFBidiStream()}
			var got *os.ProcessState
			var gotErr error
			var mu sync.Mutex
			done := make(chan struct{})
			_ = ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
				"/bin/sh", tc.args, "", false, nil, ExecuteOption{
					OnProcessExit: func(st *os.ProcessState, err error) {
						mu.Lock()
						got, gotErr = st, err
						mu.Unlock()
						close(done)
					},
				})
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("OnProcessExit was never called")
			}
			mu.Lock()
			defer mu.Unlock()
			if got == nil {
				t.Fatal("OnProcessExit got a nil ProcessState")
			}
			if got.ExitCode() != tc.code {
				t.Errorf("ExitCode() = %d, want %d", got.ExitCode(), tc.code)
			}
			if tc.code == 0 && gotErr != nil {
				t.Errorf("a clean child reported err = %v", gotErr)
			}
			if tc.code != 0 && gotErr == nil {
				t.Errorf("a child that exited %d reported no error", tc.code)
			}
		})
	}
}
