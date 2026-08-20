package exec

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
