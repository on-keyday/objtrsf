package exec

import (
	"bytes"
	"context"
	"log/slog"
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
func (r *recordingAuditor) Exit(error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exited = true
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
