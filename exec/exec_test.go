//go:build !js

package exec

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/on-keyday/objtrsf/trsf"
)

// eofBidiStream is a minimal trsf.BidirectionalStream stub for tests.
//
// Its Read side blocks on an io.Pipe until SignalEOF() is called; only then
// does Read return io.EOF and let handleInput exit. This ordering matters
// because ExecuteCommandWithOption runs handleInput concurrently with
// OnStdinWriter, and handleInput's `defer pipeIn.Close()` closes the child's
// stdin write side as soon as it returns. If Read returned EOF immediately
// (closing the pipe before OnStdinWriter ran), the OnStdinWriter's write would
// race against stdin pipe close and intermittently fail with "io: read/write
// on closed pipe".
//
// Cannot use Cancel() to trigger EOF: cmd.Wait() inside ExecuteCommandWithOption
// blocks on the os/exec stdin copy goroutine, which waits for pipeIn to close,
// which only happens after handleInput's deferred close — so triggering EOF
// only from cmd.Wait's defer creates a circular dependency. The test must
// explicitly signal EOF after OnStdinWriter completes, letting /bin/cat see
// stdin EOF and exit normally.
type eofBidiStream struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func newEOFBidiStream() *eofBidiStream {
	r, w := io.Pipe()
	return &eofBidiStream{r: r, w: w}
}

// SignalEOF closes the write end of the internal pipe so subsequent Reads
// return io.EOF. Tests call this after OnStdinWriter has completed its writes.
func (s *eofBidiStream) SignalEOF() { _ = s.w.Close() }

// SendStream methods.
func (s *eofBidiStream) ID() trsf.StreamID                                                     { return 0 }
func (s *eofBidiStream) Write(p []byte) (int, error)                                           { return len(p), nil }
func (s *eofBidiStream) Close() error                                                          { return nil }
func (s *eofBidiStream) WriteContext(_ context.Context, p []byte) (int, error)                 { return len(p), nil }
func (s *eofBidiStream) HasSendData() bool                                                     { return false }
func (s *eofBidiStream) Completed() bool                                                       { return false }
func (s *eofBidiStream) AppendData(_ bool, _ ...[]byte) error                                  { return nil }
func (s *eofBidiStream) AppendDataContext(_ context.Context, _ bool, _ ...[]byte) error        { return nil }

// ReceiveStream methods.
func (s *eofBidiStream) Read(p []byte) (int, error)                                            { return s.r.Read(p) }
func (s *eofBidiStream) ReadContext(_ context.Context, p []byte) (int, error)                  { return s.r.Read(p) }
func (s *eofBidiStream) ReadDirectContext(_ context.Context, _ uint64) ([]byte, bool, error)   { return nil, true, nil }
func (s *eofBidiStream) ReadDirect(_ uint64) ([]byte, bool, error)                             { return nil, true, nil }
func (s *eofBidiStream) HasRecvData() bool                                                     { return false }
func (s *eofBidiStream) EOF() bool                                                             { return true }
func (s *eofBidiStream) Cancel()                                                               {}

// BidirectionalStream method.
func (s *eofBidiStream) CloseBoth() error { return nil }

// TestExecuteCommandWithOption_OnStdinWriter verifies that ExecuteCommandWithOption
// invokes OnStdinWriter with a write closure bound to the child's stdin pipe.
// It uses /bin/cat (echoes stdin to stdout) as the subprocess, writes one
// payload via the hook, then cancels the context to let the function return.
func TestExecuteCommandWithOption_OnStdinWriter(t *testing.T) {
	stream := newEOFBidiStream()
	captured := make(chan struct{}, 1)
	opt := ExecuteOption{
		OnStdinWriter: func(w func([]byte) (int, error)) {
			n, err := w([]byte("test\n"))
			if err != nil {
				t.Errorf("write err = %v", err)
			}
			if n != 5 {
				t.Errorf("write n = %d, want 5", n)
			}
			captured <- struct{}{}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-captured
		// Signal EOF on the input stream so handleInput returns and closes
		// the child's stdin write side. /bin/cat reads "test\n", sees stdin
		// EOF, and exits cleanly — letting cmd.Wait complete naturally.
		stream.SignalEOF()
	}()
	logger := slog.Default()
	// Run /bin/cat which echoes stdin to stdout; we only verify that
	// OnStdinWriter is invoked and the returned writer accepts bytes without
	// error.
	_ = ExecuteCommandWithOption(ctx, stream, logger, "/bin/cat", nil, "", false, nil, opt)
}

func TestDetachIndex_RawCtrlBracket(t *testing.T) {
	for _, tc := range []struct {
		name      string
		buf       []byte
		wantStart int
		wantEnd   int
	}{
		{"alone", []byte{0x1d}, 0, 1},
		{"after prefix", []byte("hello\x1d"), 5, 6},
		{"middle of buffer", []byte("a\x1db"), 1, 2},
		{"absent", []byte("hello"), -1, -1},
		{"empty", []byte{}, -1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStart, gotEnd := detachIndex(tc.buf)
			if gotStart != tc.wantStart || gotEnd != tc.wantEnd {
				t.Fatalf("detachIndex(%q) = (%d,%d), want (%d,%d)",
					tc.buf, gotStart, gotEnd, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

func TestDetachIndex_Win32InputModeCtrlBracket(t *testing.T) {
	// Real Win32 Input Mode keydown for Ctrl+] captured from Windows
	// Terminal: Vk=221 (VK_OEM_6), Sc=43, Uc=29 (0x1d), Kd=1, Cs=8 (Ctrl), Rc=1.
	keydown := []byte("\x1b[221;43;29;1;8;1_")
	keyup := []byte("\x1b[221;43;29;0;8;1_") // Kd=0; must NOT trigger
	ctrlOnly := []byte("\x1b[17;29;0;1;8;1_") // Vk=VK_CONTROL, Uc=0; must NOT trigger
	noise := []byte("\x1b[?9001h")            // mode-set; not Win32 Input Mode

	for _, tc := range []struct {
		name      string
		buf       []byte
		wantStart int
		wantEnd   int
	}{
		{"keydown alone", keydown, 0, len(keydown)},
		{"keyup ignored", keyup, -1, -1},
		{"ctrl-only ignored", ctrlOnly, -1, -1},
		{"unrelated CSI ignored", noise, -1, -1},
		{"keydown after legit input", append([]byte("ls -la\r"), keydown...), len("ls -la\r"), len("ls -la\r") + len(keydown)},
		{"keyup then keydown picks keydown", append(append([]byte{}, keyup...), keydown...), len(keyup), len(keyup) + len(keydown)},
		{"raw 0x1d wins if earlier", append([]byte{0x1d}, keydown...), 0, 1},
		{"win32 wins if earlier", append([]byte("\x1b[221;43;29;1;8;1_x"), 0x1d), 0, len(keydown)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStart, gotEnd := detachIndex(tc.buf)
			if gotStart != tc.wantStart || gotEnd != tc.wantEnd {
				t.Fatalf("detachIndex(%x) = (%d,%d), want (%d,%d)",
					tc.buf, gotStart, gotEnd, tc.wantStart, tc.wantEnd)
			}
		})
	}
}
