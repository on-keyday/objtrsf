//go:build !js

package exec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf"
)

// recordingBidiStream captures what the input pump forwards and whether it
// half-closed the send side, on top of eofBidiStream's frame-fed read side.
type recordingBidiStream struct {
	*eofBidiStream
	mu        sync.Mutex
	appended  []byte
	closes    int
	appendErr error
}

func newRecordingBidiStream() *recordingBidiStream {
	return &recordingBidiStream{eofBidiStream: newEOFBidiStream()}
}

func (s *recordingBidiStream) AppendData(_ bool, data ...[]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return s.appendErr
	}
	for _, d := range data {
		s.appended = append(s.appended, d...)
	}
	return nil
}

func (s *recordingBidiStream) AppendDataContext(_ context.Context, eof bool, data ...[]byte) error {
	return s.AppendData(eof, data...)
}

func (s *recordingBidiStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

func (s *recordingBidiStream) forwarded() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.appended...)
}

func (s *recordingBidiStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func (s *recordingBidiStream) failAppends(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendErr = err
}

var _ trsf.BidirectionalStream = (*recordingBidiStream)(nil)

// readStep is one scripted result from the fake terminal.
type readStep struct {
	data []byte
	err  error
}

// scriptedReader replays steps in order and then blocks forever, the way a
// real terminal blocks waiting for the next keystroke.
//
// It never unblocks, deliberately. The input pump outlives pumpTerminalIO
// whenever the remote side ends first (that is production behaviour — see the
// bubbletea note in the pump), so waking it during teardown would race the
// test's own restore of the package-level hooks. A test that needs the pump
// gone scripts a terminal error as its last step instead.
type scriptedReader struct {
	steps []readStep
	i     int
	park  chan struct{}
}

func newScriptedReader(steps ...readStep) *scriptedReader {
	return &scriptedReader{steps: steps, park: make(chan struct{})}
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if r.i >= len(r.steps) {
		<-r.park
		return 0, io.EOF
	}
	s := r.steps[r.i]
	r.i++
	return copy(p, s.data), s.err
}

// repeatingReader always returns the same result without ever blocking — the
// pathological source the swallow counter exists to bound.
type repeatingReader struct{ err error }

func (r *repeatingReader) Read([]byte) (int, error) { return 0, r.err }

func shortGrace(t *testing.T) {
	t.Helper()
	old := stdinGoneGrace
	stdinGoneGrace = 50 * time.Millisecond
	t.Cleanup(func() { stdinGoneGrace = old })
}

func runPump(t *testing.T, w *CommandExecutionStream, in io.Reader, out io.Writer) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- w.pumpTerminalIO(in, out) }()
	return done
}

func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitPump(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("pumpTerminalIO did not return within %v — the session is wedged", within)
		return nil
	}
}

// The unchanged baseline: the remote closing the stream ends the session even
// though the terminal is still blocked waiting for a keystroke.
func TestPumpTerminalIO_RemoteEndEndsSession(t *testing.T) {
	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)
	var out bytes.Buffer

	done := runPump(t, ces, newScriptedReader(), &out)
	if _, err := stream.w.Write(stdoutFrame([]byte("hi"))); err != nil {
		t.Fatalf("write stdout frame: %v", err)
	}
	stream.SignalEOF()

	if err := awaitPump(t, done, 2*time.Second); err != nil {
		t.Fatalf("pumpTerminalIO = %v, want nil", err)
	}
	if out.String() != "hi" {
		t.Fatalf("out = %q, want %q", out.String(), "hi")
	}
}

// The regression this file exists for: when the input pump dies while the
// remote is still producing output, the session must end instead of running on
// with a dead input path.
func TestPumpTerminalIO_InputDeathEndsSession(t *testing.T) {
	shortGrace(t)
	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)

	readErr := errors.New("terminal read failed")
	in := newScriptedReader(readStep{err: readErr})

	// Note the stream is never closed: the remote side stays live throughout,
	// which is exactly the case that used to block forever.
	err := awaitPump(t, runPump(t, ces, in, io.Discard), 2*time.Second)
	if !errors.Is(err, ErrLocalInputLost) {
		t.Fatalf("pumpTerminalIO = %v, want it to wrap ErrLocalInputLost", err)
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("pumpTerminalIO = %v, want it to wrap the underlying read error", err)
	}
}

// Same shape, reached through the forward-write branch rather than the read
// branch: the pump gives up on a write error too.
func TestPumpTerminalIO_ForwardWriteFailureEndsSession(t *testing.T) {
	shortGrace(t)
	stream := newRecordingBidiStream()
	writeErr := errors.New("send side gone")
	stream.failAppends(writeErr)
	ces := NewCommandExecutionStream(stream)

	in := newScriptedReader(readStep{data: []byte("a")})

	err := awaitPump(t, runPump(t, ces, in, io.Discard), 2*time.Second)
	if !errors.Is(err, ErrLocalInputLost) || !errors.Is(err, writeErr) {
		t.Fatalf("pumpTerminalIO = %v, want ErrLocalInputLost wrapping %v", err, writeErr)
	}
}

// Detach half-closes the send side and normally ends when the peer closes the
// stream back. That round trip must still be what ends the session, and it
// must not be reported as a failure.
func TestPumpTerminalIO_DetachEndsWhenPeerCloses(t *testing.T) {
	shortGrace(t)
	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)

	in := newScriptedReader(readStep{data: []byte("ab\x1d")})
	done := runPump(t, ces, in, io.Discard)

	// Let the detach reach the send side first, then answer it — closing the
	// stream before the pump has seen the key would test the remote-end path
	// instead of this one.
	waitFor(t, 2*time.Second, "send side to be half-closed by the detach key",
		func() bool { return stream.closeCount() == 1 })

	// The peer answering the half-close is what ends the copy.
	stream.SignalEOF()

	if err := awaitPump(t, done, 2*time.Second); err != nil {
		t.Fatalf("pumpTerminalIO = %v, want nil for a detach", err)
	}
	if got := stream.forwarded(); !bytes.Contains(got, []byte("ab")) {
		t.Fatalf("forwarded = %q, want the bytes before the detach key", got)
	}
	if got := stream.forwarded(); bytes.Contains(got, []byte{0x1d}) {
		t.Fatalf("forwarded = %q, want the detach key itself consumed", got)
	}
}

// A peer that never answers the half-close must not hold the session open, but
// giving up on a deliberate detach is not an error either.
func TestPumpTerminalIO_DetachGivesUpOnSilentPeer(t *testing.T) {
	shortGrace(t)
	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)

	in := newScriptedReader(readStep{data: []byte{0x1d}})

	if err := awaitPump(t, runPump(t, ces, in, io.Discard), 2*time.Second); err != nil {
		t.Fatalf("pumpTerminalIO = %v, want nil — a slow peer must not turn a detach into a failure", err)
	}
}

// A swallowed artefact (Windows Ctrl+Z) must not end the input path: the
// keystroke is dropped and the next one still reaches the runner.
func TestPumpTerminalIO_SwallowedReadErrorKeepsInputAlive(t *testing.T) {
	shortGrace(t)
	old := swallowLocalReadEOF
	swallowLocalReadEOF = func(_ io.Reader, err error) bool { return errors.Is(err, io.EOF) }
	t.Cleanup(func() { swallowLocalReadEOF = old })

	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)

	// The final step ends the pump for real, so the test can join it before
	// restoring the hook. "next" can only be forwarded if the pump survived
	// the artefact on the step before it.
	stop := errors.New("terminal gone")
	in := newScriptedReader(
		readStep{err: io.EOF},          // the eaten keystroke
		readStep{data: []byte("next")}, // must still get through
		readStep{err: stop},
	)

	err := awaitPump(t, runPump(t, ces, in, io.Discard), 2*time.Second)
	if !errors.Is(err, stop) {
		t.Fatalf("pumpTerminalIO = %v, want it to end on the scripted terminal error", err)
	}
	if got := stream.forwarded(); !bytes.Contains(got, []byte("next")) {
		t.Fatalf("forwarded = %q, want it to contain %q — the input pump did not survive the artefact",
			got, "next")
	}
}

// Swallowing is bounded: a source that reports the artefact without ever
// blocking must end the session rather than spin forwarding nothing.
func TestPumpTerminalIO_SwallowIsBounded(t *testing.T) {
	shortGrace(t)
	oldHook := swallowLocalReadEOF
	swallowLocalReadEOF = func(_ io.Reader, err error) bool { return errors.Is(err, io.EOF) }
	t.Cleanup(func() { swallowLocalReadEOF = oldHook })
	oldMax := maxSwallowedReadEOF
	maxSwallowedReadEOF = 8
	t.Cleanup(func() { maxSwallowedReadEOF = oldMax })

	stream := newRecordingBidiStream()
	ces := NewCommandExecutionStream(stream)

	err := awaitPump(t, runPump(t, ces, &repeatingReader{err: io.EOF}, io.Discard), 2*time.Second)
	if !errors.Is(err, ErrLocalInputLost) {
		t.Fatalf("pumpTerminalIO = %v, want ErrLocalInputLost once the swallow limit is reached", err)
	}
	if got := stream.forwarded(); len(got) != 0 {
		t.Fatalf("forwarded = %q, want nothing — a swallowed keystroke must not reach the runner", got)
	}
}
