//go:build !js

package exec

import (
	"io"
	"testing"

	"github.com/on-keyday/objtrsf/exec/frame"
)

// writeFrames feeds wire-encoded frames into an eofBidiStream's read side and
// then signals EOF, so a CommandExecutionStream sees exactly this sequence and
// stops.
func writeFrames(t *testing.T, s *eofBidiStream, frames ...struct {
	Type frame.FrameType
	Data string
}) {
	t.Helper()
	go func() {
		for _, f := range frames {
			hdr := frame.FrameHeader{Type: f.Type, Len: uint32(len(f.Data))}
			if _, err := s.w.Write(hdr.MustAppend(nil)); err != nil {
				return
			}
			if _, err := s.w.Write([]byte(f.Data)); err != nil {
				return
			}
		}
		s.SignalEOF()
	}()
}

// A Synth frame carries bytes the SERVER synthesised — a screen repaint, a
// terminal-mode preamble — which a terminal must apply exactly like PTY output
// and at exactly that position in the stream. So it lands on Stdout(), in frame
// order, and only its TYPE tells it apart.
//
// That is the whole reason it is a frame type rather than a second stream or a
// second pipe: neither of those has an ordering relationship to the real bytes,
// and a repaint that may arrive before the output it must overwrite is a race,
// not a design. A consumer that needs the distinction reads frames itself.
func TestCommandExecutionStreamDeliversSynthOnStdoutInOrder(t *testing.T) {
	type f = struct {
		Type frame.FrameType
		Data string
	}
	s := newEOFBidiStream()
	writeFrames(t, s,
		f{frame.FrameType_Stdout, "real-"},
		f{frame.FrameType_Synth, "synth-"},
		f{frame.FrameType_Stdout, "real2"},
	)

	w := NewCommandExecutionStream(s)
	defer w.Close()

	got, err := io.ReadAll(w.Stdout())
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if want := "real-synth-real2"; string(got) != want {
		t.Errorf("Stdout() = %q, want %q — a Synth payload must interleave with "+
			"PTY output in frame order, not be dropped or reordered", got, want)
	}
}

// Stderr stays its own destination. Synth joining Stdout is deliberate; it is
// not a general "unknown types go to stdout" rule, and this pins that the
// existing split is untouched.
func TestCommandExecutionStreamStillSeparatesStderr(t *testing.T) {
	type f = struct {
		Type frame.FrameType
		Data string
	}
	s := newEOFBidiStream()
	writeFrames(t, s,
		f{frame.FrameType_Stdout, "out"},
		f{frame.FrameType_Stderr, "err"},
		f{frame.FrameType_Synth, "syn"},
	)

	w := NewCommandExecutionStream(s)
	defer w.Close()

	errCh := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(w.Stderr())
		errCh <- string(b)
	}()
	out, _ := io.ReadAll(w.Stdout())
	if want := "outsyn"; string(out) != want {
		t.Errorf("Stdout() = %q, want %q", out, want)
	}
	if got := <-errCh; got != "err" {
		t.Errorf("Stderr() = %q, want %q", got, "err")
	}
}
