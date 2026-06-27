//go:build !js

package exec

import (
	"io"
	"testing"

	"github.com/on-keyday/objtrsf/exec/frame"
)

func ctrlWinFrame(rows, cols uint16) []byte {
	ctrl := frame.Control{Type: frame.ControlType_TerminalWindowSize}
	ctrl.SetTerminalWindowSize(frame.TerminalWindowSize{Rows: rows, Columns: cols})
	enc := ctrl.MustAppend(nil)
	hdr := frame.FrameHeader{Type: frame.FrameType_Control, Len: uint32(len(enc))}
	return append(hdr.MustAppend(nil), enc...)
}

func stdoutFrame(p []byte) []byte {
	hdr := frame.FrameHeader{Type: frame.FrameType_Stdout, Len: uint32(len(p))}
	return append(hdr.MustAppend(nil), p...)
}

// The demux must extract a TerminalWindowSize control frame into LastWindowSize
// (and not surface it on Stdout), so a snapshot renderer can read the session's
// PTY size that the server replays ahead of the ring.
func TestCommandExecutionStream_LastWindowSize(t *testing.T) {
	stream := newEOFBidiStream()
	ces := NewCommandExecutionStream(stream)

	go func() {
		_, _ = stream.w.Write(ctrlWinFrame(43, 167)) // size first (as the server replays it)
		_, _ = stream.w.Write(stdoutFrame([]byte("hi")))
	}()

	// Reading the stdout payload guarantees the (earlier) control frame was
	// already processed by the demux — frames are sequential.
	got := make([]byte, 2)
	if _, err := io.ReadFull(ces.Stdout(), got); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if string(got) != "hi" {
		t.Fatalf("stdout = %q, want %q (control frame must not surface on Stdout)", got, "hi")
	}

	rows, cols, ok := ces.LastWindowSize()
	if !ok || rows != 43 || cols != 167 {
		t.Fatalf("LastWindowSize() = (%d,%d,%v), want (43,167,true)", rows, cols, ok)
	}
	stream.SignalEOF()
}
