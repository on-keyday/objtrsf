package exec

import (
	"errors"
	"io"
	"sync/atomic"
	"syscall"

	"github.com/on-keyday/objtrsf/exec/frame"
	"github.com/on-keyday/objtrsf/trsf"
)

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
