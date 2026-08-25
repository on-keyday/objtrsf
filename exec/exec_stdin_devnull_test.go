package exec

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A child started with StdinDevNull reads EOF from stdin with no window in
// which stdin is open-and-empty, and without the stream ever saying so.
//
// The stub stream is never signalled EOF on purpose: that is exactly the state
// a caller with nothing to send leaves it in, and the pipe form only reaches
// EOF when the stream ends. Without the mode this test hangs — the negative
// control is the timeout below, not a separate case.
func TestStdinDevNullChildSeesEOFWithNoStream(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/bin/sh")
	}
	stream := &cancellableStream{newEOFBidiStream()}
	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
			"/bin/sh", []string{"-c", "cat; exit 0"}, "", false, nil,
			ExecuteOption{StdinDevNull: true})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err = %v, want a clean exit", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the child never reached EOF on stdin: it was not given /dev/null")
	}
}

// It is /dev/null, not a closed descriptor: fd 0 is open and readable, so a
// child that inspects or reopens it finds something rather than an error. That
// is the difference from closing a pipe, and the reason this is a mode.
func TestStdinDevNullIsAReadableDevNull(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/bin/sh")
	}
	stream := &cancellableStream{newEOFBidiStream()}

	// The child asserts, and reports which assertion failed as its exit code:
	// the stub stream discards writes, so its stdout cannot be read back here,
	// and a code names the failure better than a missing line would.
	script := `
		[ -r /dev/stdin ] || exit 21   # fd 0 open and readable, not closed
		[ -t 0 ] && exit 22            # and not a terminal
		cat /dev/stdin >/dev/null || exit 23   # reopening it still finds EOF
		exit 0`
	err := ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
		"/bin/sh", []string{"-c", script}, "", false, nil,
		ExecuteOption{StdinDevNull: true})
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("child assertion %d failed (21=not readable, 22=a tty, 23=reopen failed)", ee.ExitCode())
		}
		t.Fatalf("err = %v", err)
	}
}

// A defensive close from a client that cannot know whether the runner honours
// this mode is a silent no-op, not a dropped frame: it must not warn per exec.
func TestStdinDevNullCloseFrameIsSilentlyIgnored(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/bin/sh")
	}
	stream := &cancellableStream{newEOFBidiStream()}
	go func() {
		time.Sleep(50 * time.Millisecond)
		w := &stdinWrapper{s: stream}
		_ = w.Close() // the 0-length Stdin frame
	}()
	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
			"/bin/sh", []string{"-c", "cat; exit 0"}, "", false, nil,
			ExecuteOption{StdinDevNull: true})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a defensive close failed the session: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a defensive close blocked the session")
	}
}

// Stdin frames that arrive anyway are dropped rather than failing the session.
// Writing them to the unread pipe would block forever; closing the pipe first
// and writing would return ErrClosedPipe and take the whole exec down for a
// caller's bookkeeping mistake.
func TestStdinDevNullDropsStdinFramesInsteadOfFailing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/bin/sh")
	}
	stream := &cancellableStream{newEOFBidiStream()}
	go func() {
		// Give the reader a moment to be parked on the frame read, then send
		// stdin the caller promised not to send.
		time.Sleep(50 * time.Millisecond)
		w := &stdinWrapper{s: stream}
		_, _ = w.Write([]byte("unexpected\n"))
	}()

	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
			"/bin/sh", []string{"-c", "cat; exit 0"}, "", false, nil,
			ExecuteOption{StdinDevNull: true})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a dropped stdin frame failed the session: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a dropped stdin frame blocked the session")
	}
}

// The two contradictions are refused rather than ignored. A typed option that
// silently does nothing is the failure shape worth spending an error on.
func TestStdinDevNullRefusesContradictions(t *testing.T) {
	stream := &cancellableStream{newEOFBidiStream()}
	err := ExecuteCommandWithOption(context.Background(), stream, slog.Default(),
		"/bin/sh", []string{"-c", "true"}, "", true, nil,
		ExecuteOption{StdinDevNull: true})
	if err == nil || !strings.Contains(err.Error(), "ptyEnabled") {
		t.Errorf("StdinDevNull + pty = %v, want a refusal naming ptyEnabled", err)
	}

	stream2 := &cancellableStream{newEOFBidiStream()}
	err = ExecuteCommandWithOption(context.Background(), stream2, slog.Default(),
		"/bin/sh", []string{"-c", "true"}, "", false, nil,
		ExecuteOption{StdinDevNull: true, OnStdinWriter: func(func([]byte) (int, error)) {}})
	if err == nil || !strings.Contains(err.Error(), "OnStdinWriter") {
		t.Errorf("StdinDevNull + OnStdinWriter = %v, want a refusal naming OnStdinWriter", err)
	}
}

// os.DevNull is the portable spelling; assert it resolves, so a platform where
// it does not turns into a named failure here rather than a child with no
// stdin somewhere downstream.
func TestDevNullOpens(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("os.Open(%q): %v", os.DevNull, err)
	}
	defer f.Close()
	var buf [1]byte
	if _, err := f.Read(buf[:]); !errors.Is(err, os.ErrClosed) && err == nil {
		t.Errorf("reading %s returned data, want EOF", os.DevNull)
	}
}
