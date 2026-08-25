//go:build !windows && !js

package exec

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// processExists reports whether pid is a live process. Signal 0 performs the
// error checking without delivering anything, which is the portable probe.
func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// runProbe starts a shell that FORKS a sleeper, waits until the grandchild
// exists, cancels the session, and reports whether the grandchild is still
// alive afterwards.
//
// The fork is the whole point. `sh -c 'sleep 120'` is a simple command, so the
// shell EXECS it and replaces itself — there is no grandchild to strand, and a
// test built on that shape passes with or without the option. The `&` plus
// `wait` is what makes the shell a parent.
func runProbe(t *testing.T, killTree bool) (survived bool) {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	script := "sleep 120 & echo $! > " + pidFile + "; wait"

	stream := &cancellableStream{newEOFBidiStream()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(ctx, stream, slog.Default(),
			"/bin/sh", []string{"-c", script}, "", false, nil,
			ExecuteOption{StdinDevNull: true, KillProcessTree: killTree})
	}()

	pid := waitForPID(t, pidFile, cancel)
	t.Cleanup(func() {
		if processExists(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	cancel()

	// Deliberately NOT waiting on done for both arms. Without the tree kill the
	// session does not return AT ALL: the orphaned grandchild inherited the
	// child's stdout pipe, so the output copier never sees EOF. That is a
	// second face of the same defect and it is asserted separately below.
	if killTree {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("the session did not end after a tree kill")
		}
	}

	// A tree kill is TERM then KILL after treeKillGrace, so allow that plus
	// slack before concluding the grandchild survived.
	until := time.Now().Add(treeKillGrace + 3*time.Second)
	for time.Now().Before(until) {
		if !processExists(pid) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return processExists(pid)
}

// The stranded grandchild does not merely outlive the command — it holds the
// command's stdout pipe open, so the SESSION never ends either. On the harness
// that means a runner goroutine parked forever on a stream the server has
// already forgotten, which is why `exec kill` could report success while
// leaving work behind on both sides.
func TestWithoutKillProcessTreeTheSessionNeverEnds(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	script := "sleep 120 & echo $! > " + pidFile + "; wait"
	stream := &cancellableStream{newEOFBidiStream()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ExecuteCommandWithOption(ctx, stream, slog.Default(),
			"/bin/sh", []string{"-c", script}, "", false, nil,
			ExecuteOption{StdinDevNull: true})
	}()

	pid := waitForPID(t, pidFile, cancel)
	t.Cleanup(func() {
		if processExists(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	cancel()

	select {
	case <-done:
		t.Error("the session ended without a tree kill — the orphan no longer holds the pipe, " +
			"so KillProcessTree's second reason has gone away and should be re-examined")
	case <-time.After(5 * time.Second):
		// Expected: parked on a pipe the orphan holds open.
	}
}

// waitForPID blocks until the probe script has written its grandchild's pid.
func waitForPID(t *testing.T, pidFile string, cancel context.CancelFunc) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if b, err := os.ReadFile(pidFile); err == nil {
			if p, cerr := strconv.Atoi(strings.TrimSpace(string(b))); cerr == nil && p > 0 {
				return p
			}
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the probe never wrote its grandchild pid")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Cancelling a session started with KillProcessTree reaches the grandchild.
func TestKillProcessTreeReachesTheGrandchild(t *testing.T) {
	if runProbe(t, true) {
		t.Error("the grandchild survived a tree kill")
	}
}

// The control, and the defect this option exists for: WITHOUT it, os/exec kills
// the shell and the sleeper it forked is adopted by init and runs on. Measured
// against a live runner before any of this existed — the operator's tooling
// reported the command as gone while the process was still there.
//
// If this ever stops surviving, os/exec's cancellation changed and the option's
// reason to be opt-in should be revisited rather than assumed.
func TestWithoutKillProcessTreeTheGrandchildSurvives(t *testing.T) {
	if !runProbe(t, false) {
		t.Error("the grandchild died without a tree kill — the control no longer controls")
	}
}

// A tree kill must never signal the CALLER's group. The negative pgid is the
// leader's pid, so a kill attempted before Start would pass 0 and reach every
// process in the runner's own group — the runner and everything it hosts.
func TestKillProcessTreeBeforeStartIsANoOp(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "true") // never started
	tree, err := newProcTree(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.release()
	if err := tree.kill(cmd); err != nil {
		t.Errorf("kill on an unstarted command = %v, want a silent no-op", err)
	}
}
