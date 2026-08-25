//go:build !windows && !js

package exec

import (
	"os/exec"
	"syscall"
	"time"
)

// procTree makes a child killable together with everything it spawned.
//
// On unix that is a process GROUP: the child becomes its own group leader
// (Setpgid), so one kill to the negative pgid reaches every descendant that has
// not deliberately left the group.
//
// Nothing here happens by default. os/exec's own cancellation kills the direct
// child ONLY, which is correct for a command that is one process and wrong for
// a shell: `sh -c 'sleep 300; :'` leaves sh dead and sleep running, adopted by
// init, in nobody's bookkeeping. Measured against a live runner before this
// existed.
type procTree struct{}

func newProcTree(cmd *exec.Cmd) (*procTree, error) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return &procTree{}, nil
}

// bind is a no-op: the group is established by the fork itself, so there is no
// window between starting and being killable. The Windows counterpart has one.
func (t *procTree) bind(*exec.Cmd) error { return nil }

// kill signals the whole group, TERM first and KILL after a grace.
//
// TERM first because a build tool or a test runner has cleanup worth running,
// and this is reached when an operator stops a command rather than when
// something has gone wrong. KILL after, because a process that ignores TERM is
// exactly what leaving it alive would strand.
//
// The negative pgid is the process GROUP, and it is the same number as the
// leader's pid — which is why this must never run before Start: pid 0 would
// signal the CALLER's group, i.e. the runner and everything it is running.
func (t *procTree) kill(cmd *exec.Cmd) error {
	if cmd.Process == nil || cmd.Process.Pid <= 0 {
		return nil
	}
	pgid := cmd.Process.Pid
	err := syscall.Kill(-pgid, syscall.SIGTERM)
	time.AfterFunc(treeKillGrace, func() {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	})
	return err
}

func (t *procTree) release() {}
