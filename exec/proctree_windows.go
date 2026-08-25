//go:build windows

package exec

import (
	"fmt"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

// procTree makes a child killable together with everything it spawned.
//
// Windows has no process groups in the unix sense — CREATE_NEW_PROCESS_GROUP
// governs Ctrl+C delivery, not lifetime — so the mechanism is a JOB OBJECT with
// KILL_ON_JOB_CLOSE: every process the child starts is created inside the job,
// and closing the handle terminates all of them.
//
// Nothing here happens by default. os/exec's own cancellation calls
// TerminateProcess on the direct child ONLY, which is correct for a command
// that is one process and wrong for a shell: `cmd /c "timeout 300 & rem"`
// leaves cmd.exe dead and timeout running, in nobody's bookkeeping.
type procTree struct{ job windows.Handle }

// newProcTree creates the job before the process exists, so the handle is ready
// to assign the moment it does.
func newProcTree(*exec.Cmd) (*procTree, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("set job limits: %w", err)
	}
	return &procTree{job: job}, nil
}

// bind puts the started process into the job. Everything it starts afterwards
// is created inside the job by inheritance.
//
// There IS a window here — between Start and this call, a grandchild started by
// a very fast child would escape — and it cannot be closed with os/exec: doing
// so needs CREATE_SUSPENDED and a ResumeThread on the primary thread handle,
// which os/exec neither exposes nor keeps. The window is microseconds against a
// shell that has to load and parse first. Named rather than hidden.
func (t *procTree) bind(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	h, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		return fmt.Errorf("open child process: %w", err)
	}
	defer windows.CloseHandle(h)
	if err := windows.AssignProcessToJobObject(t.job, h); err != nil {
		return fmt.Errorf("assign to job object: %w", err)
	}
	return nil
}

// kill closes the job handle, which terminates every process in it.
//
// There is no TERM-then-KILL ladder because Windows has no TERM to send: a
// console app can be asked to stop with a CTRL_BREAK event, but only if it
// shares a console and only if it installed a handler, and neither holds for a
// command run out of band with no console at all.
func (t *procTree) kill(*exec.Cmd) error {
	if t.job == 0 {
		return nil
	}
	err := windows.CloseHandle(t.job)
	t.job = 0
	return err
}

// release closes the handle if kill never ran — which also kills the job, so it
// must run only after the child has been waited for.
func (t *procTree) release() {
	if t.job != 0 {
		windows.CloseHandle(t.job)
		t.job = 0
	}
}
