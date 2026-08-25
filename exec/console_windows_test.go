//go:build windows

package exec

import (
	"os/exec"
	"syscall"
	"testing"
)

// The default is OFF: a non-PTY child gets no console window. This is the whole
// point of the option existing as an opt-OUT.
func TestApplyConsoleWindowSuppressesByDefault(t *testing.T) {
	cmd := exec.Command("cmd", "/c", "rem")
	applyConsoleWindow(cmd, false)
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.CreationFlags&createNoWindow == 0 {
		t.Error("CREATE_NO_WINDOW is not set: a Windows child would pop a console window")
	}
}

// ShowConsoleWindow leaves the child alone, and does not invent a SysProcAttr
// for a caller that had none.
func TestApplyConsoleWindowRespectsTheOptOut(t *testing.T) {
	cmd := exec.Command("cmd", "/c", "rem")
	applyConsoleWindow(cmd, true)
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.CreationFlags&createNoWindow != 0 {
		t.Error("CREATE_NO_WINDOW was set despite ShowConsoleWindow")
	}
}

// It must OR into whatever is already there. The process-tree setup writes to
// the same struct, and an assignment would silently drop it.
func TestApplyConsoleWindowKeepsExistingFlags(t *testing.T) {
	const other = 0x00000200 // CREATE_NEW_PROCESS_GROUP
	cmd := exec.Command("cmd", "/c", "rem")
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: other}
	applyConsoleWindow(cmd, false)
	if cmd.SysProcAttr.CreationFlags&other == 0 {
		t.Error("an existing creation flag was dropped")
	}
	if cmd.SysProcAttr.CreationFlags&createNoWindow == 0 {
		t.Error("CREATE_NO_WINDOW was not added")
	}
}
