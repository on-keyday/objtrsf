//go:build windows

package exec

import (
	"os/exec"
	"syscall"
)

// createNoWindow is CREATE_NO_WINDOW. The child runs without a console WINDOW;
// it still gets a console, so stdout/stderr redirection is unaffected. Spelled
// out rather than pulled from x/sys/windows, which this package would otherwise
// not need at all on this path.
const createNoWindow = 0x08000000

// applyConsoleWindow suppresses the console window a Windows console
// application otherwise gets, unless the caller asked to keep it.
//
// Whether a window actually APPEARS depends on the parent: a child inherits an
// existing console and pops nothing, so a runner started from a terminal hides
// the problem entirely. A runner started by Task Scheduler or as a service has
// no console to inherit, and every child then allocates a new window on the
// operator's desktop for as long as it lives. That is the case this is for, and
// it is invisible to anyone not watching that desktop.
//
// The harness this serves learned it from git: shelling out constantly made
// terminals blink in and out while nothing was wrong, and its runner/hostcmd
// package exists to set exactly this flag. That package cannot reach here —
// different module — so the same discipline has to be stated on this side.
//
// Deliberately NOT applied to the PTY path, and the same package's reasoning
// says why: a PTY session is a process the operator ATTACHES to and is meant to
// see. Suppressing its window would be suppressing the feature.
//
// Measured on a live Windows runner, both halves. Asking the child itself —
// GetConsoleWindow() through PowerShell — returns 0 down this path and a real
// handle for the same command typed into a PTY session on the same host, so the
// flag lands here and stays off there. The operator confirmed no window
// appeared on the desktop, which is the half no probe from another host can
// see. (MainWindowHandle was tried first and is useless: it is 0 for a console
// process either way, because the window belongs to conhost.)
func applyConsoleWindow(cmd *exec.Cmd, show bool) {
	if show {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// |=, not =: the process-tree setup may have put its own attributes here.
	cmd.SysProcAttr.CreationFlags |= createNoWindow
}
