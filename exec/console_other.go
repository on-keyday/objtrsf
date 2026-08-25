//go:build !windows && !js

package exec

import "os/exec"

// applyConsoleWindow is a no-op outside Windows: no platform here allocates a
// window for a child process, so there is nothing to suppress. See the Windows
// variant for what it is for.
func applyConsoleWindow(*exec.Cmd, bool) {}
