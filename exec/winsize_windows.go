//go:build windows

package exec

import (
	"os"
	"time"

	"golang.org/x/term"
)

// windowSizePollInterval is how often the Windows fallback re-queries the
// console buffer size. There is no portable resize signal on Windows that
// a Go program can subscribe to without dropping into Console API land
// (ReadConsoleInput → WINDOW_BUFFER_SIZE_EVENT, requires
// ENABLE_WINDOW_INPUT and competes with stdin reads). 250ms keeps the
// resize feel responsive while making the polling overhead negligible.
const windowSizePollInterval = 250 * time.Millisecond

// getConsoleSize returns the current console (rows, cols). It must be
// called against the console *output* handle: GetConsoleScreenBufferInfo,
// which term.GetSize wraps on Windows, requires a screen-buffer handle and
// fails with ERROR_INVALID_HANDLE on a console input handle.
// term.GetSize itself returns (width, height) — i.e. (cols, rows) — so we
// re-order to match SetTerminalWindowSize's (rows, columns, ...) signature.
func getConsoleSize() (rows, cols int, err error) {
	cols, rows, err = term.GetSize(int(os.Stdout.Fd()))
	return rows, cols, err
}

// startWindowSizeForwarder polls the local terminal dimensions every
// windowSizePollInterval and invokes sendSize when they change. This is
// the Windows fallback for the Unix SIGWINCH-driven path. The returned
// func stops the goroutine.
func startWindowSizeForwarder(sendSize func() error) func() {
	done := make(chan struct{})
	go func() {
		var lastRows, lastCols int
		haveLast := false
		ticker := time.NewTicker(windowSizePollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rows, cols, err := getConsoleSize()
				if err != nil {
					continue
				}
				if haveLast && rows == lastRows && cols == lastCols {
					continue
				}
				lastRows, lastCols = rows, cols
				haveLast = true
				_ = sendSize()
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

func (w *CommandExecutionStream) sendWindowSize() error {
	rows, cols, err := getConsoleSize()
	if err != nil {
		return err
	}
	return w.SetTerminalWindowSize(uint16(rows), uint16(cols), 0, 0)
}
