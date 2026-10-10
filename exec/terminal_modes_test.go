//go:build !js

package exec

import (
	"bytes"
	"strings"
	"testing"
)

// The reset is written to a terminal the caller may not own — an ssh channel
// rather than os.Stdout — so what matters is that ONE call emits both groups.
// A caller that writes only one leaves either a stranded alternate screen or a
// terminal still sending mouse reports at its owner's shell prompt.
func TestWriteTerminalReset_EmitsBothGroups(t *testing.T) {
	var buf bytes.Buffer
	WriteTerminalReset(&buf)
	got := buf.String()

	for _, want := range []string{
		"\x1b[?1049l", // screen group: leave the alternate screen
		"\x1b[r",      // screen group: reset the scroll region (DECSTBM)
		"\x1b[?6l",    // screen group: reset origin mode (DECOM)
		"\x1b[?1006l", // input group: SGR mouse reporting off
		"\x1b[?2004l", // input group: bracketed paste off
		"\x1b[?2031l", // input group: colour-scheme notifications off
	} {
		if !strings.Contains(got, want) {
			t.Errorf("WriteTerminalReset output is missing %q", want)
		}
	}
}

func TestWriteTerminalReset_IsExactlyTheTwoConsts(t *testing.T) {
	var buf bytes.Buffer
	WriteTerminalReset(&buf)
	if want := ScreenModeReset + InputModeReset; buf.String() != want {
		t.Errorf("WriteTerminalReset wrote %q, want ScreenModeReset+InputModeReset", buf.String())
	}
}

// DECSTBM and DECOM each home the cursor, so on a terminal already at defaults
// the reset used to move the caller's next output to row 1. Both are wrapped in
// DECSC/DECRC, which keeps the position: the save must come before the first of
// them and the restore after the last.
func TestScreenModeReset_KeepsTheCursorAcrossTheHomingResets(t *testing.T) {
	save := strings.Index(ScreenModeReset, "\x1b7")
	restore := strings.LastIndex(ScreenModeReset, "\x1b8")
	stbm := strings.Index(ScreenModeReset, "\x1b[r")
	decom := strings.Index(ScreenModeReset, "\x1b[?6l")
	if save < 0 || restore < 0 {
		t.Fatalf("ScreenModeReset has no DECSC/DECRC pair: %q", ScreenModeReset)
	}
	if !(save < stbm && save < decom && stbm < restore && decom < restore) {
		t.Fatalf("DECSTBM (%d) and DECOM (%d) must sit between DECSC (%d) and DECRC (%d)", stbm, decom, save, restore)
	}
}
