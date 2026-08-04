//go:build !windows && !js

package exec

import "io"

// platformSwallowLocalReadEOF is a no-op outside Windows: no platform here
// turns a keystroke into a read error, so an error from the local terminal is
// genuine and ends the input path. See the Windows variant for the artefact
// this exists to absorb.
func platformSwallowLocalReadEOF(io.Reader, error) bool { return false }
