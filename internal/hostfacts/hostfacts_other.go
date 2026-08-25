//go:build !linux

package hostfacts

// Detect on a non-Linux host (local development on macOS) reports nothing. The
// empty fields are omitted from the status report, so they never overwrite a real
// Linux node's values on the control plane.
func Detect() Facts {
	return Facts{}
}
