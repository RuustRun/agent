// Package hostfacts gathers OS and patch facts about the physical box (OS name and
// version, kernel, pending security updates, and whether a reboot is required), so
// the agent can report them to the control plane for the operator fleet view. The
// cheap facts (OS, kernel) are read every call; the expensive security-update check
// spawns a process and is cached, refreshed at most every few hours.
//
// Every read is defensive: a failure yields a zero value, or nil for the security
// count (so "unknown" stays distinct from "zero, patched"), rather than an error,
// so a miss never sinks a status report. British English throughout. No em dashes.
package hostfacts

// Facts is what the agent reports about the host's OS and patch state. Empty fields
// are omitted from the status report and never overwrite a known-good Host value.
type Facts struct {
	OSName    string
	OSVersion string
	Kernel    string
	// SecurityUpdates is the count of pending security updates, or nil when it could
	// not be determined, so the control plane can tell "0, patched" from "unknown".
	SecurityUpdates *int
	RebootRequired  bool
}
