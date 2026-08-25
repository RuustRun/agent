//go:build !linux

package hostcap

// Detect is a stub on non-Linux platforms (developer machines), where the host's
// /proc is not the box the agent would run on in production. It reports no
// capacity, so the agent omits the fields and the control plane keeps whatever it
// already has. British English throughout. No em dashes.
func Detect() (cpuCores float64, totalRamMb int) {
	return 0, 0
}
