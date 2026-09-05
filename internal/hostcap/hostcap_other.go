//go:build !linux

package hostcap

import "syscall"

// Detect is a stub on non-Linux platforms (developer machines), where the host's
// /proc is not the box the agent would run on in production. It reports no
// capacity, so the agent omits the fields and the control plane keeps whatever it
// already has. British English throughout. No em dashes.
func Detect() (cpuCores float64, totalRamMb int) {
	return 0, 0
}

// DetectDisk statfs the tenant-data filesystem (dataDir) so a dev box still shows
// real disk. All fields are cast to uint64 so field-type differences between OSes
// do not matter. Either value is 0 on a miss.
func DetectDisk() (totalGb, freeGb int) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir(), &st); err != nil {
		return 0, 0
	}
	bsize := uint64(st.Bsize)
	const gb = 1 << 30
	return int(uint64(st.Blocks) * bsize / gb), int(uint64(st.Bavail) * bsize / gb)
}
