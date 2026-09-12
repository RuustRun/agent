//go:build linux

// Package hostcap detects the physical box's capacity (CPU cores and total RAM)
// from the host's /proc, so the agent can report it to the control plane on every
// heartbeat. It reports HOST values, not the container's cgroup limits, so a
// resized VPS is reflected automatically.
//
// Every read is defensive: a failure yields 0 for that field rather than an
// error, so a detection miss can never sink a status report. The control plane
// treats 0 as "no change" and never overwrites a known-good value with it.
//
// British English throughout. No em dashes.
package hostcap

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// xfsSuperMagic is the statfs f_type of an xfs filesystem.
const xfsSuperMagic = 0x58465342

// QuotaEnforced reports whether this host can hard-enforce a per-volume size quota:
// its tenant-data filesystem is xfs, mounted with prjquota, and the xfs_quota tool
// is available. Returns a non-nil bool on Linux (false means volume sizes are
// advisory only here); the caller sends it every heartbeat so operators can see
// which hosts actually cap disk.
func QuotaEnforced() *bool {
	no := false
	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir(), &st); err != nil {
		return &no
	}
	if int64(st.Type) != xfsSuperMagic {
		return &no
	}
	if !mountHasPrjquota(dataDir()) {
		return &no
	}
	if _, err := exec.LookPath("xfs_quota"); err != nil {
		return &no
	}
	yes := true
	return &yes
}

// mountHasPrjquota reports whether the filesystem mounted over path carries the
// prjquota (project quota) option, by reading /proc/mounts and matching the longest
// mount point that is a prefix of path.
func mountHasPrjquota(path string) bool {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false
	}
	defer f.Close()
	best, bestOpts := "", ""
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text()) // device mountpoint fstype options ...
		if len(fields) < 4 {
			continue
		}
		mp := fields[1]
		if strings.HasPrefix(path, mp) && len(mp) > len(best) {
			best, bestOpts = mp, fields[3]
		}
	}
	for _, o := range strings.Split(bestOpts, ",") {
		if o == "prjquota" || o == "pquota" {
			return true
		}
	}
	return false
}

// Detect returns the host's CPU core count and total RAM in MB. Either may be 0
// when it cannot be read; the caller omits a zero field from the report.
func Detect() (cpuCores float64, totalRamMb int) {
	return detectCores(), detectRamMb()
}

// DetectDisk returns the total and available space (GB) of the tenant-data
// filesystem (see dataDir). Either is 0 on a statfs miss, which the caller omits so
// it never overwrites a known value.
func DetectDisk() (totalGb, freeGb int) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir(), &st); err != nil {
		return 0, 0
	}
	bsize := uint64(st.Bsize)
	const gb = 1 << 30
	return int(uint64(st.Blocks) * bsize / gb), int(uint64(st.Bavail) * bsize / gb)
}

// detectCores counts the host's logical CPUs from /proc/cpuinfo. We read /proc
// (the host view) rather than a cgroup cpu.max, which would be the container's
// share. runtime.NumCPU is only a fallback if the parse yields nothing.
func detectCores() float64 {
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		defer f.Close()
		count := 0
		s := bufio.NewScanner(f)
		for s.Scan() {
			if strings.HasPrefix(s.Text(), "processor") {
				count++
			}
		}
		if count > 0 {
			return float64(count)
		}
	}
	if n := runtime.NumCPU(); n > 0 {
		return float64(n)
	}
	return 0
}

// detectRamMb reads MemTotal (in kB) from /proc/meminfo, the host's total RAM,
// and converts it to MB. Not the container's cgroup memory.max.
func detectRamMb() int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line) // "MemTotal:  16308100 kB"
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb <= 0 {
			return 0
		}
		return int(kb / 1024)
	}
	return 0
}
