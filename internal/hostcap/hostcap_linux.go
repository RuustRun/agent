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
	"runtime"
	"strconv"
	"strings"
)

// Detect returns the host's CPU core count and total RAM in MB. Either may be 0
// when it cannot be read; the caller omits a zero field from the report.
func Detect() (cpuCores float64, totalRamMb int) {
	return detectCores(), detectRamMb()
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
