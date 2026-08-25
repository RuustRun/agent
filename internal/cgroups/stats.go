// Package cgroups reads cgroup v2 statistics directly from the filesystem, per
// the plan (the agent reads cgroup v2 stats directly rather than shelling out).
// It reports memory, CPU and egress usage for a container's cgroup.
//
// Egress is read for observability only. It is never used to cap or bill
// traffic, since egress is unmetered on every tier. There is no egress cap
// anywhere in the agent.
//
// Reads are defensive: a missing file or an unreadable cgroup yields a zero
// value for that field rather than an error, so a single unreadable stat never
// fails the whole host status report. This package is Linux-oriented (cgroup v2
// lives under /sys/fs/cgroup), but it compiles and returns zeroes elsewhere so
// the agent builds on any platform for local development.
package cgroups

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/RuustRun/agent/internal/contract"
)

// DefaultRoot is the standard cgroup v2 unified hierarchy mount point.
const DefaultRoot = "/sys/fs/cgroup"

// Reader reads cgroup v2 stats from a filesystem root. It carries a small amount
// of state so CPU usage can be reported as a fraction of a core between two
// samples.
type Reader struct {
	// prevCPUUsec remembers the last cumulative CPU microseconds per cgroup path,
	// so CpuUsage can be a rate rather than a lifetime total.
	prevCPUUsec map[string]cpuSample
}

type cpuSample struct {
	usageUsec int64
	// wallNanos is the monotonic-ish wall time of the sample, in nanoseconds,
	// supplied by the caller via SampleAt. It lets us turn a delta of CPU
	// microseconds into a fraction of one core.
	wallNanos int64
}

// NewReader constructs a stats reader.
func NewReader() *Reader {
	return &Reader{prevCPUUsec: map[string]cpuSample{}}
}

// Usage reads memory, CPU and egress for the cgroup at cgroupPath, using wallNow
// (nanoseconds, monotonic) to compute the CPU fraction against the previous
// sample. The first sample for a path reports a CPU fraction of zero because
// there is no prior point to difference against.
//
// A blank cgroupPath (for example when the container's cgroup could not be
// resolved) returns a zero-valued usage without error.
func (r *Reader) Usage(cgroupPath string, wallNow int64) contract.CgroupUsage {
	usage := contract.CgroupUsage{}
	if cgroupPath == "" {
		return usage
	}

	usage.MemoryBytes = r.readMemoryBytes(cgroupPath)
	usage.CpuUsage = r.readCPUFraction(cgroupPath, wallNow)
	usage.EgressBytes = r.readEgressBytes(cgroupPath)
	return usage
}

// readMemoryBytes reads memory.current, the current memory usage in bytes.
func (r *Reader) readMemoryBytes(cgroupPath string) int64 {
	return readInt64(filepath.Join(cgroupPath, "memory.current"))
}

// readCPUFraction reads cpu.stat's usage_usec (cumulative CPU microseconds) and
// converts the delta since the last sample into a fraction of one core.
func (r *Reader) readCPUFraction(cgroupPath string, wallNow int64) float64 {
	usageUsec := readCPUUsageUsec(filepath.Join(cgroupPath, "cpu.stat"))
	prev, ok := r.prevCPUUsec[cgroupPath]
	r.prevCPUUsec[cgroupPath] = cpuSample{usageUsec: usageUsec, wallNanos: wallNow}
	if !ok || wallNow <= prev.wallNanos {
		return 0
	}
	// CPU microseconds -> nanoseconds is x1000. Divide the CPU-time delta by the
	// wall-time delta to get cores used.
	cpuDeltaNanos := float64(usageUsec-prev.usageUsec) * 1000.0
	wallDeltaNanos := float64(wallNow - prev.wallNanos)
	if wallDeltaNanos <= 0 {
		return 0
	}
	frac := cpuDeltaNanos / wallDeltaNanos
	if frac < 0 {
		return 0
	}
	return frac
}

// readEgressBytes reads egress (transmit) bytes for the cgroup. cgroup v2 does
// not itself expose per-cgroup network counters, so egress is derived from the
// per-interface transmit counters of the network namespace pinned under this
// cgroup path (a symlink or sidecar file the agent maintains).
//
// Egress is reported for observability only and is never capped: egress is
// unmetered on every tier.
//
// TODO(phase-1): wire this to the container's veth transmit counter (read
// /sys/class/net/<veth>/statistics/tx_bytes for the container's peer interface,
// or an eBPF cgroup egress program). Until then we read an optional
// "net.egress_bytes" sidecar the agent may drop next to the cgroup, and report
// zero when it is absent rather than failing the status report.
func (r *Reader) readEgressBytes(cgroupPath string) int64 {
	return readInt64(filepath.Join(cgroupPath, "net.egress_bytes"))
}

// readInt64 reads a single integer from a cgroup file. A missing or malformed
// file (including the cgroup v2 sentinel "max") yields zero.
func readInt64(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(b))
	if s == "" || s == "max" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// readCPUUsageUsec parses cpu.stat and returns the usage_usec line value, the
// cumulative CPU time consumed by the cgroup in microseconds. A missing file
// yields zero.
func readCPUUsageUsec(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "usage_usec" {
			v, perr := strconv.ParseInt(fields[1], 10, 64)
			if perr != nil {
				return 0
			}
			return v
		}
	}
	return 0
}
