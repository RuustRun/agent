//go:build linux

package shaping

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// iface is the device to shape inside the container's netns. Egg containers get a
// single eth0 (the Docker bridge veth peer inside the container).
const iface = "eth0"

// Available reports whether the host has the tooling to shape egress. It does not
// check privilege (that surfaces as an error on the first Apply); missing binaries
// are the common misconfiguration, so we detect those up front.
func Available() bool {
	if _, err := exec.LookPath("nsenter"); err != nil {
		return false
	}
	if _, err := exec.LookPath("tc"); err != nil {
		return false
	}
	return true
}

// Apply sets an egress ceiling of bytesPerSecond on the container whose init process
// is pid, by replacing the root qdisc on its eth0 with a token-bucket filter (tbf).
// Idempotent (uses `replace`). A non-positive rate clears the cap instead.
func Apply(ctx context.Context, pid int, bytesPerSecond int64) error {
	if pid <= 0 {
		return fmt.Errorf("shaping: invalid pid %d", pid)
	}
	if bytesPerSecond <= 0 {
		return Clear(ctx, pid)
	}
	// tc rates are in bits/second. Size the burst at ~1/10th of a second of traffic
	// (min 32 KiB) so short spikes are not choked, and bound the queue with latency.
	rateBits := bytesPerSecond * 8
	burst := bytesPerSecond / 10
	if burst < 32*1024 {
		burst = 32 * 1024
	}
	return nsenterTC(ctx, pid,
		"qdisc", "replace", "dev", iface, "root", "tbf",
		"rate", fmt.Sprintf("%dbit", rateBits),
		"burst", fmt.Sprintf("%d", burst),
		"latency", "50ms",
	)
}

// Clear removes egress shaping from the container at pid. Idempotent: a missing qdisc
// (nothing to delete) is treated as success.
func Clear(ctx context.Context, pid int) error {
	if pid <= 0 {
		return nil
	}
	err := nsenterTC(ctx, pid, "qdisc", "del", "dev", iface, "root")
	if err != nil && isNoSuchQdisc(err) {
		return nil
	}
	return err
}

// nsenterTC runs `nsenter -t <pid> -n tc <args...>`, i.e. tc inside the container's
// network namespace, using the host's own binaries.
func nsenterTC(ctx context.Context, pid int, args ...string) error {
	full := append([]string{"-t", fmt.Sprintf("%d", pid), "-n", "tc"}, args...)
	out, err := exec.CommandContext(ctx, "nsenter", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nsenter tc %v: %w: %s", args, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// isNoSuchQdisc reports whether a `qdisc del` failed only because there was nothing
// to delete (the default pfifo_fast root), which we treat as already-clear.
func isNoSuchQdisc(err error) bool {
	s := err.Error()
	return strings.Contains(s, "No such file or directory") ||
		strings.Contains(s, "Cannot delete qdisc with handle of zero") ||
		strings.Contains(s, "RTNETLINK answers: No such")
}
