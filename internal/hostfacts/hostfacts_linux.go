//go:build linux

package hostfacts

import (
	"bufio"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The security-update check spawns update-notifier's apt-check, so we do it at most
// once every patchTTL and cache the result, reporting the cached value each tick.
const patchTTL = 3 * time.Hour

var (
	mu             sync.Mutex
	cachedSecurity *int
	cachedReboot   bool
	lastPatchCheck time.Time
)

// Detect reads the host's OS and patch facts. OS and kernel are read every call
// (cheap file reads); the security-update count and reboot flag are cached and
// refreshed at most once every patchTTL.
func Detect() Facts {
	name, version := osRelease()
	f := Facts{
		OSName:    name,
		OSVersion: version,
		Kernel:    kernelRelease(),
	}

	mu.Lock()
	if lastPatchCheck.IsZero() || time.Since(lastPatchCheck) > patchTTL {
		cachedSecurity = securityUpdates()
		cachedReboot = rebootRequired()
		lastPatchCheck = time.Now()
	}
	f.SecurityUpdates = cachedSecurity
	f.RebootRequired = cachedReboot
	mu.Unlock()

	return f
}

// osRelease parses NAME and VERSION_ID from /etc/os-release, unquoting the values.
func osRelease() (name, version string) {
	f, err := os.Open("/etc/os-release")
	if err != nil {
		return "", ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		k, v, ok := strings.Cut(s.Text(), "=")
		if !ok {
			continue
		}
		v = strings.Trim(strings.TrimSpace(v), `"`)
		switch strings.TrimSpace(k) {
		case "NAME":
			name = v
		case "VERSION_ID":
			version = v
		}
	}
	return name, version
}

// kernelRelease reads the running kernel version from /proc.
func kernelRelease() string {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// securityUpdates returns the pending security-update count via update-notifier's
// apt-check helper, which prints "<updates>;<security>". Nil when it is absent or
// unparseable, so "unknown" stays distinct from "zero".
func securityUpdates() *int {
	out, err := exec.Command("/usr/lib/update-notifier/apt-check").CombinedOutput()
	if err != nil {
		return nil
	}
	parts := strings.Split(strings.TrimSpace(string(out)), ";")
	if len(parts) != 2 {
		return nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil
	}
	return &n
}

// rebootRequired reports whether the OS has flagged that a reboot is needed
// (Debian and Ubuntu drop this file after certain package upgrades).
func rebootRequired() bool {
	_, err := os.Stat("/var/run/reboot-required")
	return err == nil
}
