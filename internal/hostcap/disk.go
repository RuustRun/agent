// Shared (non OS-specific) helper for locating the filesystem whose capacity the
// agent reports as the host's tenant-data disk. The per-OS DetectDisk functions
// statfs this path. British English. No em dashes.
package hostcap

import "os"

// dataDir is the directory whose filesystem holds tenant data (Egg volumes, backup
// snapshots). Its free/total space is what the control plane budgets placement
// against. RUUST_DATA_DIR overrides it; otherwise the first existing of the usual
// data mounts, falling back to the root filesystem (so a dev box still reports
// something sensible).
func dataDir() string {
	if d := os.Getenv("RUUST_DATA_DIR"); d != "" {
		return d
	}
	if d := os.Getenv("RUUST_BACKUP_DIR"); d != "" {
		return d
	}
	for _, p := range []string{"/var/lib/ruust", "/var/lib/docker", "/var/lib", "/"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "/"
}
