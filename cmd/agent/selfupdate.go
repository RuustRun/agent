package main

// Self-update rollback safety.
//
// The agent replaces its own binary in place when the control plane serves a newer
// build (see maybeSelfUpdate). That is convenient but dangerous: a bad build could
// take down the whole fleet at once. This file makes the update reversible.
//
// The scheme, all local to the node (no coordination):
//   - Before swapping in the new binary, we keep the current one as <binary>.prev
//     and write a probation marker <binary>.update recording the from/to versions.
//   - On EVERY startup, handleUpdateProbation counts the boot. If the new build
//     keeps exiting before it commits as healthy, after a few attempts we restore
//     the previous binary, QUARANTINE the bad version (so we do not immediately
//     fetch it again), and exit so systemd restarts into the known-good build.
//   - Once the new build has stayed up for a probation window, commitUpdate clears
//     the marker and the kept binary. The update is now permanent.
//
// This defends against a build that fails to start or crashes early. A build that
// starts, looks healthy, then misbehaves is the job of staged rollout, not this.
//
// British English throughout. No em dashes.

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	updateStateSuffix  = ".update"      // probation marker (JSON updateState)
	prevBinarySuffix   = ".prev"        // the previous binary, kept for rollback
	quarantineSuffix   = ".badversions" // versions that failed and must not be re-fetched
	updateCommitWindow = 90 * time.Second
	maxUpdateAttempts  = 3
)

// updateState is the probation marker written before an in-place update.
type updateState struct {
	FromVersion string `json:"fromVersion"`
	ToVersion   string `json:"toVersion"`
	Attempts    int    `json:"attempts"`
}

// selfPath resolves the running binary's real path (following any symlink).
func selfPath() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	if resolved, rerr := filepath.EvalSymlinks(self); rerr == nil {
		self = resolved
	}
	return self, nil
}

// copyFile copies src to dst atomically (via a temp file in dst's directory) and
// makes it executable, so a half-written binary is never left in place.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".ruust-cp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// quarantinedVersions reads the set of versions that have failed and must not be
// fetched again (until a different, presumably fixed, version is served).
func quarantinedVersions(self string) map[string]bool {
	out := map[string]bool{}
	data, err := os.ReadFile(self + quarantineSuffix)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v := strings.TrimSpace(line); v != "" {
			out[v] = true
		}
	}
	return out
}

// quarantinedList returns the versions this node fetched, failed to run, and rolled
// back from, for reporting to the control plane so an operator sees a failed update.
func quarantinedList() []string {
	self, err := selfPath()
	if err != nil {
		return nil
	}
	// Non-nil so it marshals to [] not null, and always sending it (below) lets the
	// control plane clear the alert once the quarantine file is cleared.
	out := []string{}
	data, err := os.ReadFile(self + quarantineSuffix)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v := strings.TrimSpace(line); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// quarantineVersion records a version that failed to run, so maybeSelfUpdate will
// skip it and the node stays on the last good build until a new version is served.
func quarantineVersion(self, version string) {
	f, err := os.OpenFile(self+quarantineSuffix, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.WriteString(version + "\n")
}

// handleUpdateProbation runs at the very start of main. If this binary was just
// self-updated it is on probation: we count the boot, and if the new build keeps
// exiting before committing (a crash-looping bad build) we roll back to the kept
// previous binary and quarantine the bad version. A no-op when not on probation.
func handleUpdateProbation(log *slog.Logger) {
	self, err := selfPath()
	if err != nil {
		return
	}
	statePath := self + updateStateSuffix
	data, err := os.ReadFile(statePath)
	if err != nil {
		return // not on probation
	}
	var st updateState
	if json.Unmarshal(data, &st) != nil {
		_ = os.Remove(statePath)
		return
	}

	st.Attempts++
	if st.Attempts > maxUpdateAttempts {
		prev := self + prevBinarySuffix
		if cerr := copyFile(prev, self); cerr != nil {
			// Cannot restore the previous binary; drop the markers so we do not loop
			// forever, and stay on the new build (systemd keeps it running).
			log.Error("self-update rollback failed; staying on the new build", "err", cerr)
			_ = os.Remove(statePath)
			_ = os.Remove(prev)
			return
		}
		quarantineVersion(self, st.ToVersion)
		log.Warn("self-update rolled back after repeated failed boots",
			"badVersion", st.ToVersion, "restored", st.FromVersion, "attempts", st.Attempts)
		_ = os.Remove(statePath)
		_ = os.Remove(prev)
		os.Exit(0) // systemd restarts into the restored previous binary
	}

	if b, merr := json.Marshal(st); merr == nil {
		_ = os.WriteFile(statePath, b, 0o600)
	}
	log.Info("agent on probation after self-update", "version", st.ToVersion, "attempt", st.Attempts)
}

// commitUpdate clears the probation marker and the kept previous binary once the
// new build has proven healthy (stayed up for the commit window). A no-op when the
// agent is not on probation.
func commitUpdate(log *slog.Logger) {
	self, err := selfPath()
	if err != nil {
		return
	}
	statePath := self + updateStateSuffix
	if _, serr := os.Stat(statePath); serr != nil {
		return // not on probation; nothing to commit
	}
	_ = os.Remove(statePath)
	_ = os.Remove(self + prevBinarySuffix)
	log.Info("self-update committed: the new agent build is healthy")
}
