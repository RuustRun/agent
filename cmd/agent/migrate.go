package main

// Database Egg migration, agent side. When the desired state carries a migration
// directive on a workload, this host has a part in moving a database Egg's data to
// another host so a node can be drained without losing its host-local volume.
//
//   - SOURCE: the serving container has already been stopped (the source quiesces via
//     replicas 0), so we snapshot the volume with a throwaway isolated container,
//     upload the dump, and report the counts + checksum. The source stays down until
//     the cutover flips placement (or, on failure, resumes serving on the next poll).
//   - TARGET: download the snapshot, restore it into a fresh copy of the volume,
//     read the counts and report verified. The Egg only starts serving here after the
//     control plane confirms the counts match and cuts over, on the normal path.
//
// Every step keys on the migration id and is idempotent. The bytes move through the
// control-plane relay (host-token authenticated) unless the directive carries a
// presigned put/get URL. Snapshot contents and database credentials are never logged.
//
// British English. No em dashes.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// handleMigrations runs any migration directives in the desired state. It is called
// after Converge (so a quiescing source's serving container is already stopped) and
// is best effort: a failure is reported to the control plane and retried next tick,
// and never stops the rest of the loop. Runs serially with the poll loop, so a long
// snapshot or restore simply delays the next tick rather than overlapping itself.
func (a *agent) handleMigrations(ctx context.Context, desired contract.DesiredState) {
	mig, ok := a.docker.(docker.Migrator)
	if !ok {
		return // the real engine client implements Migrator; nothing to do otherwise
	}
	if a.migrationDone == nil {
		a.migrationDone = make(map[string]bool)
	}
	for i := range desired.Workloads {
		w := desired.Workloads[i]
		if w.Migration == nil {
			continue
		}
		m := w.Migration
		// Skip a role we have already completed for this migration in this process, so
		// a report that raced the control plane's state transition does not re-snapshot
		// or re-restore on the next tick.
		key := m.ID + ":" + m.Role
		if a.migrationDone[key] {
			continue
		}
		var err error
		switch m.Role {
		case "source":
			err = a.migrateSource(ctx, mig, w)
		case "target":
			err = a.migrateTarget(ctx, mig, w)
		default:
			continue
		}
		if err != nil {
			a.log.Error("migration step failed, reporting and retrying next tick",
				"migrationId", m.ID, "role", m.Role, "blobId", w.BlobID, "err", err)
			a.reportMigration(ctx, contract.MigrationReport{
				MigrationID: m.ID, Role: m.Role, Phase: phaseOr(m.Phase), State: "failed",
				Error: firstLine(err.Error()),
			})
			continue
		}
		a.migrationDone[key] = true
	}
}

// migrateSource snapshots the live serving container, uploads the dump, and reports
// uploaded. The source keeps serving until the control plane moves it to replicas 0
// once the upload lands, so the dump is taken from a live, consistent database.
func (a *agent) migrateSource(ctx context.Context, mig docker.Migrator, w contract.WorkloadSpec) error {
	m := w.Migration
	a.log.Info("migration: snapshotting source", "migrationId", m.ID, "engine", m.Engine, "blobId", w.BlobID)

	// Find the live serving container for this workload to snapshot it.
	containers, err := a.docker.List(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	var containerID string
	for _, c := range containers {
		if c.WorkloadID == w.ID && c.State == contract.StateRunning {
			containerID = c.ID
			break
		}
	}
	if containerID == "" {
		return fmt.Errorf("no running container to snapshot for workload %s", w.ID)
	}

	tmp, err := os.CreateTemp("", "ruust-msnap-*.dump")
	if err != nil {
		return fmt.Errorf("temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	// Hash the dump as it is written so the checksum is path independent (relay or
	// presigned) and we never re-read the file to compute it.
	hash := sha256.New()
	counts, err := mig.SnapshotLive(ctx, m.Engine, containerID, w.EnvValues, io.MultiWriter(tmp, hash))
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("flush snapshot: %w", err)
	}
	info, err := tmp.Stat()
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("stat snapshot: %w", err)
	}
	size := info.Size()
	checksum := hex.EncodeToString(hash.Sum(nil))

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("rewind snapshot: %w", err)
	}
	if err := a.uploadSnapshot(ctx, m, tmp, size); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("upload: %w", err)
	}
	_ = tmp.Close()

	a.log.Info("migration: source uploaded", "migrationId", m.ID, "sizeBytes", size)
	a.reportMigration(ctx, contract.MigrationReport{
		MigrationID: m.ID, Role: "source", Phase: phaseOr(m.Phase), State: "uploaded",
		Counts: &counts, Checksum: checksum, SizeBytes: size,
	})
	return nil
}

// migrateTarget downloads the snapshot, restores it into a fresh volume, and reports
// verified.
func (a *agent) migrateTarget(ctx context.Context, mig docker.Migrator, w contract.WorkloadSpec) error {
	m := w.Migration
	a.log.Info("migration: restoring on target", "migrationId", m.ID, "engine", m.Engine, "blobId", w.BlobID)

	tmp, err := os.CreateTemp("", "ruust-mrest-*.dump")
	if err != nil {
		return fmt.Errorf("temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	got, err := a.downloadSnapshot(ctx, m, tmp)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("download: %w", err)
	}
	// Verify the download against the source's checksum before restoring, so a
	// truncated or corrupt transfer never silently becomes the live database.
	if m.Checksum != "" && !strings.EqualFold(got, m.Checksum) {
		_ = tmp.Close()
		return fmt.Errorf("snapshot checksum mismatch: got %s, want %s", got, m.Checksum)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("rewind snapshot: %w", err)
	}

	name := "ruust-mrest-" + m.ID
	counts, err := mig.RestoreDatabase(ctx, m.Engine, name, m.VolumeName, w.ImageRef, w.EnvValues, tmp)
	_ = tmp.Close()
	if err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	a.log.Info("migration: target restored and verified", "migrationId", m.ID)
	a.reportMigration(ctx, contract.MigrationReport{
		MigrationID: m.ID, Role: "target", Phase: phaseOr(m.Phase), State: "verified",
		Counts: &counts,
	})
	return nil
}

// uploadSnapshot streams the snapshot file to the relay (host-token authenticated) or
// to a presigned PUT URL if the directive carries one.
func (a *agent) uploadSnapshot(ctx context.Context, m *contract.MigrationDirective, body *os.File, size int64) error {
	url := m.PutURL
	authed := false
	if url == "" {
		url = fmt.Sprintf("%s/api/%s/hosts/%s/migration/%s/upload?phase=%s",
			strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID, m.ID, phaseOr(m.Phase))
		authed = true
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	if authed {
		a.authorise(req)
	}
	resp, err := a.migHTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("upload returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

// downloadSnapshot streams the staged snapshot into dst, returning its sha256 so the
// caller can verify it against the source's checksum. Uses the relay (host-token
// authenticated) or a presigned GET URL if the directive carries one.
func (a *agent) downloadSnapshot(ctx context.Context, m *contract.MigrationDirective, dst io.Writer) (string, error) {
	url := m.GetURL
	authed := false
	if url == "" {
		url = fmt.Sprintf("%s/api/%s/hosts/%s/migration/%s/download?phase=%s",
			strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID, m.ID, phaseOr(m.Phase))
		authed = true
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	if authed {
		a.authorise(req)
	}
	resp, err := a.migHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("download returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(dst, hash), resp.Body); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// backupRelayURL builds this host's Vault-backup staging endpoint for a relay id. The
// control plane never needs to know its own public URL; the agent constructs it from
// its configured control-plane URL, exactly like the migration relay fallback.
func (a *agent) backupRelayURL(relayID, verb string) string {
	return fmt.Sprintf("%s/api/%s/hosts/%s/vault-backup/%s/%s",
		strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID, relayID, verb)
}

// backupUpload PUTs a snapshot's bytes to the control-plane staging endpoint for a
// relay id, host-token authorised, for relay to a Vault (or a fetch back for a
// restore).
func (a *agent) backupUpload(ctx context.Context, relayID string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, a.backupRelayURL(relayID, "upload"), body)
	if err != nil {
		return err
	}
	if size > 0 {
		req.ContentLength = size
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	a.authorise(req)
	resp, err := a.migHTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("upload returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

// backupDownload GETs a staged snapshot for a relay id into dst, host-token
// authorised, returning its sha256 so the caller can verify the recorded checksum.
func (a *agent) backupDownload(ctx context.Context, relayID string, dst io.Writer) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.backupRelayURL(relayID, "download"), nil)
	if err != nil {
		return "", err
	}
	a.authorise(req)
	resp, err := a.migHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("download returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(dst, hash), resp.Body); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// reportMigration POSTs a progress report to the migration-status endpoint. Best
// effort: a failure is logged and the step is retried on the next tick.
func (a *agent) reportMigration(ctx context.Context, report contract.MigrationReport) {
	body, err := json.Marshal(report)
	if err != nil {
		a.log.Warn("could not encode migration report", "err", err)
		return
	}
	url := fmt.Sprintf("%s/api/%s/hosts/%s/migration-status",
		strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		a.log.Warn("could not build migration report request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	a.authorise(req)
	resp, err := a.http.Do(req)
	if err != nil {
		a.log.Warn("could not post migration report", "err", err, "migrationId", report.MigrationID)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		a.log.Warn("migration report rejected", "status", resp.StatusCode, "body", strings.TrimSpace(string(b)))
	}
}

// phaseOr defaults an empty phase to "base" (v1 takes a single full snapshot).
func phaseOr(phase string) string {
	if phase == "" {
		return "base"
	}
	return phase
}

// firstLine trims an error to a single short line for a report, so no multi-line
// internal detail leaks into the control plane.
func firstLine(s string) string {
	s = strings.TrimSpace(strings.SplitN(s, "\n", 2)[0])
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}
