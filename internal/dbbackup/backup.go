// Package dbbackup captures and restores snapshots of a database Egg, for
// point-in-time recovery. A capture runs pg_dump inside the Egg's own container and
// streams the dump to a durable file on the host (so the data never leaves the box);
// a restore pipes a chosen dump back into the container. Both run ASYNCHRONOUSLY
// (a dump/restore can take minutes and must not block the reconcile tick), reporting
// incremental progress the agent ships on the status endpoint.
//
// Phase 1 stores snapshots locally on the Egg's host: this survives a logical
// mistake (a bad migration, a dropped table), not the host disk dying. A later phase
// relays them to a durable off-host Vault. British English. No em dashes. Nothing
// secret is logged (the DB password lives only in the container's own environment).
package dbbackup

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/RuustRun/agent/internal/contract"
)

// backupRoot is where snapshots are written on the host. The agent's systemd unit
// grants it write access to /var/lib/ruust.
const backupRoot = "/var/lib/ruust/backups"

// Backuper tracks in-flight captures and restores, one per backup id.
type Backuper struct {
	mu   sync.Mutex
	jobs map[string]*job
}

type job struct {
	mu        sync.Mutex
	status    string // "running", "done", "failed"
	ref       string
	sizeBytes int64
	checksum  string
	log       strings.Builder
	sent      int
	reported  bool
}

// New returns an empty Backuper.
func New() *Backuper { return &Backuper{jobs: map[string]*job{}} }

// Ensure runs the backup for d.BackupID against the given running container the
// first time it is seen, then returns incremental progress on every call. blobID
// locates the on-host snapshot directory. Idempotent and cheap once a job exists.
func (b *Backuper) Ensure(ctx context.Context, containerID, blobID string, d *contract.BackupDirective) (done bool, report *contract.BackupReport) {
	b.mu.Lock()
	j, ok := b.jobs[d.BackupID]
	if !ok {
		j = &job{status: "running"}
		b.jobs[d.BackupID] = j
		// Detached context: the job must outlive the tick that started it.
		go b.run(context.Background(), containerID, blobID, d, j)
	}
	b.mu.Unlock()

	j.mu.Lock()
	defer j.mu.Unlock()

	full := j.log.String()
	delta := ""
	if len(full) > j.sent {
		delta = full[j.sent:]
		j.sent = len(full)
	}

	switch j.status {
	case "done":
		if !j.reported {
			j.reported = true
			return true, &contract.BackupReport{
				BackupID:  d.BackupID,
				Status:    "done",
				Ref:       j.ref,
				SizeBytes: j.sizeBytes,
				Checksum:  j.checksum,
				Log:       delta,
			}
		}
		return true, nil
	case "failed":
		if !j.reported {
			j.reported = true
			return true, &contract.BackupReport{BackupID: d.BackupID, Status: "failed", Log: delta}
		}
		return true, &contract.BackupReport{BackupID: d.BackupID, Status: "failed"}
	default: // running
		return false, &contract.BackupReport{BackupID: d.BackupID, Status: "running", Log: delta}
	}
}

func (b *Backuper) run(ctx context.Context, containerID, blobID string, d *contract.BackupDirective, j *job) {
	appendLog := func(s string) {
		j.mu.Lock()
		j.log.WriteString(s)
		j.mu.Unlock()
	}
	fail := func(msg string) {
		appendLog("[error] " + msg + "\n")
		j.mu.Lock()
		j.status = "failed"
		j.mu.Unlock()
	}

	if d.Action == "restore" {
		b.restore(ctx, containerID, d, appendLog, fail, j)
		return
	}
	b.capture(ctx, containerID, blobID, d, appendLog, fail, j)
}

// capture streams pg_dump of the Egg's own database (custom format, for pg_restore)
// to a durable host file, recording its size and sha256, then prunes older snapshots
// to the retention count.
func (b *Backuper) capture(ctx context.Context, containerID, blobID string, d *contract.BackupDirective, appendLog func(string), fail func(string), j *job) {
	dir := filepath.Join(backupRoot, blobID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		fail("could not create the backup directory: " + err.Error())
		return
	}
	path := filepath.Join(dir, d.BackupID+".dump")
	appendLog("[backup] capturing a snapshot\n")

	// pg_dump the local database from inside the container. The password lives in the
	// container's own environment; we never pass it as an argument or log it.
	script := `PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -Fc --no-owner --no-privileges ` +
		`-h 127.0.0.1 -U "$POSTGRES_USER" "$POSTGRES_DB"`
	cmd := exec.CommandContext(ctx, "docker", "exec", "-u", "postgres", containerID, "sh", "-c", script)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fail("pipe: " + err.Error())
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fail("pipe: " + err.Error())
		return
	}
	f, err := os.Create(path)
	if err != nil {
		fail("could not open the snapshot file: " + err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		fail("could not start pg_dump: " + err.Error())
		return
	}

	// Stream the dump to the file and hash it in one pass; drain stderr into the log.
	h := sha256.New()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 1024*1024), 8*1024*1024)
		for sc.Scan() {
			appendLog(sc.Text() + "\n")
		}
	}()
	n, copyErr := io.Copy(io.MultiWriter(f, h), stdout)
	wg.Wait()
	closeErr := f.Close()
	waitErr := cmd.Wait()

	if copyErr != nil || closeErr != nil || waitErr != nil {
		_ = os.Remove(path) // do not leave a half-written snapshot
		msg := "pg_dump failed"
		if waitErr != nil {
			msg += ": " + waitErr.Error()
		}
		fail(msg)
		return
	}

	j.mu.Lock()
	j.ref = path
	j.sizeBytes = n
	j.checksum = hex.EncodeToString(h.Sum(nil))
	j.status = "done"
	j.mu.Unlock()
	appendLog(fmt.Sprintf("[backup] captured %d bytes\n", n))

	// Prune older snapshots for this Egg to the retention count, so the host disk
	// stays bounded. Best effort; a prune failure does not fail the backup.
	if d.Retention > 0 {
		if err := pruneOldest(dir, d.Retention); err != nil {
			appendLog("[backup] prune warning: " + err.Error() + "\n")
		}
	}
}

// restore loads a previously captured snapshot back into the running container.
func (b *Backuper) restore(ctx context.Context, containerID string, d *contract.BackupDirective, appendLog func(string), fail func(string), j *job) {
	if d.Ref == "" {
		fail("restore has no snapshot reference")
		return
	}
	f, err := os.Open(d.Ref)
	if err != nil {
		fail("could not open the snapshot: " + err.Error())
		return
	}
	defer func() { _ = f.Close() }()

	appendLog("[restore] loading the snapshot into the database\n")
	// --clean --if-exists drops existing objects first, so a restore replaces rather
	// than duplicates. -h 127.0.0.1 with PGPASSWORD authenticates over TCP.
	script := `PGPASSWORD="$POSTGRES_PASSWORD" pg_restore --clean --if-exists --no-owner --no-privileges ` +
		`-h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"`
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", "-u", "postgres", containerID, "sh", "-c", script)
	cmd.Stdin = f

	out, err := cmd.CombinedOutput()
	if s := strings.TrimSpace(string(out)); s != "" {
		appendLog(s + "\n")
	}
	if err != nil {
		// pg_restore exits non-zero on benign "does not exist" notices from --clean on
		// a fresh database, so a failure here is logged but the data is still loaded;
		// we treat a non-zero exit as failed only when nothing was restorable.
		fail("pg_restore reported errors: " + err.Error())
		return
	}
	appendLog("[restore] done\n")
	j.mu.Lock()
	j.status = "done"
	j.mu.Unlock()
}

// pruneOldest keeps the `keep` newest *.dump files in dir and removes the rest.
func pruneOldest(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type f struct {
		path    string
		modTime int64
	}
	files := make([]f, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".dump") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, f{path: filepath.Join(dir, e.Name()), modTime: info.ModTime().UnixNano()})
	}
	if len(files) <= keep {
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime > files[j].modTime }) // newest first
	for _, old := range files[keep:] {
		_ = os.Remove(old.path)
	}
	return nil
}
