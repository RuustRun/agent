// Package dbimport runs one-off data imports into a database Egg's own container:
// pg_dump of an external Postgres source piped into the container's local psql, via
// docker exec. The data (and the source credential) never leave the host: the dump
// streams straight into the target inside its container.
//
// Imports run ASYNCHRONOUSLY (a dump and restore can take minutes and must not block
// the reconcile tick). Ensure kicks off a detached import the first time it sees an
// import id, then returns incremental progress on every subsequent tick, which the
// agent ships to the control plane on the status endpoint.
//
// British English throughout. No em dashes. The source URL and its password are
// redacted from the import log before it leaves the host.
package dbimport

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/RuustRun/agent/internal/contract"
)

// Importer tracks in-flight imports, one per import id.
type Importer struct {
	mu   sync.Mutex
	jobs map[string]*job
}

type job struct {
	mu       sync.Mutex
	status   string // "importing", "done", "failed"
	log      strings.Builder
	sent     int  // bytes of log already handed back to the caller
	reported bool // whether the terminal (done/failed) report has been returned once
}

// New returns an empty Importer.
func New() *Importer { return &Importer{jobs: map[string]*job{}} }

// Ensure starts the import for d.ImportID against the given running container the
// first time it is seen, then returns incremental progress on every call. Idempotent
// and cheap: once a job exists it just returns the log delta and current status.
// sourceURL is the external Postgres connection string; it is redacted from the log
// and passed to docker exec via the environment, never as a command argument.
func (im *Importer) Ensure(ctx context.Context, containerID string, d *contract.ImportDirective, sourceURL string) (done bool, report *contract.ImportReport) {
	im.mu.Lock()
	j, ok := im.jobs[d.ImportID]
	if !ok {
		j = &job{status: "importing"}
		im.jobs[d.ImportID] = j
		// Detached context: the import must outlive the tick that started it.
		go im.run(context.Background(), containerID, sourceURL, j)
	}
	im.mu.Unlock()

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
			return true, &contract.ImportReport{ImportID: d.ImportID, Status: "done", Log: delta}
		}
		return true, nil
	case "failed":
		if !j.reported {
			j.reported = true
			return true, &contract.ImportReport{ImportID: d.ImportID, Status: "failed", Log: delta}
		}
		// Keep asserting failed (no new log) so the control plane stays failed.
		return true, &contract.ImportReport{ImportID: d.ImportID, Status: "failed"}
	default: // importing
		return false, &contract.ImportReport{ImportID: d.ImportID, Status: "importing", Log: delta}
	}
}

// run performs one import to completion in its own goroutine, streaming redacted
// output into the job's log and setting the terminal status.
func (im *Importer) run(ctx context.Context, containerID, sourceURL string, j *job) {
	redact := redactor(sourceURL)
	appendLog := func(s string) {
		j.mu.Lock()
		j.log.WriteString(redact(s))
		j.mu.Unlock()
	}
	fail := func(msg string) {
		appendLog("[error] " + msg + "\n")
		j.mu.Lock()
		j.status = "failed"
		j.mu.Unlock()
	}

	appendLog("[import] dumping the source database and loading it into your Egg\n")

	// pg_dump the external source (its credentials are in the URL, supplied via the
	// environment so they never reach the process arguments) piped into the target
	// Egg's own local psql. Runs INSIDE the target container, which already has the
	// Postgres client, local database access and outbound network. --clean makes a
	// re-import replace rather than duplicate; --no-owner/--no-privileges avoid role
	// mismatches against the Egg's single 'ruust' role.
	script := `set -o pipefail; ` +
		`pg_dump --no-owner --no-privileges --clean --if-exists "$RUUST_IMPORT_SRC" ` +
		`| PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"`

	// -e with no value forwards RUUST_IMPORT_SRC from this process's environment into
	// the container, so the source URL is never a docker-exec argument (it would show
	// in the host process list). We set it on cmd.Env below.
	cmd := exec.CommandContext(ctx, "docker", "exec", "-u", "postgres", "-e", "RUUST_IMPORT_SRC", containerID, "sh", "-c", script)
	cmd.Env = append(os.Environ(), "RUUST_IMPORT_SRC="+sourceURL)

	if err := runStreamed(cmd, appendLog); err != nil {
		fail("import failed: " + err.Error())
		return
	}
	appendLog("[import] done\n")
	j.mu.Lock()
	j.status = "done"
	j.mu.Unlock()
}

// runStreamed runs cmd, streaming combined stdout+stderr line by line through
// appendLog. Returns the process error (nil on exit 0).
func runStreamed(cmd *exec.Cmd, appendLog func(string)) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	stream := func(r io.Reader) {
		defer wg.Done()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 1024*1024), 8*1024*1024)
		for sc.Scan() {
			appendLog(sc.Text() + "\n")
		}
	}
	wg.Add(2)
	go stream(stdout)
	go stream(stderr)
	wg.Wait()
	return cmd.Wait()
}

// redactor returns a function that strips the source URL and its password from
// output, so the credential never reaches the control plane or the dashboard.
func redactor(sourceURL string) func(string) string {
	secrets := make([]string, 0, 2)
	if len(sourceURL) >= 8 {
		secrets = append(secrets, sourceURL)
	}
	if u, err := url.Parse(sourceURL); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && len(pw) >= 4 {
			secrets = append(secrets, pw)
		}
	}
	return func(s string) string {
		for _, v := range secrets {
			s = strings.ReplaceAll(s, v, "[redacted]")
		}
		return s
	}
}
