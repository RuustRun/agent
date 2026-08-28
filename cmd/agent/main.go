// Command agent is the Ruust host agent: a single static Go binary deployed to
// workload hosts. It implements the pull-based reconciliation loop from the
// build plan (phase 1).
//
// The agent needs no inbound ports. It polls the control plane on an interval
// with jitter, diffs desired state against the containers it owns on the host
// (those carrying the ruust.blob label prefix), converges (start missing, stop
// extra, restart unhealthy or crashed), and reports actual state back. If the
// control plane is unreachable the agent keeps the existing containers running
// and simply retries on the next tick: workloads keep running when the control
// plane is down. Every operation is idempotent and safely retryable.
//
// Configuration comes from the environment, with a token read from disk:
//
//	RUUST_CONTROL_PLANE_URL   base URL of the control plane (for example
//	                          https://cp.eu-west.ruust.internal)
//	RUUST_HOST_ID             this host's id
//	RUUST_HOST_TOKEN          bearer token (inline), or
//	RUUST_HOST_TOKEN_FILE     path to a file holding the token (default
//	                          /etc/ruust/host.token)
//	RUUST_REGION              region slug, for structured logging only
//	RUUST_POLL_INTERVAL       base poll interval (default 10s)
//	RUUST_POLL_JITTER         max extra random delay per tick (default 3s)
//
// British English throughout. No em dashes. The customer-facing unit is an Egg;
// "Blob" is internal only and never logged in a way a customer would read.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/RuustRun/agent/internal/cgroups"
	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
	"github.com/RuustRun/agent/internal/hostcap"
	"github.com/RuustRun/agent/internal/hostfacts"
	"github.com/RuustRun/agent/internal/ingress"
	"github.com/RuustRun/agent/internal/reconcile"
)

// agentVersion is the version string reported to the control plane. Set via
// -ldflags "-X main.agentVersion=..." at build time.
var agentVersion = "dev"

// config holds the resolved runtime configuration.
type config struct {
	controlPlaneURL string
	hostID          string
	hostToken       string
	region          string
	pollInterval    time.Duration
	pollJitter      time.Duration

	// Ingress: the local Caddy admin API, where the agent serves the on-demand
	// TLS ask endpoint, how Caddy reaches Egg containers, and the ask URL Caddy
	// calls (reachable from the Caddy container).
	caddyAdminURL  string
	ingressAskAddr string
	upstreamHost   string
	askEndpoint    string
	localTLS       bool
}

func main() {
	// Tee the agent's own logs to a bounded ring as well as stdout, so recent
	// agent output can be shipped with status reports and streamed in the console.
	logs := newLogRing(300)
	logger := slog.New(slog.NewJSONHandler(io.MultiWriter(os.Stdout, logs), &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// If we just self-updated, count this boot before doing anything that could
	// fail: a bad build that crashes during startup still increments the attempt
	// count, and after a few tries we roll back to the previous binary. Must be the
	// first thing in main so a crash in config or Docker setup is still counted.
	handleUpdateProbation(logger)

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	log := logger.With("hostId", cfg.hostID, "regionId", cfg.region, "agentVersion", agentVersion)
	log.Info("Ruust agent starting",
		"controlPlaneUrl", cfg.controlPlaneURL,
		"pollInterval", cfg.pollInterval.String(),
		"pollJitter", cfg.pollJitter.String(),
	)

	dcli, err := docker.NewEngineClient()
	if err != nil {
		// A missing Docker daemon is fatal at start, but the loop below tolerates
		// a control plane that is down.
		log.Error("cannot reach the Docker daemon", "err", err)
		os.Exit(1)
	}
	defer func() { _ = dcli.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := &agent{
		cfg:     cfg,
		docker:  dcli,
		http:    &http.Client{Timeout: 15 * time.Second},
		stats:   cgroups.NewReader(),
		log:     log,
		logs:    logs,
		ingress: ingress.New(cfg.caddyAdminURL, cfg.upstreamHost, cfg.askEndpoint, cfg.localTLS, log),
	}

	// Serve the on-demand TLS ask endpoint that Caddy calls before issuing a
	// certificate. It runs for the life of the agent; a failure to bind is logged
	// but not fatal (ingress just will not issue new certs until it is reachable).
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/ask", a.ingress.AskHandler())
		if err := http.ListenAndServe(cfg.ingressAskAddr, mux); err != nil {
			log.Warn("ingress ask endpoint stopped", "err", err)
		}
	}()

	a.run(ctx)
	log.Info("Ruust agent stopped")
}

// agent bundles the loop's dependencies.
type agent struct {
	cfg    config
	docker docker.Client
	http   *http.Client
	stats  *cgroups.Reader

	log     *slog.Logger
	logs    *logRing
	ingress *ingress.Reconciler

	// appliedVersion is the last desired-state version the agent successfully
	// converged to. It lets the agent no-op cheaply whilst the hash is unchanged.
	appliedVersion string

	// logSince tracks, per container ID, the timestamp of the newest log line
	// already shipped, so each report carries only new output (incremental). The
	// poll loop is single-goroutine, so this needs no lock.
	logSince map[string]string
}

// run drives the poll loop until the context is cancelled. Each tick fires after
// the base interval plus a random jitter, which spreads load across a fleet so
// hosts do not stampede the control plane in lockstep.
func (a *agent) run(ctx context.Context) {
	// We reached the run loop, so startup succeeded. If we are on probation after a
	// self-update, commit it once we have stayed up for the probation window, which
	// clears the rollback state and makes the new build permanent.
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(updateCommitWindow):
			commitUpdate(a.log)
		}
	}()

	// Check for a newer build shortly after starting, then periodically. This is
	// how the fleet updates itself after a deploy, with no operator action.
	a.maybeSelfUpdate(ctx)

	ticks := 0
	for {
		delay := a.cfg.pollInterval + jitter(a.cfg.pollJitter)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			a.safeTick(ctx)
			ticks++
			if ticks%updateCheckEveryTicks == 0 {
				a.maybeSelfUpdate(ctx)
			}
		}
	}
}

// updateCheckEveryTicks spaces out the self-update probe so it is cheap: at the
// default 10s poll that is roughly every five minutes.
const updateCheckEveryTicks = 30

// safeTick runs one tick and recovers from any panic, so a single bad cycle (a
// converge bug, an ingress hiccup, a malformed desired state) can never crash the
// agent. The loop keeps going, existing containers keep running, and the next tick
// tries again. This is the agent's core self-healing property; systemd is the
// backstop if the process ever does exit.
func (a *agent) safeTick(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("tick panicked; recovered and continuing",
				"panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
		}
	}()
	a.tick(ctx)
}

// maybeSelfUpdate checks whether the control plane is serving a newer agent build
// and, if so, downloads it, atomically replaces this binary, and exits so systemd
// restarts into the new version. Pull-based and best effort: any failure is logged
// and the agent keeps running the current build. The control plane advertises the
// current version in the X-Ruust-Agent-Version header on the download endpoint.
func (a *agent) maybeSelfUpdate(ctx context.Context) {
	url := fmt.Sprintf("%s/downloads/ruust-agent-linux-%s",
		strings.TrimRight(a.cfg.controlPlaneURL, "/"), runtime.GOARCH)

	head, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return
	}
	hres, err := a.http.Do(head)
	if err != nil {
		a.log.Warn("self-update check failed", "err", err)
		return
	}
	target := hres.Header.Get("X-Ruust-Agent-Version")
	_ = hres.Body.Close()
	// Nothing to do when the control plane advertises no version, the same version,
	// or a placeholder dev build (never auto-replace a hand-built dev binary).
	if target == "" || target == agentVersion || agentVersion == "dev" {
		return
	}

	self, err := selfPath()
	if err != nil {
		a.log.Warn("self-update: cannot resolve own path", "err", err)
		return
	}
	// Do not re-fetch a version that already failed and was rolled back on this node.
	// We wait for a different (presumably fixed) build to be served.
	if quarantinedVersions(self)[target] {
		a.log.Warn("self-update: target is quarantined after a previous failed update, skipping", "target", target)
		return
	}

	a.log.Info("newer agent build available, self-updating", "from", agentVersion, "to", target)

	get, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	res, err := a.http.Do(get)
	if err != nil {
		a.log.Warn("self-update download failed", "err", err)
		return
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		a.log.Warn("self-update download bad status", "status", res.StatusCode)
		return
	}

	// Integrity: fetch the published sha256 for this build and verify the download
	// against it before we ever chmod +x and swap it into place. Fail closed: no
	// verified checksum, no update, so a tampered or corrupt binary is never exec'd
	// as root. (Stronger provenance, a signature, is a follow-up on top of this.)
	expected, cerr := a.fetchChecksum(ctx, url+".sha256")
	if cerr != nil {
		a.log.Warn("self-update: could not verify checksum, skipping update", "err", cerr)
		return
	}

	// Write to a temp file in the SAME directory so the rename is atomic on one
	// filesystem, then swap it into place over the running binary (allowed on Linux
	// even while executing: the open handle keeps the old inode until exit).
	tmp, err := os.CreateTemp(filepath.Dir(self), ".ruust-agent-*")
	if err != nil {
		a.log.Warn("self-update: temp file failed", "err", err)
		return
	}
	tmpName := tmp.Name()
	// Hash the bytes as they are written, so we never re-read the file to verify.
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hash), res.Body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		a.log.Warn("self-update: write failed", "err", err)
		return
	}
	_ = tmp.Close()
	if got := hex.EncodeToString(hash.Sum(nil)); !strings.EqualFold(got, expected) {
		_ = os.Remove(tmpName)
		a.log.Warn("self-update: checksum mismatch, refusing to install", "expected", expected, "got", got)
		return
	}
	if err := os.Chmod(tmpName, 0o755); err != nil {
		_ = os.Remove(tmpName)
		a.log.Warn("self-update: chmod failed", "err", err)
		return
	}
	// Keep the current binary and write a probation marker BEFORE swapping, so if the
	// new build fails to stay up, handleUpdateProbation can roll back to this one.
	if err := copyFile(self, self+prevBinarySuffix); err != nil {
		_ = os.Remove(tmpName)
		a.log.Warn("self-update: could not keep previous binary, skipping for safety", "err", err)
		return
	}
	if b, merr := json.Marshal(updateState{FromVersion: agentVersion, ToVersion: target}); merr == nil {
		_ = os.WriteFile(self+updateStateSuffix, b, 0o600)
	}

	if err := os.Rename(tmpName, self); err != nil {
		_ = os.Remove(tmpName)
		_ = os.Remove(self + prevBinarySuffix)
		_ = os.Remove(self + updateStateSuffix)
		a.log.Warn("self-update: swap failed", "err", err)
		return
	}

	a.log.Info("agent updated, restarting into new build (on probation)", "version", target)
	os.Exit(0) // systemd (Restart=always) brings it straight back on the new binary.
}

// fetchChecksum GETs a published sha256 file for the agent binary and returns its
// hex digest. The body is a bare digest or "sha256sum" style ("<hex>  <name>").
func (a *agent) fetchChecksum(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	res, err := a.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("checksum status %d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, 4096))
	if err != nil {
		return "", err
	}
	return parseSha256(string(b))
}

// parseSha256 extracts a lower-case 64-hex-char sha256 digest from a checksum file
// body, rejecting anything that is not exactly one.
func parseSha256(s string) (string, error) {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return "", fmt.Errorf("empty checksum")
	}
	sum := strings.ToLower(fields[0])
	if len(sum) != 64 {
		return "", fmt.Errorf("unexpected checksum length %d", len(sum))
	}
	for _, c := range sum {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", fmt.Errorf("non-hex checksum")
		}
	}
	return sum, nil
}

// tick performs one poll-diff-converge-report cycle. It never returns an error:
// a control plane that is down, or a converge that partly fails, is logged and
// retried next tick, whilst existing containers keep running.
func (a *agent) tick(ctx context.Context) {
	desired, changed, err := a.fetchDesiredState(ctx)
	if err != nil {
		// Control plane unreachable or erroring: keep running, try again later.
		a.log.Warn("could not fetch desired state, keeping current containers running", "err", err)
		return
	}

	if changed {
		a.log.Info("desired state changed, converging", "version", desired.Version, "workloads", len(desired.Workloads))
	}

	// Fetch the out-of-band secrets and attach the decrypted env to each workload
	// before converging, so Create injects them. A secrets failure must not stop
	// reconciliation: log and proceed. Values are never logged.
	if secrets, serr := a.fetchSecrets(ctx); serr != nil {
		a.log.Warn("could not fetch secrets, proceeding without updated env", "err", serr)
	} else {
		for i := range desired.Workloads {
			if env, ok := secrets[desired.Workloads[i].ID]; ok {
				desired.Workloads[i].EnvValues = env
			}
		}
	}

	// Reconcile actual against desired on EVERY tick, not only when the desired
	// version changes. The version hash tells us when the control plane has moved
	// something, but a container can also drift on the host on its own: killed by
	// hand, crashed, or OOM killed. Converge diffs actual against desired and only
	// acts on drift, so an unchanged desired state with no drift is a cheap no-op
	// (a single container list), whilst a missing or unhealthy container is healed
	// here. This is what makes a hand-killed container come back.
	plan, cerr := reconcile.Converge(ctx, a.docker, desired)
	for _, step := range plan.Steps {
		// blobId is internal; it is safe in host-side structured logs but must
		// never be rendered to a customer.
		a.log.Info("converge step", "action", string(step.Action), "workloadId", step.WorkloadID, "blobId", step.BlobID)
	}
	if cerr != nil {
		// Partial failure: do not advance appliedVersion, so the next tick retries.
		a.log.Error("converge encountered errors, will retry next tick", "err", cerr)
	} else {
		a.appliedVersion = desired.Version
	}

	// Reconcile ingress: register a Caddy route for every running Egg that has a
	// published port and at least one hostname, and refresh the ask allow-list. A
	// failure here must not stop the workload loop; it is logged and retried.
	routes := make([]ingress.Route, 0, len(desired.Workloads))
	for _, w := range desired.Workloads {
		if w.PublishPort > 0 && len(w.Hostnames) > 0 {
			routes = append(routes, ingress.Route{Hostnames: w.Hostnames, UpstreamPort: w.PublishPort})
		}
	}
	if err := a.ingress.Reconcile(ctx, routes); err != nil {
		a.log.Warn("could not reconcile ingress", "err", err)
	}

	a.reportStatus(ctx, a.appliedVersion)
}

// fetchDesiredState performs GET /api/v1/hosts/:id/desired-state. It returns the
// desired state, whether the version differs from the last applied version, and
// any error. The changed flag short-circuits the expensive converge path.
func (a *agent) fetchDesiredState(ctx context.Context) (contract.DesiredState, bool, error) {
	url := fmt.Sprintf("%s/api/%s/hosts/%s/desired-state", strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return contract.DesiredState{}, false, err
	}
	a.authorise(req)

	resp, err := a.http.Do(req)
	if err != nil {
		return contract.DesiredState{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return contract.DesiredState{}, false, fmt.Errorf("desired-state returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var desired contract.DesiredState
	if err := json.NewDecoder(resp.Body).Decode(&desired); err != nil {
		return contract.DesiredState{}, false, fmt.Errorf("decoding desired state: %w", err)
	}
	if desired.Version == "" {
		return contract.DesiredState{}, false, fmt.Errorf("desired state has no version hash")
	}

	changed := desired.Version != a.appliedVersion
	return desired, changed, nil
}

// fetchSecrets performs GET /api/v1/hosts/:id/secrets and returns the decrypted
// env per workload as "KEY=VALUE" lines. This is the out-of-band half of secret
// delivery; the values are injected into containers and never logged.
func (a *agent) fetchSecrets(ctx context.Context) (map[string][]string, error) {
	url := fmt.Sprintf("%s/api/%s/hosts/%s/secrets", strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	a.authorise(req)

	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("secrets returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var sr contract.SecretsResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("decoding secrets: %w", err)
	}

	out := make(map[string][]string, len(sr.Workloads))
	for _, w := range sr.Workloads {
		lines := make([]string, 0, len(w.Env))
		for k, v := range w.Env {
			lines = append(lines, k+"="+v)
		}
		sort.Strings(lines) // deterministic order so Create is stable
		out[w.WorkloadID] = lines
	}
	return out, nil
}

// reportStatus performs POST /api/v1/hosts/:id/status with per-container health,
// restart counts and cgroup usage. A failure here is logged and swallowed: the
// control plane's dead-host timer will notice, and the next tick tries again.
func (a *agent) reportStatus(ctx context.Context, appliedVersion string) {
	containers, err := a.docker.List(ctx)
	if err != nil {
		a.log.Warn("could not list containers for status report", "err", err)
		return
	}

	if a.logSince == nil {
		a.logSince = make(map[string]string)
	}

	health := make([]contract.ContainerHealth, 0, len(containers))
	for _, c := range containers {
		// Usage comes from the Docker Engine API so it is real on macOS and Linux
		// alike. A stats hiccup for one container must not sink the whole report.
		usage, uerr := a.docker.Stats(ctx, c.ID)
		if uerr != nil {
			a.log.Warn("could not read container stats", "workloadId", c.WorkloadID, "err", uerr)
			usage = contract.CgroupUsage{}
		}

		// Ship only log lines newer than the last report. A log hiccup for one
		// container must not sink the whole report either.
		logs, lerr := a.docker.Logs(ctx, c.ID, a.logSince[c.ID])
		if lerr != nil {
			a.log.Warn("could not read container logs", "workloadId", c.WorkloadID, "err", lerr)
			logs = nil
		}
		if n := len(logs); n > 0 {
			a.logSince[c.ID] = logs[n-1].Ts // advance the cursor to the newest line
		}

		health = append(health, contract.ContainerHealth{
			WorkloadID:   c.WorkloadID,
			BlobID:       c.BlobID,
			State:        c.State,
			Healthy:      c.Healthy,
			RestartCount: c.RestartCount,
			Usage:        usage,
			Logs:         logs,
		})
	}

	// Detect the host's real capacity every heartbeat, so a resized VPS is picked
	// up on the next tick. A detection miss returns 0 and is omitted from the JSON,
	// so it never overwrites a known-good value on the control plane.
	cpuCores, totalRamMb := hostcap.Detect()

	// OS and patch facts for the operator fleet view. Cheap facts are read each
	// tick; the security-update check is cached inside hostfacts and refreshed only
	// every few hours.
	facts := hostfacts.Detect()

	status := contract.HostStatus{
		HostID:          a.cfg.hostID,
		AgentVersion:    agentVersion,
		AppliedVersion:  appliedVersion,
		CpuCores:        cpuCores,
		TotalRamMb:      totalRamMb,
		OSName:          facts.OSName,
		OSVersion:       facts.OSVersion,
		Kernel:          facts.Kernel,
		SecurityUpdates: facts.SecurityUpdates,
		RebootRequired:  facts.RebootRequired,
		RolledBack:      quarantinedList(),
		Containers:      health,
		AgentLogs:       a.logs.snapshot(),
	}

	body, err := json.Marshal(status)
	if err != nil {
		a.log.Error("could not marshal status report", "err", err)
		return
	}

	url := fmt.Sprintf("%s/api/%s/hosts/%s/status", strings.TrimRight(a.cfg.controlPlaneURL, "/"), contract.APIVersion, a.cfg.hostID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		a.log.Error("could not build status request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	a.authorise(req)

	resp, err := a.http.Do(req)
	if err != nil {
		a.log.Warn("could not post status, control plane may be down", "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		a.log.Warn("status report rejected", "status", resp.StatusCode)
	}
}

// authorise attaches the host bearer token. The token is never logged.
func (a *agent) authorise(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+a.cfg.hostToken)
	req.Header.Set("User-Agent", "ruust-agent/"+agentVersion)
}

// requireSecureURL rejects a control-plane URL that is not https, unless it points
// at localhost or an explicit opt-out (RUUST_ALLOW_INSECURE_CP=1) is set. The
// control plane is the agent's root of trust, so a remote http URL is refused.
func requireSecureURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("RUUST_CONTROL_PLANE_URL is not a valid URL: %w", err)
	}
	if u.Scheme == "https" {
		return nil
	}
	host := u.Hostname()
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || os.Getenv("RUUST_ALLOW_INSECURE_CP") == "1" {
		return nil
	}
	return fmt.Errorf("RUUST_CONTROL_PLANE_URL must be https (got scheme %q)", u.Scheme)
}

// loadConfig resolves configuration from the environment and reads the host
// token from disk when a file path is given.
func loadConfig() (config, error) {
	cfg := config{
		controlPlaneURL: os.Getenv("RUUST_CONTROL_PLANE_URL"),
		hostID:          os.Getenv("RUUST_HOST_ID"),
		region:          os.Getenv("RUUST_REGION"),
		pollInterval:    durationEnv("RUUST_POLL_INTERVAL", 10*time.Second),
		pollJitter:      durationEnv("RUUST_POLL_JITTER", 3*time.Second),
		caddyAdminURL:   envOr("RUUST_CADDY_ADMIN", "http://localhost:2019"),
		ingressAskAddr:  envOr("RUUST_INGRESS_ASK_ADDR", ":9700"),
		upstreamHost:    envOr("RUUST_UPSTREAM_HOST", "host.docker.internal"),
		askEndpoint:     envOr("RUUST_ASK_ENDPOINT", "http://host.docker.internal:9700/ask"),
		// Production uses Let's Encrypt; set RUUST_INGRESS_LOCAL_TLS=1 for a local
		// single-box setup that must fall back to Caddy's internal self-signed CA.
		localTLS: os.Getenv("RUUST_INGRESS_LOCAL_TLS") == "1",
	}

	if cfg.controlPlaneURL == "" {
		return config{}, fmt.Errorf("RUUST_CONTROL_PLANE_URL is required")
	}
	// The control plane is the root of trust for self-update and everything else
	// the agent fetches, so require https. Allow http only for a localhost dev
	// control plane, or an explicit opt-out, never for a remote host.
	if err := requireSecureURL(cfg.controlPlaneURL); err != nil {
		return config{}, err
	}
	if cfg.hostID == "" {
		return config{}, fmt.Errorf("RUUST_HOST_ID is required")
	}

	token, err := loadToken()
	if err != nil {
		return config{}, err
	}
	cfg.hostToken = token
	return cfg, nil
}

// loadToken reads the host token from RUUST_HOST_TOKEN (inline) or from the file
// at RUUST_HOST_TOKEN_FILE (default /etc/ruust/host.token).
func loadToken() (string, error) {
	if inline := os.Getenv("RUUST_HOST_TOKEN"); inline != "" {
		return strings.TrimSpace(inline), nil
	}
	path := os.Getenv("RUUST_HOST_TOKEN_FILE")
	if path == "" {
		path = "/etc/ruust/host.token"
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading host token from %s: %w", path, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("host token file %s is empty", path)
	}
	return token, nil
}

// envOr returns the environment value for key, or fallback when it is unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// durationEnv parses a duration from the environment, falling back to a default.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// jitter returns a uniformly random duration in [0, max), using crypto/rand so
// a whole fleet does not share a seeded PRNG and re-synchronise. A zero or
// negative max yields zero.
func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}
