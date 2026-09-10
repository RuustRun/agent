// Package build builds host-placement Egg images locally, on the workload host
// itself (bring your own host), so the customer's source code and build never
// leave their machine and nothing is pushed to any registry.
//
// Builds run ASYNCHRONOUSLY: a repo build takes minutes and must not block the
// reconcile tick. Ensure kicks off a detached build the first time it sees a tag
// it cannot find locally, then returns incremental progress on every subsequent
// tick, which the agent ships to the control plane on the status endpoint.
//
// British English throughout. No em dashes. The clone token and env values are
// redacted from the build log before it leaves the host.
package build

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/RuustRun/agent/internal/contract"
)

// nixpacksVersion pins the buildpack version, matching the build-host bootstrap.
// Bumped 1.29.1 -> 1.39.0: 1.29.1 bundled a June-2024 nixpkgs whose Node patches
// were below modern tool floors (its nodejs_22 is 22.3.0; Prisma needs 20.19+/22.12+),
// so a standard Prisma/Next app failed to build even with the right Node major. 1.39.0
// bundles a current nixpkgs. Keep in step with infra/provisioning build-host bootstrap.
const nixpacksVersion = "1.39.0"

// pinnedNixpkgsArchive is a nixpkgs commit that ships a current Node across the majors
// we build (nodejs_20 = 20.20.2, nodejs_22 >= 22.12). Even 1.39.0's bundled snapshot
// lags: it ships nodejs_22 = 22.11.0, one patch below Prisma's 22.12 floor, and its
// Node 20 is below the 20.19 floor too, so no major it bundles clears Prisma. Pinning
// the archive ourselves stops the build inheriting whatever the buildpack happens to
// bundle, so a repo builds against a known-good Node without the customer writing any
// config. Verify a bump with `nixhub.io` (nodejs_20 >= 20.19 and nodejs_22 >= 22.12).
const pinnedNixpkgsArchive = "389ed85304b281ca7f306cf8a1eb4378651ca44e"

// buildTimeout caps how long a single build may run before it is stopped, so a wedged
// build (a hung "exporting layers", a runaway install) can never tie up the host's
// build slot and resources indefinitely. Generous by default so a legitimate slow
// build (a cold nix cache on a small box) is never killed; override with
// RUUST_BUILD_TIMEOUT (a Go duration like "45m"). When it fires, the build context is
// cancelled, which SIGTERMs the build process group like a manual cancel.
var buildTimeout = func() time.Duration {
	if v := os.Getenv("RUUST_BUILD_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Minute
}()

// Builder tracks in-flight local builds, one per image tag.
type Builder struct {
	mu   sync.Mutex
	jobs map[string]*job
	// cancels holds the cancel func for each in-flight build, keyed by ImageTag, so a
	// running build can be stopped (its context is cancelled, which SIGTERMs the build
	// process group). tagByDeploy maps a DeploymentID to its ImageTag, because the
	// control plane cancels by deployment but jobs are keyed by tag.
	cancels     map[string]context.CancelFunc
	tagByDeploy map[string]string
	// sem caps concurrent builds to one per host: a build acquires it before doing any
	// work and releases it when done, so rapid redeploys queue instead of thrashing the
	// box (several parallel nixpacks/docker builds can exhaust its CPU and disk).
	sem chan struct{}
	// Registry hosts this process has already logged in to (dedicated build host,
	// pushing built images). Keyed by host so a re-login is skipped per build.
	loggedIn map[string]bool
}

type job struct {
	mu       sync.Mutex
	status   string // "building", "built", "failed"
	log      strings.Builder
	sent     int  // bytes of log already handed back to the caller
	reported bool // whether the terminal (built/failed) report has been returned once
	canceled bool // set when the build was cancelled, to soften the failure log
}

// New returns an empty Builder.
func New() *Builder {
	return &Builder{
		jobs:        map[string]*job{},
		cancels:     map[string]context.CancelFunc{},
		tagByDeploy: map[string]string{},
		sem:         make(chan struct{}, 1), // one build at a time per host
		loggedIn:    map[string]bool{},
	}
}

// Ensure makes sure the directive's ImageTag exists locally, building it in the
// background when it does not. It returns whether the image is ready to run and,
// whilst a build is in progress or has just finished, an incremental BuildReport
// for the agent to ship on the status endpoint. Idempotent and cheap to call every
// tick: the fast path is a single image inspect.
//
// envValues are "KEY=VALUE" build-time variables (from the secrets endpoint) and
// cloneToken is a short-lived git token for a private repo (empty for public).
// Both are redacted from the returned log.
func (b *Builder) Ensure(ctx context.Context, d *contract.BuildDirective, envValues []string, cloneToken string) (ready bool, report *contract.BuildReport) {
	b.mu.Lock()
	j, ok := b.jobs[d.ImageTag]
	if !ok {
		// No in-flight build for this tag. If the image already exists (a previous run,
		// or a prior process before a restart) there is nothing to build or report.
		// This check MUST be inside the no-job branch: up front it would swallow a
		// just-finished build's terminal report, because the build creates the image
		// partway through "exporting layers", so the next tick would short-circuit here
		// before ever reporting 'built' and the deploy would hang on "building".
		if imageExists(ctx, d.ImageTag) {
			b.mu.Unlock()
			return true, nil
		}
		j = &job{status: "building"}
		b.jobs[d.ImageTag] = j
		// Detached, cancellable, time-bounded context: the build must outlive the tick
		// that started it, but stay stoppable (Cancel closes buildCtx, which SIGTERMs
		// the build) and self-stop if it runs past buildTimeout so a wedged build cannot
		// hog the host forever.
		buildCtx, cancel := context.WithTimeout(context.Background(), buildTimeout)
		b.cancels[d.ImageTag] = cancel
		if d.DeploymentID != "" {
			b.tagByDeploy[d.DeploymentID] = d.ImageTag
		}
		go b.run(buildCtx, d, envValues, cloneToken, j)
	}
	b.mu.Unlock()

	j.mu.Lock()
	defer j.mu.Unlock()

	// Incremental log since the last read.
	full := j.log.String()
	delta := ""
	if len(full) > j.sent {
		delta = full[j.sent:]
		j.sent = len(full)
	}

	switch j.status {
	case "built":
		if !j.reported {
			j.reported = true
			b.forget(d.ImageTag, d.DeploymentID)
		}
		// Re-assert 'built' on EVERY tick, not just once. A single terminal report can
		// be lost (a failed status POST), and because the image now exists nothing
		// would retry it, hanging the deploy on "building". Re-reporting is idempotent
		// on the control plane (it flips the deployment live once, then no-ops); the log
		// delta is only non-empty on the first report.
		return true, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "built", Log: delta}
	case "failed":
		if !j.reported {
			j.reported = true
			b.forget(d.ImageTag, d.DeploymentID)
		}
		// Keep asserting failed each tick so the control plane converges to failed even
		// if a report is lost, until a redeploy mints a new tag and a fresh job.
		return false, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "failed", Log: delta}
	default: // building
		return false, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "building", Log: delta}
	}
}

// forget drops the cancel handle and deployment mapping for a finished build, so the
// maps do not grow without bound. The job itself is kept (a failed job keeps asserting
// failed until a new tag supersedes it). Safe to call holding a job lock: it only takes
// the builder lock, and Cancel never holds the builder lock whilst taking a job lock.
func (b *Builder) forget(tag, deploymentID string) {
	b.mu.Lock()
	delete(b.cancels, tag)
	if deploymentID != "" {
		delete(b.tagByDeploy, deploymentID)
	}
	b.mu.Unlock()
}

// Cancel stops the in-flight build for a deployment, if one is running. It closes the
// build's context (which SIGTERMs the build process group) and marks the job cancelled
// so its failure is logged softly rather than as an error. A no-op for an unknown or
// already-finished deployment. Non-blocking and safe to call every tick.
func (b *Builder) Cancel(deploymentID string) {
	b.mu.Lock()
	tag := b.tagByDeploy[deploymentID]
	cancel := b.cancels[tag]
	j := b.jobs[tag]
	b.mu.Unlock() // release before taking a job lock, so lock order never inverts.

	if j != nil {
		j.mu.Lock()
		alreadyDone := j.status == "built" || j.status == "failed"
		j.canceled = true
		j.mu.Unlock()
		if alreadyDone {
			return
		}
	}
	if cancel != nil {
		cancel()
	}
}

// run performs one build to completion in its own goroutine, streaming redacted
// output into the job's log and setting the terminal status.
func (b *Builder) run(ctx context.Context, d *contract.BuildDirective, envValues []string, cloneToken string, j *job) {
	// A build host reads its registry push credential from the environment (set by
	// provisioning, never in the image). Redact it from the log as a backstop, even
	// though docker login reads it on stdin and never echoes it.
	_, _, regPass := registryCreds(d.PushTo)
	redact := redactor(cloneToken, append(envValues, "PUSH="+regPass))
	appendLog := func(s string) {
		j.mu.Lock()
		j.log.WriteString(redact(s))
		j.mu.Unlock()
	}
	fail := func(msg string) {
		j.mu.Lock()
		canceled := j.canceled
		j.status = "failed"
		j.mu.Unlock()
		switch {
		case ctx.Err() == context.DeadlineExceeded:
			// The build ran past its ceiling and was stopped: a real failure (a hung
			// step), reported as such so the deploy fails rather than hanging.
			appendLog(fmt.Sprintf("[error] build exceeded the %s timeout and was stopped\n", buildTimeout))
		case canceled || ctx.Err() != nil:
			// A cancelled build failing is expected (we stopped it), so log it softly
			// rather than as an [error] that would read as a real build failure.
			appendLog("[cancel] build stopped\n")
		default:
			appendLog("[error] " + msg + "\n")
		}
	}

	// Cap concurrent builds to one per host: acquire the slot before doing any work,
	// so rapid redeploys queue rather than thrash the box. If this build is cancelled
	// whilst it waits in the queue, bail out here without ever starting.
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
		// Reclaim disk once the build is done, whilst we still hold the (only) build
		// slot so no build runs during the prune. Over many deploys the build cache and
		// old Egg images fill a BYO host's disk, and a full docker filesystem wedges
		// buildkit at "exporting layers". Runs first (LIFO) then releases the slot.
		defer pruneBuildDisk(appendLog)
	case <-ctx.Done():
		fail("build cancelled before it started")
		return
	}
	if ctx.Err() != nil {
		fail("build cancelled before it started")
		return
	}

	dir, err := os.MkdirTemp("", "ruust-hostbuild-")
	if err != nil {
		fail("could not create build directory: " + err.Error())
		return
	}
	defer os.RemoveAll(dir)

	// docker and nixpacks write config and caches under $HOME. Point them at a
	// writable directory in case the agent's own home is restricted.
	home, dockerCfg := writableHome()
	env := append(os.Environ(), "HOME="+home, "DOCKER_CONFIG="+dockerCfg)

	cloneURL := d.RepoURL
	if cloneToken != "" {
		cloneURL = authedCloneURL(d.RepoURL, cloneToken)
	}
	appendLog(fmt.Sprintf("[build] cloning %s (branch %s)\n", d.RepoURL, d.Branch))
	if err := runCmd(ctx, dir, env, appendLog, "git", "clone", "--depth", "1", "--branch", d.Branch, cloneURL, dir); err != nil {
		fail("git clone failed: " + err.Error())
		return
	}

	buildDir := dir
	if d.RootDirectory != "" {
		buildDir = filepath.Join(dir, d.RootDirectory)
		appendLog("[build] root directory: " + d.RootDirectory + "\n")
	}

	if len(envValues) > 0 {
		appendLog(fmt.Sprintf("[build] passing %d build-time env var(s)\n", len(envValues)))
	}

	// A repo Dockerfile is built directly; otherwise Nixpacks auto-detects the stack.
	if _, err := os.Stat(filepath.Join(buildDir, "Dockerfile")); err == nil {
		args := []string{"build"}
		for _, kv := range envValues {
			args = append(args, "--build-arg", kv)
		}
		args = append(args, "-t", d.ImageTag, ".")
		appendLog("[build] building image with docker\n")
		if err := runCmd(ctx, buildDir, env, appendLog, "docker", args...); err != nil {
			fail("docker build failed: " + err.Error())
			return
		}
	} else {
		nixpacks, nerr := ensureNixpacks(ctx, appendLog)
		if nerr != nil {
			fail("no Dockerfile found and nixpacks is unavailable: " + nerr.Error())
			return
		}
		// Pin the nixpkgs archive so the build gets a current Node, not whatever
		// (possibly too old) snapshot the buildpack happens to bundle.
		ensureNixpkgsPin(buildDir, appendLog)
		args := []string{"build", "."}
		for _, kv := range envValues {
			args = append(args, "--env", kv)
		}
		if d.StartCommand != "" {
			args = append(args, "--start-cmd", d.StartCommand)
		}
		args = append(args, "--name", d.ImageTag)
		appendLog("[build] building image with nixpacks\n")
		if err := runCmd(ctx, buildDir, env, appendLog, nixpacks, args...); err != nil {
			fail("nixpacks build failed: " + err.Error())
			return
		}
	}

	appendLog(fmt.Sprintf("[build] built %s\n", d.ImageTag))

	// Dedicated build host: push the built image to the private registry so a
	// workload host can pull it. The build succeeds only once the push does, so the
	// control plane never flips a deployment live against an image that is not in the
	// registry. Bring-your-own-host has no PushTo and runs the local tag instead.
	if d.PushTo != "" {
		if err := b.pushImage(ctx, d, env, appendLog); err != nil {
			fail("registry push failed: " + err.Error())
			return
		}
	}

	j.mu.Lock()
	j.status = "built"
	j.mu.Unlock()
}

// pushImage tags the freshly built image to the registry reference and pushes it,
// logging in to the registry first (once per host). The registry credential comes
// from the environment and is fed to docker login on stdin, so it never appears in
// the process args or the build log.
func (b *Builder) pushImage(ctx context.Context, d *contract.BuildDirective, env []string, appendLog func(string)) error {
	host, user, pass := registryCreds(d.PushTo)
	if host == "" || user == "" || pass == "" {
		return fmt.Errorf("registry credentials are not configured on this build host")
	}
	if err := b.ensureRegistryLogin(ctx, env, host, user, pass, appendLog); err != nil {
		return err
	}
	if err := runCmd(ctx, "", env, appendLog, "docker", "tag", d.ImageTag, d.PushTo); err != nil {
		return fmt.Errorf("docker tag: %w", err)
	}
	appendLog(fmt.Sprintf("[deploy] pushing %s\n", d.PushTo))
	if err := runCmd(ctx, "", env, appendLog, "docker", "push", d.PushTo); err != nil {
		return fmt.Errorf("docker push: %w", err)
	}
	appendLog(fmt.Sprintf("[deploy] pushed %s\n", d.PushTo))
	return nil
}

// ensureRegistryLogin logs in to the registry host once per process, feeding the
// password on stdin (never an arg, never logged). Uses the build env so the auth is
// written to the same DOCKER_CONFIG the subsequent push reads.
func (b *Builder) ensureRegistryLogin(ctx context.Context, env []string, host, user, pass string, appendLog func(string)) error {
	b.mu.Lock()
	already := b.loggedIn[host]
	b.mu.Unlock()
	if already {
		return nil
	}

	appendLog("[deploy] authenticating to the registry\n")
	cmd := exec.CommandContext(ctx, "docker", "login", host, "-u", user, "--password-stdin")
	cmd.Env = env
	cmd.Stdin = strings.NewReader(pass)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// docker login does not echo the password, so its output is safe to surface.
		return fmt.Errorf("docker login: %w: %s", err, strings.TrimSpace(string(out)))
	}

	b.mu.Lock()
	b.loggedIn[host] = true
	b.mu.Unlock()
	return nil
}

// registryCreds returns the push registry host, user and password for a build host.
// The host is derived from the push reference (everything before the first slash),
// with RUUST_REGISTRY as a fallback; the credentials come from the environment.
func registryCreds(pushTo string) (host, user, pass string) {
	host = strings.TrimRight(os.Getenv("RUUST_REGISTRY"), "/")
	if i := strings.IndexByte(pushTo, '/'); i > 0 {
		host = pushTo[:i]
	}
	return host, os.Getenv("RUUST_REGISTRY_PUSH_USER"), os.Getenv("RUUST_REGISTRY_PUSH_PASS")
}

// imageExists returns true when the tag is present in the local Docker image store.
func imageExists(ctx context.Context, tag string) bool {
	return exec.CommandContext(ctx, "docker", "image", "inspect", tag).Run() == nil
}

// buildCacheKeep bounds how much recent build cache to retain (fast rebuilds), whilst
// stopping it from growing without limit. Overridable for small BYO disks.
var buildCacheKeep = func() string {
	if v := os.Getenv("RUUST_BUILD_CACHE_KEEP"); v != "" {
		return v
	}
	return "8GB"
}()

// pruneBuildDisk reclaims Docker disk after a build, so a BYO host does not fill up over
// many deploys and wedge buildkit at "exporting layers". Best effort on a fresh, short
// context (so a cancelled build still tidies up), and safe: prune never touches an image
// that currently backs a container, so the running Egg (and, during a zero-downtime roll,
// the old replica still serving) is always kept. It removes dangling layers, unused
// images older than a day (old Egg builds pile up tagged, one per redeploy), and build
// cache beyond a recent budget. Silent: this is host hygiene, not build output.
func pruneBuildDisk(appendLog func(string)) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	home, dockerCfg := writableHome()
	env := append(os.Environ(), "HOME="+home, "DOCKER_CONFIG="+dockerCfg)
	quiet := func(string) {}

	_ = runCmd(ctx, "", env, quiet, "docker", "image", "prune", "-f")
	// Unused images older than 24h: reclaims superseded Egg builds without touching the
	// just-built image (far newer) or anything backing a live container.
	_ = runCmd(ctx, "", env, quiet, "docker", "image", "prune", "-a", "-f", "--filter", "until=24h")
	// Bound the build cache. --keep-storage is not on every Docker version, so fall back
	// to a full cache prune if it is rejected.
	if err := runCmd(ctx, "", env, quiet, "docker", "builder", "prune", "-f", "--keep-storage="+buildCacheKeep); err != nil {
		_ = runCmd(ctx, "", env, quiet, "docker", "builder", "prune", "-f")
	}
	appendLog("[cleanup] reclaimed unused Docker images and build cache\n")
}

// runCmd runs a command in dir with env, streaming combined stdout+stderr line by
// line through appendLog. Returns the process error (nil on exit 0).
func runCmd(ctx context.Context, dir string, env []string, appendLog func(string), name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	// Put the build in its own process group and, when the context is cancelled (a
	// build cancel), SIGTERM the whole group so nixpacks/docker and their children
	// stop, not just the immediate child. WaitDelay then SIGKILLs anything that lingers.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second
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

// authedCloneURL embeds a token into a github https clone URL. Other hosts and
// non-https URLs are returned unchanged (they are rejected earlier by the control
// plane's repo allowlist).
func authedCloneURL(repoURL, token string) string {
	u := strings.TrimSuffix(repoURL, ".git")
	const gh = "https://github.com/"
	if strings.HasPrefix(u, gh) {
		return "https://x-access-token:" + token + "@github.com/" + strings.TrimPrefix(u, gh) + ".git"
	}
	return repoURL
}

// redactor returns a function that strips the clone token and any env value (of a
// meaningful length) from build output, so nothing secret is shipped to the
// control plane or shown in the Build tab.
func redactor(token string, envValues []string) func(string) string {
	secrets := make([]string, 0, len(envValues)+1)
	if len(token) >= 8 {
		secrets = append(secrets, token)
	}
	for _, kv := range envValues {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			if v := kv[i+1:]; len(v) >= 5 {
				secrets = append(secrets, v)
			}
		}
	}
	return func(s string) string {
		for _, v := range secrets {
			s = strings.ReplaceAll(s, v, "[redacted]")
		}
		return s
	}
}

// writableHome returns a writable HOME and DOCKER_CONFIG for build subprocesses,
// creating them if needed. Best effort: on failure the caller's env still points at
// the paths and docker/nixpacks surface a clear error in the build log.
func writableHome() (home, dockerConfig string) {
	home = filepath.Join(os.TempDir(), "ruust-build-home")
	dockerConfig = filepath.Join(home, ".docker")
	_ = os.MkdirAll(dockerConfig, 0o755)
	return home, dockerConfig
}

// ensureNixpkgsPin writes a minimal nixpacks.toml pinning the nixpkgs archive when the
// repo does not already carry Nixpacks config, so the build resolves its Node (and other
// setup packages) against a current nixpkgs rather than the buildpack's stale bundled
// snapshot. The partial config merges over Nixpacks' auto-detected plan: it only pins the
// archive and leaves stack detection, install, build and start phases to Nixpacks.
//
// A repo that ships its own nixpacks.toml or nixpacks.json owns that file and is left
// untouched (the customer can pin the archive there themselves). Best effort: a write
// failure is logged and the build proceeds on the bundled snapshot.
func ensureNixpkgsPin(buildDir string, appendLog func(string)) {
	for _, name := range []string{"nixpacks.toml", "nixpacks.json"} {
		if _, err := os.Stat(filepath.Join(buildDir, name)); err == nil {
			return // the repo owns its Nixpacks config; do not override it
		}
	}
	toml := "# Written by ruust-agent: pin nixpkgs to a current Node.\n" +
		"[phases.setup]\n" +
		"nixpkgsArchive = '" + pinnedNixpkgsArchive + "'\n"
	if err := os.WriteFile(filepath.Join(buildDir, "nixpacks.toml"), []byte(toml), 0o644); err != nil {
		appendLog("[build] warning: could not pin nixpkgs archive: " + err.Error() + "\n")
		return
	}
	appendLog("[build] pinning nixpkgs archive " + pinnedNixpkgsArchive[:12] + " for a current Node\n")
}

// ensureNixpacks returns a path to the nixpacks binary, installing the pinned
// release into a writable cache directory when it is not already on PATH. This lets
// an already-enrolled BYO host build without re-running its bootstrap.
func ensureNixpacks(ctx context.Context, appendLog func(string)) (string, error) {
	// Only accept an existing nixpacks if it is the version we pin. A host bootstrapped
	// on (or previously self-installed with) an older release keeps that binary on PATH
	// or in the cache dir, and an existence-only check would let a stale Nixpacks keep
	// shipping old package versions (a Node too old for modern Prisma, say) forever. A
	// version mismatch falls through to re-install the pinned release into the cache.
	if p, err := exec.LookPath("nixpacks"); err == nil && nixpacksVersionMatches(p) {
		return p, nil
	}
	dir := nixpacksCacheDir()
	bin := filepath.Join(dir, "nixpacks")
	if fi, err := os.Stat(bin); err == nil && !fi.IsDir() && nixpacksVersionMatches(bin) {
		return bin, nil
	}

	var arch string
	switch runtime.GOARCH {
	case "amd64":
		arch = "x86_64-unknown-linux-musl"
	case "arm64":
		arch = "aarch64-unknown-linux-musl"
	default:
		return "", fmt.Errorf("unsupported architecture for nixpacks: %s", runtime.GOARCH)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	url := fmt.Sprintf(
		"https://github.com/railwayapp/nixpacks/releases/download/v%s/nixpacks-v%s-%s.tar.gz",
		nixpacksVersion, nixpacksVersion, arch,
	)
	appendLog(fmt.Sprintf("[build] installing nixpacks %s\n", nixpacksVersion))
	if err := downloadNixpacks(ctx, url, bin); err != nil {
		return "", err
	}
	return bin, nil
}

// nixpacksVersionMatches reports whether the nixpacks at bin is the pinned version.
// `nixpacks --version` prints e.g. "nixpacks 1.39.0"; a non-match (or an error running
// it) means re-install, so the buildpack's bundled package set stays current.
func nixpacksVersionMatches(bin string) bool {
	out, err := exec.Command(bin, "--version").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), nixpacksVersion)
}

// nixpacksCacheDir picks the first writable directory to cache the nixpacks binary.
func nixpacksCacheDir() string {
	for _, d := range []string{"/var/lib/ruust/bin", "/opt/ruust/bin"} {
		if writableDir(d) {
			return d
		}
	}
	return filepath.Join(os.TempDir(), "ruust-bin")
}

func writableDir(dir string) bool {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false
	}
	f, err := os.CreateTemp(dir, ".wtest")
	if err != nil {
		return false
	}
	_ = f.Close()
	_ = os.Remove(f.Name())
	return true
}

// downloadNixpacks fetches the release tarball and extracts the nixpacks binary to
// dest (mode 0755).
func downloadNixpacks(ctx context.Context, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading nixpacks: HTTP %d", resp.StatusCode)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if filepath.Base(h.Name) != "nixpacks" {
			continue
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // trusted release tarball
			_ = out.Close()
			return err
		}
		return out.Close()
	}
	return fmt.Errorf("nixpacks binary not found in the release archive")
}
