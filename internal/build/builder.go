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

	"github.com/RuustRun/agent/internal/contract"
)

// nixpacksVersion pins the buildpack version, matching the build-host bootstrap.
const nixpacksVersion = "1.29.1"

// Builder tracks in-flight local builds, one per image tag.
type Builder struct {
	mu   sync.Mutex
	jobs map[string]*job
}

type job struct {
	mu       sync.Mutex
	status   string // "building", "built", "failed"
	log      strings.Builder
	sent     int  // bytes of log already handed back to the caller
	reported bool // whether the terminal (built/failed) report has been returned once
}

// New returns an empty Builder.
func New() *Builder { return &Builder{jobs: map[string]*job{}} }

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
	// Fast path: the image already exists (built this run, a previous run, or a
	// prior tick). Nothing to build and nothing to report.
	if imageExists(ctx, d.ImageTag) {
		return true, nil
	}

	b.mu.Lock()
	j, ok := b.jobs[d.ImageTag]
	if !ok {
		j = &job{status: "building"}
		b.jobs[d.ImageTag] = j
		// Detached context: the build must outlive the tick that started it.
		go b.run(context.Background(), d, envValues, cloneToken, j)
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
			return true, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "built", Log: delta}
		}
		return true, nil
	case "failed":
		if !j.reported {
			j.reported = true
			return false, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "failed", Log: delta}
		}
		// Keep asserting failed (no new log) so the control plane stays failed until a
		// redeploy mints a new tag and a fresh job.
		return false, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "failed"}
	default: // building
		return false, &contract.BuildReport{DeploymentID: d.DeploymentID, Status: "building", Log: delta}
	}
}

// run performs one build to completion in its own goroutine, streaming redacted
// output into the job's log and setting the terminal status.
func (b *Builder) run(ctx context.Context, d *contract.BuildDirective, envValues []string, cloneToken string, j *job) {
	redact := redactor(cloneToken, envValues)
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
	j.mu.Lock()
	j.status = "built"
	j.mu.Unlock()
}

// imageExists returns true when the tag is present in the local Docker image store.
func imageExists(ctx context.Context, tag string) bool {
	return exec.CommandContext(ctx, "docker", "image", "inspect", tag).Run() == nil
}

// runCmd runs a command in dir with env, streaming combined stdout+stderr line by
// line through appendLog. Returns the process error (nil on exit 0).
func runCmd(ctx context.Context, dir string, env []string, appendLog func(string), name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
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

// ensureNixpacks returns a path to the nixpacks binary, installing the pinned
// release into a writable cache directory when it is not already on PATH. This lets
// an already-enrolled BYO host build without re-running its bootstrap.
func ensureNixpacks(ctx context.Context, appendLog func(string)) (string, error) {
	if p, err := exec.LookPath("nixpacks"); err == nil {
		return p, nil
	}
	dir := nixpacksCacheDir()
	bin := filepath.Join(dir, "nixpacks")
	if fi, err := os.Stat(bin); err == nil && !fi.IsDir() {
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
