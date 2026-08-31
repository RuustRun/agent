package docker

// Snapshot and restore for database Eggs, the primitive behind automated migration
// (move a database Egg's data to another host without loss when draining a node).
//
// The approach is LOGICAL: a portable dump taken with the engine's own tools inside
// a running container (pg_dump / redis SAVE), not a physical copy of the data dir.
// It reuses the exec path against the live container, so the bulk snapshot needs no
// downtime, and it is version- and architecture-portable across hosts.
//
// These functions never log the dump contents or the database credentials. British
// English. No em dashes.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/pkg/stdcopy"
)

// Migrator is the database Egg migration capability, kept separate from the core
// Client so the reconcile logic (and its fake) need not know about it. The real
// engineClient implements both; the agent's migration handler type-asserts to this.
type Migrator interface {
	// SnapshotLive streams a logical dump of the LIVE serving container to w and reads
	// the verification counts. The source keeps serving during the dump: pg_dump takes
	// an MVCC-consistent snapshot and redis SAVE flushes current memory to disk, so the
	// dump reflects a real point in time. Snapshotting the live container (rather than a
	// throwaway on a stopped volume) is what makes it correct for an in-memory engine
	// like Redis, whose data is not on disk until it is saved.
	SnapshotLive(ctx context.Context, engine, containerID string, env []string, w io.Writer) (contract.MigrationCounts, error)
	// RestoreDatabase restores a logical dump read from r into a FRESH copy of the
	// Egg's volume, reads the verification counts, then removes the restore container
	// (the Egg only starts serving on the target after cutover). Isolated (no network)
	// and idempotent: a retry re-restores cleanly.
	RestoreDatabase(ctx context.Context, engine, name, volumeName, image string, env []string, r io.Reader) (contract.MigrationCounts, error)
}

// envValue extracts KEY from a slice of "KEY=VALUE" strings, or returns def.
func envValue(env []string, key, def string) string {
	prefix := key + "="
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return e[len(prefix):]
		}
	}
	return def
}

// pgEnv adds PGPASSWORD (from POSTGRES_PASSWORD) so pg_dump/pg_restore/psql
// authenticate regardless of the image's local auth method. Never logged.
func pgEnv(env []string) []string {
	out := append([]string{}, env...)
	if pw := envValue(env, "POSTGRES_PASSWORD", ""); pw != "" {
		out = append(out, "PGPASSWORD="+pw)
	}
	return out
}

// execCapture runs cmd inside containerID, optionally streaming stdin in and stdout
// to the given writer. A non-zero exit is an error carrying the (short) stderr.
func (e *engineClient) execCapture(
	ctx context.Context,
	containerID string,
	cmd []string,
	env []string,
	stdin io.Reader,
	stdout io.Writer,
) error {
	created, err := e.cli.ContainerExecCreate(ctx, containerID, types.ExecConfig{
		Cmd:          cmd,
		Env:          env,
		AttachStdin:  stdin != nil,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return fmt.Errorf("exec create: %w", err)
	}
	resp, err := e.cli.ContainerExecAttach(ctx, created.ID, types.ExecStartCheck{})
	if err != nil {
		return fmt.Errorf("exec attach: %w", err)
	}
	defer resp.Close()

	if stdin != nil {
		go func() {
			_, _ = io.Copy(resp.Conn, stdin)
			_ = resp.CloseWrite()
		}()
	}
	var stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(stdout, &stderr, resp.Reader); err != nil {
		return fmt.Errorf("exec stream: %w", err)
	}
	insp, err := e.cli.ContainerExecInspect(ctx, created.ID)
	if err != nil {
		return fmt.Errorf("exec inspect: %w", err)
	}
	if insp.ExitCode != 0 {
		return fmt.Errorf("command %v exited %d: %s", cmd[0], insp.ExitCode, safeSnippetExec(stderr.String()))
	}
	return nil
}

// safeSnippetExec trims stderr to a short, single-line diagnostic. Dump contents and
// secrets never flow through stderr, so a snippet is safe.
func safeSnippetExec(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
}

// SnapshotPostgres streams a custom-format pg_dump of the live source container to w.
// The engine keeps serving throughout (pg_dump takes an MVCC-consistent snapshot).
func (e *engineClient) SnapshotPostgres(ctx context.Context, containerID string, env []string, w io.Writer) error {
	user := envValue(env, "POSTGRES_USER", "postgres")
	dbname := envValue(env, "POSTGRES_DB", user)
	return e.execCapture(ctx, containerID,
		[]string{"pg_dump", "-U", user, "-d", dbname, "-Fc"},
		pgEnv(env), nil, w)
}

// RestorePostgres brings up a fresh Postgres against the target volume (the entrypoint
// runs initdb and creates POSTGRES_DB), waits until it is genuinely ready, then
// pg_restores the dump read from r into it. Returns the restored container's id (left
// running so the caller can verify, then stop or keep it). Idempotent: pg_restore uses
// --clean --if-exists so a retry re-restores cleanly.
func (e *engineClient) RestorePostgres(
	ctx context.Context,
	name, volumeName, image string,
	env []string,
	r io.Reader,
) (string, error) {
	if err := e.EnsureImage(ctx, image); err != nil {
		return "", err
	}
	if err := e.ensureEggNetwork(ctx); err != nil {
		return "", err
	}
	// Remove any leftover container with this name from a previous attempt.
	_ = e.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})

	cfg := &container.Config{Image: image, Env: env}
	hostCfg := &container.HostConfig{
		NetworkMode:   container.NetworkMode(EggNetwork),
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/var/lib/postgresql/data"}},
	}
	created, err := e.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("creating restore container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return created.ID, fmt.Errorf("starting restore container: %w", err)
	}
	if err := e.waitPgReady(ctx, created.ID, env, 90*time.Second); err != nil {
		return created.ID, err
	}
	user := envValue(env, "POSTGRES_USER", "postgres")
	dbname := envValue(env, "POSTGRES_DB", user)
	if err := e.execCapture(ctx, created.ID,
		[]string{"pg_restore", "-U", user, "-d", dbname, "--clean", "--if-exists", "--no-owner"},
		pgEnv(env), r, io.Discard); err != nil {
		return created.ID, fmt.Errorf("pg_restore: %w", err)
	}
	return created.ID, nil
}

// waitPgReady waits until Postgres is genuinely accepting connections. A fresh volume
// starts a TEMPORARY server for init before restarting the real one, so we require
// several consecutive successes to avoid restoring into the temporary instance.
func (e *engineClient) waitPgReady(ctx context.Context, containerID string, env []string, timeout time.Duration) error {
	user := envValue(env, "POSTGRES_USER", "postgres")
	deadline := time.Now().Add(timeout)
	streak := 0
	for {
		if err := e.execCapture(ctx, containerID, []string{"pg_isready", "-U", user, "-q"}, pgEnv(env), nil, io.Discard); err == nil {
			streak++
			if streak >= 3 {
				return nil
			}
		} else {
			streak = 0
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres did not become ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// QueryScalarPostgres runs a single-value SQL query and returns the trimmed result,
// used to verify a restore (e.g. row counts match the source).
func (e *engineClient) QueryScalarPostgres(ctx context.Context, containerID string, env []string, sql string) (string, error) {
	user := envValue(env, "POSTGRES_USER", "postgres")
	dbname := envValue(env, "POSTGRES_DB", user)
	var out bytes.Buffer
	err := e.execCapture(ctx, containerID,
		[]string{"psql", "-U", user, "-d", dbname, "-tAc", sql},
		pgEnv(env), nil, &out)
	return strings.TrimSpace(out.String()), err
}

// SnapshotRedis forces a synchronous save on the live source container and streams the
// resulting RDB file to w. A Ruust Redis Egg carries no auth, so redis-cli needs none.
func (e *engineClient) SnapshotRedis(ctx context.Context, containerID string, w io.Writer) error {
	if err := e.execCapture(ctx, containerID, []string{"redis-cli", "SAVE"}, nil, nil, io.Discard); err != nil {
		return fmt.Errorf("redis SAVE: %w", err)
	}
	return e.execCapture(ctx, containerID, []string{"cat", "/data/dump.rdb"}, nil, nil, w)
}

// RestoreRedis places the RDB read from r into the target volume BEFORE Redis boots
// (Redis only loads an RDB at startup), then starts Redis on that volume so it loads
// the data. Returns the running container's id. A throwaway helper container writes
// the file into the fresh volume.
func (e *engineClient) RestoreRedis(ctx context.Context, name, volumeName, image string, r io.Reader) (string, error) {
	if err := e.EnsureImage(ctx, image); err != nil {
		return "", err
	}
	if err := e.ensureEggNetwork(ctx); err != nil {
		return "", err
	}
	mounts := []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/data"}}

	// 1. A helper that just sleeps (Redis never starts), so we can write the RDB into
	// the fresh volume before the real server boots and would load an empty one.
	helper := name + "-seed"
	_ = e.cli.ContainerRemove(ctx, helper, container.RemoveOptions{Force: true})
	hCreated, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image, Entrypoint: []string{"sh", "-c", "sleep 3600"}},
		&container.HostConfig{Mounts: mounts, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}},
		nil, nil, helper)
	if err != nil {
		return "", fmt.Errorf("creating seed container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, hCreated.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("starting seed container: %w", err)
	}
	writeErr := e.execCapture(ctx, hCreated.ID, []string{"sh", "-c", "cat > /data/dump.rdb"}, nil, r, io.Discard)
	_ = e.cli.ContainerRemove(ctx, hCreated.ID, container.RemoveOptions{Force: true})
	if writeErr != nil {
		return "", fmt.Errorf("writing rdb into volume: %w", writeErr)
	}

	// 2. Start the real Redis on the seeded volume; it loads dump.rdb on boot.
	_ = e.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image},
		&container.HostConfig{
			NetworkMode:   container.NetworkMode(EggNetwork),
			Mounts:        mounts,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		}, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("creating redis container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return created.ID, fmt.Errorf("starting redis container: %w", err)
	}
	if err := e.waitRedisReady(ctx, created.ID, 30*time.Second); err != nil {
		return created.ID, err
	}
	return created.ID, nil
}

// waitRedisReady waits until Redis answers PING with PONG.
func (e *engineClient) waitRedisReady(ctx context.Context, containerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var out bytes.Buffer
		if err := e.execCapture(ctx, containerID, []string{"redis-cli", "PING"}, nil, nil, &out); err == nil &&
			strings.TrimSpace(out.String()) == "PONG" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("redis did not become ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// RedisCommand runs a redis-cli command and returns the trimmed reply, used to verify
// a restore (e.g. DBSIZE matches the source).
func (e *engineClient) RedisCommand(ctx context.Context, containerID string, args ...string) (string, error) {
	var out bytes.Buffer
	err := e.execCapture(ctx, containerID, append([]string{"redis-cli"}, args...), nil, nil, &out)
	return strings.TrimSpace(out.String()), err
}

// ---- Higher-level migration orchestration (implements Migrator) --------------

// pgTableCountSQL counts a database's user tables, the measure the control plane
// compares between source and target before it cuts over.
const pgTableCountSQL = "SELECT count(*) FROM information_schema.tables " +
	"WHERE table_schema NOT IN ('pg_catalog','information_schema')"

// removeByName force-removes a container by name, ignoring "no such container".
func (e *engineClient) removeByName(ctx context.Context, name string) {
	_ = e.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
}

// SnapshotLive implements Migrator: dump the live serving container to w and read its
// verification counts. The source keeps running throughout.
func (e *engineClient) SnapshotLive(ctx context.Context, engine, containerID string, env []string, w io.Writer) (contract.MigrationCounts, error) {
	switch engine {
	case "postgres":
		if err := e.SnapshotPostgres(ctx, containerID, env, w); err != nil {
			return contract.MigrationCounts{}, err
		}
		return e.postgresCounts(ctx, containerID, env)
	case "redis":
		if err := e.SnapshotRedis(ctx, containerID, w); err != nil {
			return contract.MigrationCounts{}, err
		}
		return e.redisCounts(ctx, containerID)
	default:
		return contract.MigrationCounts{}, fmt.Errorf("unknown database engine %q", engine)
	}
}

// RestoreDatabase implements Migrator: restore the dump read from r into a fresh copy
// of volumeName, read the verification counts, then remove the restore container.
func (e *engineClient) RestoreDatabase(ctx context.Context, engine, name, volumeName, image string, env []string, r io.Reader) (contract.MigrationCounts, error) {
	switch engine {
	case "postgres":
		id, err := e.restorePostgresIsolated(ctx, name, volumeName, image, env, r)
		defer e.removeByName(ctx, name)
		if err != nil {
			return contract.MigrationCounts{}, err
		}
		return e.postgresCounts(ctx, id, env)
	case "redis":
		id, err := e.restoreRedisIsolated(ctx, name, volumeName, image, r)
		defer e.removeByName(ctx, name)
		defer e.removeByName(ctx, name+"-seed")
		if err != nil {
			return contract.MigrationCounts{}, err
		}
		return e.redisCounts(ctx, id)
	default:
		return contract.MigrationCounts{}, fmt.Errorf("unknown database engine %q", engine)
	}
}

// restorePostgresIsolated boots a fresh Postgres on the target volume (entrypoint runs
// initdb), waits until it is genuinely ready, then pg_restores the dump. Isolated (no
// network): the restored Egg only serves after cutover, on the normal path.
func (e *engineClient) restorePostgresIsolated(ctx context.Context, name, volumeName, image string, env []string, r io.Reader) (string, error) {
	if err := e.EnsureImage(ctx, image); err != nil {
		return "", err
	}
	e.removeByName(ctx, name)
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image, Env: env},
		&container.HostConfig{
			NetworkMode:   "none",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/var/lib/postgresql/data"}},
		}, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("creating restore container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return created.ID, fmt.Errorf("starting restore container: %w", err)
	}
	if err := e.waitPgReady(ctx, created.ID, env, 90*time.Second); err != nil {
		return created.ID, err
	}
	user := envValue(env, "POSTGRES_USER", "postgres")
	dbname := envValue(env, "POSTGRES_DB", user)
	if err := e.execCapture(ctx, created.ID,
		[]string{"pg_restore", "-U", user, "-d", dbname, "--clean", "--if-exists", "--no-owner"},
		pgEnv(env), r, io.Discard); err != nil {
		return created.ID, fmt.Errorf("pg_restore: %w", err)
	}
	return created.ID, nil
}

// restoreRedisIsolated writes the RDB into a fresh volume BEFORE Redis boots (Redis
// only loads an RDB at startup), then boots Redis on that volume with no network.
func (e *engineClient) restoreRedisIsolated(ctx context.Context, name, volumeName, image string, r io.Reader) (string, error) {
	if err := e.EnsureImage(ctx, image); err != nil {
		return "", err
	}
	mounts := []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/data"}}

	// Seed helper: never runs Redis, just holds the volume so we can write dump.rdb in.
	helper := name + "-seed"
	e.removeByName(ctx, helper)
	hCreated, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image, Entrypoint: []string{"sh", "-c", "sleep 3600"}},
		&container.HostConfig{NetworkMode: "none", Mounts: mounts, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}},
		nil, nil, helper)
	if err != nil {
		return "", fmt.Errorf("creating seed container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, hCreated.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("starting seed container: %w", err)
	}
	writeErr := e.execCapture(ctx, hCreated.ID, []string{"sh", "-c", "cat > /data/dump.rdb"}, nil, r, io.Discard)
	e.removeByName(ctx, helper)
	if writeErr != nil {
		return "", fmt.Errorf("writing rdb into volume: %w", writeErr)
	}

	e.removeByName(ctx, name)
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image},
		&container.HostConfig{NetworkMode: "none", Mounts: mounts, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}},
		nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("creating redis container: %w", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return created.ID, fmt.Errorf("starting redis container: %w", err)
	}
	if err := e.waitRedisReady(ctx, created.ID, 30*time.Second); err != nil {
		return created.ID, err
	}
	return created.ID, nil
}

// postgresCounts reads the verification counts for a running Postgres container.
func (e *engineClient) postgresCounts(ctx context.Context, id string, env []string) (contract.MigrationCounts, error) {
	tables, err := e.QueryScalarPostgres(ctx, id, env, pgTableCountSQL)
	if err != nil {
		return contract.MigrationCounts{}, fmt.Errorf("counting tables: %w", err)
	}
	n, err := strconv.Atoi(tables)
	if err != nil {
		return contract.MigrationCounts{}, fmt.Errorf("parsing table count %q: %w", tables, err)
	}
	return contract.MigrationCounts{Tables: n}, nil
}

// redisCounts reads the verification counts for a running Redis container.
func (e *engineClient) redisCounts(ctx context.Context, id string) (contract.MigrationCounts, error) {
	size, err := e.RedisCommand(ctx, id, "DBSIZE")
	if err != nil {
		return contract.MigrationCounts{}, fmt.Errorf("reading dbsize: %w", err)
	}
	n, err := strconv.Atoi(size)
	if err != nil {
		return contract.MigrationCounts{}, fmt.Errorf("parsing dbsize %q: %w", size, err)
	}
	return contract.MigrationCounts{Keys: n}, nil
}
