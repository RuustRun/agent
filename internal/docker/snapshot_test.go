package docker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

// TestPostgresSnapshotRestore proves the migration primitive at the Docker layer:
// dump a live Postgres Egg, restore it into a brand-new volume, and confirm the data
// (row counts) survives. This is phase 1 of automated database Egg migration.
func TestPostgresSnapshotRestore(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	const image = "postgres:16-alpine"
	env := []string{"POSTGRES_USER=ruust", "POSTGRES_PASSWORD=snaptestpw", "POSTGRES_DB=ruust"}

	if err := e.EnsureImage(ctx, image); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	cleanup := func() {
		for _, n := range []string{"snaptest-src", "snaptest-dst"} {
			_ = e.cli.ContainerRemove(ctx, n, container.RemoveOptions{Force: true})
		}
		for _, v := range []string{"snaptest-src-vol", "snaptest-dst-vol"} {
			_ = e.cli.VolumeRemove(ctx, v, true)
		}
	}
	cleanup()
	defer cleanup()

	// Start a source Postgres on a fresh volume and wait until it is ready.
	src := startPostgres(t, e, ctx, "snaptest-src", "snaptest-src-vol", image, env)

	// Seed 42 rows.
	seed := "CREATE TABLE widgets(id serial primary key, name text); " +
		"INSERT INTO widgets(name) SELECT 'w'||g FROM generate_series(1,42) g;"
	if err := e.execCapture(ctx, src, []string{"psql", "-U", "ruust", "-d", "ruust", "-v", "ON_ERROR_STOP=1", "-c", seed}, pgEnv(env), nil, io.Discard); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Snapshot the live source.
	var dump bytes.Buffer
	if err := e.SnapshotPostgres(ctx, src, env, &dump); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if dump.Len() == 0 {
		t.Fatal("snapshot produced an empty dump")
	}
	t.Logf("dump is %d bytes", dump.Len())

	// Restore into a brand-new volume + container.
	dst, err := e.RestorePostgres(ctx, "snaptest-dst", "snaptest-dst-vol", image, env, &dump)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify the data made it across.
	got, err := e.QueryScalarPostgres(ctx, dst, env, "SELECT count(*) FROM widgets")
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if got != "42" {
		t.Errorf("restored row count = %q, want 42", got)
	}
}

// TestSnapshotRestoreDatabaseOrchestration exercises the higher-level Migrator methods
// the agent actually calls: SnapshotLive from the running source, then RestoreDatabase
// into a fresh volume (target), asserting both the data and the verification counts
// survive. This is the real migration primitive.
func TestSnapshotRestoreDatabaseOrchestration(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	const image = "postgres:16-alpine"
	env := []string{"POSTGRES_USER=ruust", "POSTGRES_PASSWORD=orchpw", "POSTGRES_DB=ruust"}
	if err := e.EnsureImage(ctx, image); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	cleanup := func() {
		for _, n := range []string{"orch-src", "ruust-mrest-mtest"} {
			_ = e.cli.ContainerRemove(ctx, n, container.RemoveOptions{Force: true})
		}
		for _, v := range []string{"orch-src-vol", "orch-dst-vol"} {
			_ = e.cli.VolumeRemove(ctx, v, true)
		}
	}
	cleanup()
	defer cleanup()

	// Seed a live source Postgres.
	src := startPostgres(t, e, ctx, "orch-src", "orch-src-vol", image, env)
	seed := "CREATE TABLE widgets(id serial primary key, name text); " +
		"INSERT INTO widgets(name) SELECT 'w'||g FROM generate_series(1,42) g;"
	if err := e.execCapture(ctx, src, []string{"psql", "-U", "ruust", "-d", "ruust", "-v", "ON_ERROR_STOP=1", "-c", seed}, pgEnv(env), nil, io.Discard); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Source: snapshot the LIVE container (it keeps serving).
	var dump bytes.Buffer
	sc, err := e.SnapshotLive(ctx, "postgres", src, env, &dump)
	if err != nil {
		t.Fatalf("snapshot live: %v", err)
	}
	if sc.Tables != 1 {
		t.Errorf("source table count = %d, want 1", sc.Tables)
	}

	// Target: restore into a brand-new volume and confirm the counts match.
	tc, err := e.RestoreDatabase(ctx, "postgres", "ruust-mrest-mtest", "orch-dst-vol", image, env, &dump)
	if err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if tc.Tables != sc.Tables {
		t.Errorf("target table count = %d, want %d", tc.Tables, sc.Tables)
	}
}

// TestSnapshotRestoreDatabaseRedis is the Redis equivalent of the orchestration test.
func TestSnapshotRestoreDatabaseRedis(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	const image = "redis:7-alpine"
	if err := e.EnsureImage(ctx, image); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	cleanup := func() {
		for _, n := range []string{"orch-rsrc", "ruust-mrest-rtest", "ruust-mrest-rtest-seed"} {
			_ = e.cli.ContainerRemove(ctx, n, container.RemoveOptions{Force: true})
		}
		for _, v := range []string{"orch-rsrc-vol", "orch-rdst-vol"} {
			_ = e.cli.VolumeRemove(ctx, v, true)
		}
	}
	cleanup()
	defer cleanup()

	// Seed a live source Redis (kept running: the snapshot is taken live so the 42
	// in-memory keys are captured, which is exactly why SnapshotLive is correct here).
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image},
		&container.HostConfig{
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: "orch-rsrc-vol", Target: "/data"}},
		}, nil, nil, "orch-rsrc")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start source: %v", err)
	}
	if err := e.waitRedisReady(ctx, created.ID, 30*time.Second); err != nil {
		t.Fatalf("source not ready: %v", err)
	}
	if err := e.execCapture(ctx, created.ID,
		[]string{"redis-cli", "EVAL", "for i=1,42 do redis.call('set','k'..i,i) end return 42", "0"},
		nil, nil, io.Discard); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var rdb bytes.Buffer
	sc, err := e.SnapshotLive(ctx, "redis", created.ID, nil, &rdb)
	if err != nil {
		t.Fatalf("snapshot live: %v", err)
	}
	if sc.Keys != 42 {
		t.Errorf("source key count = %d, want 42", sc.Keys)
	}
	tc, err := e.RestoreDatabase(ctx, "redis", "ruust-mrest-rtest", "orch-rdst-vol", image, nil, &rdb)
	if err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if tc.Keys != sc.Keys {
		t.Errorf("target key count = %d, want %d", tc.Keys, sc.Keys)
	}
}

func startPostgres(t *testing.T, e *engineClient, ctx context.Context, name, volumeName, image string, env []string) string {
	t.Helper()
	cfg := &container.Config{Image: image, Env: env}
	hostCfg := &container.HostConfig{
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/var/lib/postgresql/data"}},
	}
	created, err := e.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	if err := e.waitPgReady(ctx, created.ID, env, 90*time.Second); err != nil {
		t.Fatalf("%s not ready: %v", name, err)
	}
	return created.ID
}

// TestRedisSnapshotRestore proves the Redis migration primitive: SAVE + copy the RDB
// off a live Redis Egg, seed a fresh volume with it, boot Redis, and confirm the keys
// survive.
func TestRedisSnapshotRestore(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skip("no docker client")
	}
	ctx := context.Background()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skip("docker not reachable")
	}
	e := &engineClient{cli: cli, publishHost: "127.0.0.1"}
	const image = "redis:7-alpine"
	if err := e.EnsureImage(ctx, image); err != nil {
		t.Fatalf("ensure image: %v", err)
	}

	cleanup := func() {
		for _, n := range []string{"snaptest-rsrc", "snaptest-rdst", "snaptest-rdst-seed"} {
			_ = e.cli.ContainerRemove(ctx, n, container.RemoveOptions{Force: true})
		}
		for _, v := range []string{"snaptest-rsrc-vol", "snaptest-rdst-vol"} {
			_ = e.cli.VolumeRemove(ctx, v, true)
		}
	}
	cleanup()
	defer cleanup()

	// Source Redis on a fresh volume.
	created, err := e.cli.ContainerCreate(ctx,
		&container.Config{Image: image},
		&container.HostConfig{
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: "snaptest-rsrc-vol", Target: "/data"}},
		}, nil, nil, "snaptest-rsrc")
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start source: %v", err)
	}
	if err := e.waitRedisReady(ctx, created.ID, 30*time.Second); err != nil {
		t.Fatalf("source not ready: %v", err)
	}

	// Seed 42 keys.
	if err := e.execCapture(ctx, created.ID,
		[]string{"redis-cli", "EVAL", "for i=1,42 do redis.call('set','k'..i,i) end return 42", "0"},
		nil, nil, io.Discard); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Snapshot, restore into a new volume.
	var rdb bytes.Buffer
	if err := e.SnapshotRedis(ctx, created.ID, &rdb); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if rdb.Len() == 0 {
		t.Fatal("snapshot produced an empty rdb")
	}
	t.Logf("rdb is %d bytes", rdb.Len())

	dst, err := e.RestoreRedis(ctx, "snaptest-rdst", "snaptest-rdst-vol", image, &rdb)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := e.RedisCommand(ctx, dst, "DBSIZE")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got != "42" {
		t.Errorf("restored DBSIZE = %q, want 42", got)
	}
}
