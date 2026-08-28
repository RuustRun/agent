// Package docker wraps the Docker Engine SDK behind a small interface, per the
// architecture plan (Docker Engine via the official SDK behind an interface).
// The reconcile loop depends only on the Client interface here, so it can be
// unit tested with a fake, whilst the concrete engineClient talks to a real
// Docker daemon.
//
// Every container the agent starts carries our label prefix (LabelPrefix) so we
// can enumerate exactly the containers we own and never touch anything else on
// the host. Every container is created with hard limits on every axis: memory,
// CPUs, PIDs, a read-only root filesystem, dropped capabilities,
// no-new-privileges, and a shared bridge with inter-container communication
// disabled so co-located Eggs cannot reach one another. Published ports bind to
// loopback by default, so an Egg is reachable only through the local ingress.
// There is deliberately no egress or bandwidth cap: egress is unmetered on every
// tier.
package docker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/docker/go-units"

	"github.com/RuustRun/agent/internal/contract"
)

// LabelPrefix is the namespace for every label the agent sets on containers it
// owns. We only ever enumerate, start, stop or restart containers carrying this
// prefix.
const LabelPrefix = "ruust.blob"

// Label keys the agent stamps onto every container it owns.
const (
	// LabelManaged marks a container as owned by the Ruust agent.
	LabelManaged = LabelPrefix + ".managed"
	// LabelWorkloadID carries the WorkloadSpec.ID.
	LabelWorkloadID = LabelPrefix + ".workload"
	// LabelBlobID carries the internal Blob identifier (never shown to
	// customers).
	LabelBlobID = LabelPrefix + ".blobid"
	// LabelEggID carries the customer-facing Egg identifier.
	LabelEggID = LabelPrefix + ".eggid"
	// LabelVersion carries the desired-state version this container was created
	// for, so a changed spec forces a roll.
	LabelVersion = LabelPrefix + ".version"
	// LabelImageRef carries the image reference this container was created for.
	LabelImageRef = LabelPrefix + ".imageref"
	// LabelPort carries the port the app is expected to listen on, so the health
	// probe knows where to connect.
	LabelPort = LabelPrefix + ".port"
	// LabelHealthPath carries the HTTP path the health probe requests.
	LabelHealthPath = LabelPrefix + ".healthpath"
)

// coopInternalSuffix is appended to an Egg's alias to give the documented
// <name>.coop.internal private hostname, alongside the bare name.
const coopInternalSuffix = ".coop.internal"

// networkAliases returns the DNS names a peered sibling can reach this Egg at on
// a shared private network: the bare Egg alias and the <name>.coop.internal form
// the docs describe. Both resolve to the same container. Returns nil for an empty
// alias.
func networkAliases(alias string) []string {
	if alias == "" {
		return nil
	}
	return []string{alias, alias + coopInternalSuffix}
}

// EggNetwork is the shared bridge every Egg container joins. Inter-container
// communication is disabled on it (enable_icc=false), so two Eggs co-located on
// the same host cannot reach each other over the internal network, whilst each
// Egg keeps unmetered outbound egress and its ingress through the published port.
// One shared network, rather than one per Egg, avoids exhausting Docker's default
// address pool at scale (a per-Egg network would run out at a few dozen Eggs).
const EggNetwork = "ruust-eggs"

// EggBridgeName is the fixed host bridge interface for EggNetwork. Pinning it
// (rather than letting Docker generate a br-<id> name) gives the host egress
// firewall a stable interface to match on, so it can block Egg traffic to the
// cloud metadata endpoint and the private network. See
// infra/provisioning/firewall/egg-egress.sh.
const EggBridgeName = "ruust-eggs0"

// Container is the agent's view of one running (or exited) container it owns.
// It is intentionally a small, SDK-independent shape so the reconcile package
// never imports the Docker SDK directly.
type Container struct {
	// ID is the Docker container ID.
	ID string
	// WorkloadID is the WorkloadSpec.ID this container serves.
	WorkloadID string
	// BlobID is the internal Blob identity of the Egg.
	BlobID string
	// EggID is the customer-facing Egg identity.
	EggID string
	// ImageRef is the image this container was created from.
	ImageRef string
	// SpecVersion is the desired-state version this container was created for.
	SpecVersion string
	// State is the low-level runtime state.
	State contract.ContainerState
	// Healthy reports the last observed health-check result.
	Healthy bool
	// RestartCount is the Docker-reported restart count.
	RestartCount int
	// CgroupPath is the absolute cgroup v2 path for this container, used to read
	// resource stats directly.
	CgroupPath string
}

// Client is the surface the reconcile loop depends on. It is deliberately
// narrow and idempotent: List reflects reality, and Ensure/Stop/Restart can be
// called repeatedly with the same arguments safely.
type Client interface {
	// List returns every container carrying our label prefix, running or not.
	List(ctx context.Context) ([]Container, error)
	// EnsureImage pulls the image if it is not already present. Idempotent.
	EnsureImage(ctx context.Context, imageRef string) error
	// Create creates and starts a container for the given workload with hard
	// limits on every axis. It is idempotent by workload identity: if a matching
	// container already exists it is a no-op.
	Create(ctx context.Context, spec contract.WorkloadSpec, version string) (Container, error)
	// Stop stops and removes the container with the given ID. Idempotent: a
	// missing container is treated as success.
	Stop(ctx context.Context, id string) error
	// Restart restarts the container with the given ID. Idempotent.
	Restart(ctx context.Context, id string) error
	// Stats returns current memory and CPU usage for a container, read via the
	// Docker Engine API so it works uniformly on Linux and on Docker Desktop
	// (macOS, Windows), where container cgroups are not on the host filesystem.
	Stats(ctx context.Context, id string) (contract.CgroupUsage, error)
	// Logs returns container output newer than `since` (an RFC3339 timestamp). If
	// `since` is empty a short tail is returned, so a first report is not a flood.
	// Read via the Docker Engine API so it works on Linux and Docker Desktop alike.
	Logs(ctx context.Context, id string, since string) ([]contract.LogLine, error)
	// Close releases any underlying SDK resources.
	Close() error
}

// engineClient is the concrete implementation over the Docker Engine SDK.
type engineClient struct {
	cli *client.Client
	// publishHost is the host interface that published Egg ports bind to. It
	// defaults to loopback so an Egg is only reachable through a local ingress
	// (native Caddy on the host), never on the host's public IP where it would
	// bypass TLS. The secure production shape is native Caddy plus this loopback
	// default.
	//
	// RUUST_PUBLISH_HOST overrides it, but note the security cost: binding to
	// 0.0.0.0 or the bridge gateway republishes every Egg's port on an address
	// the containers themselves can reach, which lets one Egg reach another via
	// the host and DEFEATS the enable_icc isolation. Only widen this for local
	// development, and never to 0.0.0.0 on a host serving untrusted tenants.
	publishHost string
}

// NewEngineClient connects to the local Docker daemon using the environment
// (DOCKER_HOST and friends) and negotiates the API version so the agent
// tolerates daemon version skew.
func NewEngineClient() (Client, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connecting to docker daemon: %w", err)
	}
	publishHost := os.Getenv("RUUST_PUBLISH_HOST")
	if publishHost == "" {
		publishHost = "127.0.0.1"
	}
	return &engineClient{cli: cli, publishHost: publishHost}, nil
}

// containerName builds a stable, idempotent container name from the workload
// identity so repeated Create calls collide by name rather than duplicating.
func containerName(workloadID string) string {
	return "ruust-" + workloadID
}

func (e *engineClient) List(ctx context.Context) ([]Container, error) {
	args := filters.NewArgs()
	args.Add("label", LabelManaged+"=true")

	summaries, err := e.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, fmt.Errorf("listing containers: %w", err)
	}

	out := make([]Container, 0, len(summaries))
	for _, s := range summaries {
		c := Container{
			ID:          s.ID,
			WorkloadID:  s.Labels[LabelWorkloadID],
			BlobID:      s.Labels[LabelBlobID],
			EggID:       s.Labels[LabelEggID],
			ImageRef:    s.Labels[LabelImageRef],
			SpecVersion: s.Labels[LabelVersion],
			State:       mapState(s.State),
		}

		// Inspect for restart count, health and cgroup path. Inspection failing on
		// a single container must not fail the whole reconcile, so we degrade to
		// the summary-derived fields.
		if info, ierr := e.cli.ContainerInspect(ctx, s.ID); ierr == nil {
			c.RestartCount = info.RestartCount
			c.State = mapInspectState(info)
			c.Healthy = e.probeHealth(ctx, info)
			// Up in Docker but not yet answering its health probe (still booting,
			// or bound to the wrong interface): report it as still hatching, not
			// hatched, so the Egg only goes live once it actually serves.
			if c.State == contract.StateRunning && !c.Healthy {
				c.State = contract.StateStarting
			}
			c.CgroupPath = cgroupPathFor(info)
		}
		out = append(out, c)
	}
	return out, nil
}

func (e *engineClient) EnsureImage(ctx context.Context, imageRef string) error {
	// A cheap presence check first keeps steady-state polls quiet.
	if _, _, err := e.cli.ImageInspectWithRaw(ctx, imageRef); err == nil {
		return nil
	}
	rc, err := e.cli.ImagePull(ctx, imageRef, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("pulling image %q: %w", imageRef, err)
	}
	defer func() { _ = rc.Close() }()
	// Drain the pull stream so the pull actually completes before we return.
	buf := make([]byte, 32*1024)
	for {
		_, rerr := rc.Read(buf)
		if rerr != nil {
			break
		}
	}
	return nil
}

// iccOption is the bridge option that turns off inter-container communication.
const iccOption = "com.docker.network.bridge.enable_icc"

// ensureEggNetwork makes sure the shared Egg bridge exists AND actually has
// inter-container communication disabled before a container joins it. It is
// idempotent and race-tolerant, and it fails loudly rather than silently
// attaching an Egg to a network that does not isolate it:
//
//   - A correct existing network (enable_icc=false) is a fast no-op.
//   - A pre-existing network WITHOUT isolation (an older agent, a manual create,
//     a leftover) is removed and recreated. Docker never reconfigures an existing
//     network's options, so trusting it would silently defeat isolation. If it
//     cannot be removed (containers still attached), we return an error.
//   - After creating, we read the network back and confirm the option applied,
//     so isolation is verified, never assumed.
func (e *engineClient) ensureEggNetwork(ctx context.Context) error {
	if existing, err := e.cli.NetworkInspect(ctx, EggNetwork, types.NetworkInspectOptions{}); err == nil {
		if existing.Options[iccOption] == "false" {
			return nil
		}
		// Exists but not isolated. Remove and recreate. Fail loud if we cannot.
		if rmErr := e.cli.NetworkRemove(ctx, EggNetwork); rmErr != nil {
			return fmt.Errorf(
				"egg network %q exists without inter-container isolation (enable_icc=%q) and could not be recreated: %w",
				EggNetwork, existing.Options[iccOption], rmErr,
			)
		}
	}

	if _, err := e.cli.NetworkCreate(ctx, EggNetwork, types.NetworkCreate{
		Driver:     "bridge",
		Attachable: true,
		// Not Internal: Eggs still need outbound egress, which is unmetered.
		Labels: map[string]string{LabelManaged: "true"},
		Options: map[string]string{
			// Inter-container communication off. Two Eggs on this bridge cannot
			// reach one another; each can still reach the internet.
			iccOption: "false",
			// Pin the host bridge interface name so the egress firewall has a
			// stable interface to match on (block metadata and RFC1918).
			"com.docker.network.bridge.name": EggBridgeName,
		},
	}); err != nil && !strings.Contains(strings.ToLower(err.Error()), "exists") {
		return fmt.Errorf("ensuring egg network %q: %w", EggNetwork, err)
	}

	// Verify isolation actually applied before any Egg joins (covers a losing
	// create race and any daemon that silently ignored the option).
	res, err := e.cli.NetworkInspect(ctx, EggNetwork, types.NetworkInspectOptions{})
	if err != nil {
		return fmt.Errorf("verifying egg network %q: %w", EggNetwork, err)
	}
	if res.Options[iccOption] != "false" {
		return fmt.Errorf(
			"egg network %q did not apply inter-container isolation (enable_icc=%q); refusing to place Eggs on it",
			EggNetwork, res.Options[iccOption],
		)
	}
	return nil
}

// privateBridgeName derives a stable host bridge interface name for a private
// network. It starts with "ruust" so the egress firewall's ruust+ match still
// covers it, and fits inside the 15-character interface name limit.
func privateBridgeName(networkName string) string {
	sum := sha256.Sum256([]byte(networkName))
	return "ruust-" + hex.EncodeToString(sum[:])[:9]
}

// ensurePrivateNetwork makes sure a per-customer private network exists. Unlike
// the shared Egg network, inter-container communication is LEFT ON, so a
// customer's own Eggs on this network can reach one another. Cross-tenant
// isolation is preserved because each customer's network is a separate bridge,
// and its egress is still filtered by the host firewall (the bridge is named with
// a ruust prefix so the firewall's ruust+ match covers it). Idempotent and
// race-tolerant.
func (e *engineClient) ensurePrivateNetwork(ctx context.Context, name string) error {
	if _, err := e.cli.NetworkInspect(ctx, name, types.NetworkInspectOptions{}); err == nil {
		return nil
	}
	if _, err := e.cli.NetworkCreate(ctx, name, types.NetworkCreate{
		Driver:     "bridge",
		Attachable: true,
		Labels:     map[string]string{LabelManaged: "true"},
		Options: map[string]string{
			// Ruust-prefixed interface so the egress firewall covers it. Inter-
			// container communication stays on (the default), so siblings talk.
			"com.docker.network.bridge.name": privateBridgeName(name),
		},
	}); err != nil && !strings.Contains(strings.ToLower(err.Error()), "exists") {
		return fmt.Errorf("ensuring private network %q: %w", name, err)
	}
	return nil
}

func (e *engineClient) Create(ctx context.Context, spec contract.WorkloadSpec, version string) (Container, error) {
	name := containerName(spec.ID)

	// Idempotency: if a container with this name already exists for the same
	// spec version, do nothing. If it exists for an older version, the caller
	// (reconcile) will have stopped it first, so a leftover here means a partial
	// previous run; remove it and recreate.
	if existing, err := e.cli.ContainerInspect(ctx, name); err == nil {
		if existing.Config != nil && existing.Config.Labels[LabelVersion] == version {
			return e.toContainer(ctx, existing.ID), nil
		}
		_ = e.Stop(ctx, existing.ID)
	}

	if err := e.EnsureImage(ctx, spec.ImageRef); err != nil {
		return Container{}, err
	}

	// Release phase: run the Procfile release command once (e.g. migrations) with
	// this image, env and private networks, BEFORE the workload rolls, and abort the
	// roll if it fails, so new code never serves against an unmigrated database.
	if spec.ReleaseCommand != "" {
		if err := e.runRelease(ctx, spec); err != nil {
			return Container{}, fmt.Errorf("release command failed for workload %q: %w", spec.ID, err)
		}
	}

	// Base network: the shared, inter-container-isolated Egg bridge. Private
	// reachability comes only from the explicit peer networks joined after create,
	// so an Egg on the base bridge alone can reach no sibling. The deprecated
	// single-network form (NetworkName with no Networks) is still honoured for one
	// version of skew.
	netName := EggNetwork
	useLegacyNet := len(spec.Networks) == 0 && spec.NetworkName != ""
	if useLegacyNet {
		netName = spec.NetworkName
		if err := e.ensurePrivateNetwork(ctx, netName); err != nil {
			return Container{}, err
		}
	} else if err := e.ensureEggNetwork(ctx); err != nil {
		return Container{}, err
	}

	limits := spec.Limits
	dropped := limits.DroppedCaps
	if len(dropped) == 0 {
		dropped = []string{"ALL"}
	}

	labels := map[string]string{
		LabelManaged:    "true",
		LabelWorkloadID: spec.ID,
		LabelBlobID:     spec.BlobID,
		LabelEggID:      spec.EggID,
		LabelImageRef:   spec.ImageRef,
		LabelVersion:    version,
		// Recorded so the periodic health probe knows where and what to request.
		LabelPort:       strconv.Itoa(spec.Port),
		LabelHealthPath: spec.HealthCheckPath,
	}

	// Env values are injected out of band: the agent fetched them from the secrets
	// endpoint and placed them on spec.EnvValues ("KEY=VALUE" lines). They are set
	// on the container here and never logged.
	//
	// The port contract: Ruust sets PORT so the app binds to the port we route to.
	// It is appended last, so it is authoritative even if a customer also set PORT
	// (the app must listen where the ingress and health check expect it). We do not
	// mutate spec.EnvValues.
	env := spec.EnvValues
	if spec.Port > 0 {
		env = append(append([]string{}, spec.EnvValues...), fmt.Sprintf("PORT=%d", spec.Port))
	}
	config := &container.Config{
		Image:  spec.ImageRef,
		Env:    env,
		Labels: labels,
	}

	hostConfig := &container.HostConfig{
		// The Egg joins its chosen network: the shared isolated bridge by default,
		// or its customer's private network when internal networking is on.
		NetworkMode: container.NetworkMode(netName),
		// Limits on every axis, enforced through cgroups v2.
		Resources: container.Resources{
			// memory.max: a hard ceiling. The container is killed if it exceeds it.
			Memory: int64(limits.MemoryMb) * int64(units.MiB),
			// cpu.weight: the guaranteed share under contention, proportional to the
			// tier's CPU floor. 1 vCPU == 1024 shares by convention; the daemon
			// converts shares to a cgroup v2 cpu.weight.
			CPUShares: int64(math.Round(limits.CpuFloor * 1024)),
			// cpu.max: the absolute burst ceiling the container may reach when the
			// host is idle. Bursting above the floor is never chargeable.
			NanoCPUs: int64(limits.CpuBurst * 1e9),
			// pids.max: a hard cap on process IDs, per tier.
			PidsLimit: int64ptr(int64(limits.PidsLimit)),
		},
		ReadonlyRootfs: limits.ReadOnlyRootfs,
		CapDrop:        dropped,
		SecurityOpt:    []string{"no-new-privileges"},
		// Restart policy is intentionally "no": the agent owns restart decisions
		// so it can count restarts and surface cracked Eggs, rather than letting
		// the daemon silently loop a crash.
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
		// There is deliberately no egress or bandwidth cap here. Egress is
		// unmetered on every tier. Rate-based fair use lives elsewhere (phase 6),
		// not as a hard cap on the container.
	}

	// Persistent volumes for stateful Eggs (databases). Each is a named Docker
	// volume, created on demand and reused across recreations, so the data outlives
	// a redeploy or restart. A volume is writable even under a read-only rootfs, so
	// the engine's data dir works whilst the rest of the filesystem stays locked
	// down where the image allows it.
	for _, v := range spec.Volumes {
		if v.Name == "" || v.Path == "" {
			continue
		}
		hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
			Type:   mount.TypeVolume,
			Source: v.Name,
			Target: v.Path,
		})
	}

	// Publish the container port to a host port when the desired state asks for
	// it, so the local ingress (Caddy) can reach the Egg. The bind defaults to
	// loopback (publishHost) so the Egg is never directly reachable on the host's
	// public IP where it would bypass TLS and the ingress tier.
	if spec.PublishPort > 0 && spec.Port > 0 {
		if cp, perr := nat.NewPort("tcp", strconv.Itoa(spec.Port)); perr == nil {
			config.ExposedPorts = nat.PortSet{cp: struct{}{}}
			hostConfig.PortBindings = nat.PortMap{
				cp: []nat.PortBinding{{HostIP: e.publishHost, HostPort: strconv.Itoa(spec.PublishPort)}},
			}
		}
	}

	// On the deprecated single network, give the container its alias at create
	// time. Peer networks (the current model) are attached after create, below.
	var netConfig *network.NetworkingConfig
	if useLegacyNet && spec.InternalAlias != "" {
		netConfig = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				netName: {Aliases: networkAliases(spec.InternalAlias)},
			},
		}
	}

	created, err := e.cli.ContainerCreate(ctx, config, hostConfig, netConfig, nil, name)
	if err != nil {
		return Container{}, fmt.Errorf("creating container for workload %q: %w", spec.ID, err)
	}

	// Attach each peer network before start: a dedicated two-party bridge shared
	// with one explicitly peered sibling, joined with this Egg's alias so the peer
	// can reach it by name. Only peered pairs share a network, so this is the whole
	// of the deny-by-default enforcement. A peering change bumps the desired-state
	// version, so the container is recreated with the correct set here.
	for _, att := range spec.Networks {
		if att.Name == "" {
			continue
		}
		if err := e.ensurePrivateNetwork(ctx, att.Name); err != nil {
			_ = e.Stop(ctx, created.ID)
			return Container{}, err
		}
		endpoint := &network.EndpointSettings{}
		if att.Alias != "" {
			// Reachable at the bare Egg name and the documented
			// <name>.coop.internal form; both resolve to this container.
			endpoint.Aliases = networkAliases(att.Alias)
		}
		if err := e.cli.NetworkConnect(ctx, att.Name, created.ID, endpoint); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "already exists") {
			_ = e.Stop(ctx, created.ID)
			return Container{}, fmt.Errorf("connecting workload %q to peer network %q: %w", spec.ID, att.Name, err)
		}
	}

	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return Container{}, fmt.Errorf("starting container for workload %q: %w", spec.ID, err)
	}
	return e.toContainer(ctx, created.ID), nil
}

// runRelease runs the workload's release command once as a short-lived container,
// with the same image, env and private networks as the workload, and returns an
// error if it exits non-zero. The container is always removed and is deliberately
// NOT labelled as a managed workload, so reconcile never adopts it.
func (e *engineClient) runRelease(ctx context.Context, spec contract.WorkloadSpec) error {
	env := spec.EnvValues
	if spec.Port > 0 {
		env = append(append([]string{}, spec.EnvValues...), fmt.Sprintf("PORT=%d", spec.Port))
	}
	name := fmt.Sprintf("ruust-release-%s-%d", strings.ToLower(spec.ID), time.Now().UnixNano())
	config := &container.Config{
		Image:  spec.ImageRef,
		Env:    env,
		Cmd:    []string{"sh", "-c", spec.ReleaseCommand},
		Labels: map[string]string{LabelPrefix + ".release": spec.ID},
	}

	// Join the base Egg bridge (so it resolves) plus the peer networks below, so the
	// release can reach the Egg's database over private networking.
	netName := EggNetwork
	if len(spec.Networks) == 0 && spec.NetworkName != "" {
		netName = spec.NetworkName
		if err := e.ensurePrivateNetwork(ctx, netName); err != nil {
			return err
		}
	} else if err := e.ensureEggNetwork(ctx); err != nil {
		return err
	}
	hostConfig := &container.HostConfig{
		NetworkMode:   container.NetworkMode(netName),
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}

	created, err := e.cli.ContainerCreate(ctx, config, hostConfig, nil, nil, name)
	if err != nil {
		return fmt.Errorf("creating release container: %w", err)
	}
	defer func() { _ = e.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true}) }()

	for _, att := range spec.Networks {
		if att.Name == "" {
			continue
		}
		if err := e.ensurePrivateNetwork(ctx, att.Name); err != nil {
			return err
		}
		if err := e.cli.NetworkConnect(ctx, att.Name, created.ID, &network.EndpointSettings{}); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("connecting release container to %q: %w", att.Name, err)
		}
	}

	if err := e.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting release container: %w", err)
	}

	statusCh, errCh := e.cli.ContainerWait(ctx, created.ID, container.WaitConditionNotRunning)
	select {
	case werr := <-errCh:
		if werr != nil {
			return fmt.Errorf("waiting for release container: %w", werr)
		}
		return nil
	case st := <-statusCh:
		if st.StatusCode != 0 {
			return fmt.Errorf("release command exited with code %d", st.StatusCode)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *engineClient) Stop(ctx context.Context, id string) error {
	err := e.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true})
	if err != nil && !client.IsErrNotFound(err) {
		return fmt.Errorf("removing container %q: %w", id, err)
	}
	return nil
}

func (e *engineClient) Restart(ctx context.Context, id string) error {
	if err := e.cli.ContainerRestart(ctx, id, container.StopOptions{}); err != nil {
		if client.IsErrNotFound(err) {
			return nil
		}
		return fmt.Errorf("restarting container %q: %w", id, err)
	}
	return nil
}

// Stats reads one usage sample for a container via the Docker Engine API. This
// works on Linux and on Docker Desktop (macOS, Windows) alike, unlike reading
// cgroup files off the host, because the daemon reports the numbers regardless
// of where the container's cgroups physically live.
func (e *engineClient) Stats(ctx context.Context, id string) (contract.CgroupUsage, error) {
	resp, err := e.cli.ContainerStats(ctx, id, true)
	if err != nil {
		return contract.CgroupUsage{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Read up to two samples. The stats stream populates PreCPUStats on the second
	// and later frames, which we need for a valid instantaneous CPU delta.
	dec := json.NewDecoder(resp.Body)
	var last types.StatsJSON
	got := false
	for i := 0; i < 2; i++ {
		var s types.StatsJSON
		if derr := dec.Decode(&s); derr != nil {
			break
		}
		last = s
		got = true
	}
	if !got {
		return contract.CgroupUsage{}, fmt.Errorf("no stats returned for container %q", id)
	}

	// Memory used, matching `docker stats`: usage minus reclaimable file cache.
	mem := int64(last.MemoryStats.Usage)
	if inactive, ok := last.MemoryStats.Stats["inactive_file"]; ok { // cgroup v2
		mem -= int64(inactive)
	} else if cache, ok := last.MemoryStats.Stats["cache"]; ok { // cgroup v1
		mem -= int64(cache)
	}
	if mem < 0 {
		mem = 0
	}

	// CPU as a fraction of one core, from the delta between the two samples.
	var cpuFrac float64
	cpuDelta := float64(last.CPUStats.CPUUsage.TotalUsage) - float64(last.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(last.CPUStats.SystemUsage) - float64(last.PreCPUStats.SystemUsage)
	if cpuDelta > 0 && sysDelta > 0 {
		cpus := float64(last.CPUStats.OnlineCPUs)
		if cpus == 0 {
			cpus = float64(len(last.CPUStats.CPUUsage.PercpuUsage))
		}
		if cpus == 0 {
			cpus = 1
		}
		cpuFrac = (cpuDelta / sysDelta) * cpus
	}

	return contract.CgroupUsage{MemoryBytes: mem, CpuUsage: cpuFrac, EgressBytes: 0}, nil
}

// Logs returns the container's stdout and stderr as timestamped lines, newer
// than `since`. With no `since` it returns a short tail so a first report is not
// a flood. The Docker log stream multiplexes stdout and stderr with an 8-byte
// frame header; stdcopy.StdCopy demultiplexes it into the two buffers.
func (e *engineClient) Logs(ctx context.Context, id string, since string) ([]contract.LogLine, error) {
	opts := container.LogsOptions{ShowStdout: true, ShowStderr: true, Timestamps: true}
	if since != "" {
		opts.Since = since
	} else {
		opts.Tail = "50"
	}

	rc, err := e.cli.ContainerLogs(ctx, id, opts)
	if err != nil {
		return nil, fmt.Errorf("container logs: %w", err)
	}
	defer func() { _ = rc.Close() }()

	var out, errb bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &errb, rc); err != nil {
		return nil, fmt.Errorf("demultiplex logs: %w", err)
	}

	lines := append(parseLogLines(out.String(), "stdout"), parseLogLines(errb.String(), "stderr")...)
	if lines == nil {
		lines = []contract.LogLine{} // marshal as [] not null, so the report validates
	}
	// Oldest first. RFC3339Nano sorts lexically in chronological order.
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].Ts < lines[j].Ts })
	return lines, nil
}

// parseLogLines splits a timestamped log blob into structured lines. Each line
// from the runtime is "<rfc3339> <message>"; we split off the leading timestamp.
func parseLogLines(blob, stream string) []contract.LogLine {
	if blob == "" {
		return nil
	}
	raw := strings.Split(blob, "\n")
	out := make([]contract.LogLine, 0, len(raw))
	for _, ln := range raw {
		ln = strings.TrimRight(ln, "\r")
		if ln == "" {
			continue
		}
		ts, text := ln, ""
		if sp := strings.IndexByte(ln, ' '); sp >= 0 {
			ts, text = ln[:sp], ln[sp+1:]
		}
		out = append(out, contract.LogLine{Ts: ts, Stream: stream, Text: text})
	}
	return out
}

func (e *engineClient) Close() error {
	return e.cli.Close()
}

func (e *engineClient) toContainer(ctx context.Context, id string) Container {
	c := Container{ID: id}
	if info, err := e.cli.ContainerInspect(ctx, id); err == nil {
		if info.Config != nil {
			c.WorkloadID = info.Config.Labels[LabelWorkloadID]
			c.BlobID = info.Config.Labels[LabelBlobID]
			c.EggID = info.Config.Labels[LabelEggID]
			c.ImageRef = info.Config.Labels[LabelImageRef]
			c.SpecVersion = info.Config.Labels[LabelVersion]
		}
		c.RestartCount = info.RestartCount
		c.State = mapInspectState(info)
		c.Healthy = e.probeHealth(ctx, info)
		if c.State == contract.StateRunning && !c.Healthy {
			c.State = contract.StateStarting
		}
		c.CgroupPath = cgroupPathFor(info)
	}
	return c
}

// mapState maps a Docker container-summary state string to our contract state.
func mapState(s string) contract.ContainerState {
	switch strings.ToLower(s) {
	case "created":
		return contract.StatePending
	case "restarting":
		return contract.StateStarting
	case "running":
		return contract.StateRunning
	case "paused", "exited", "removing", "dead":
		return contract.StateStopped
	default:
		return contract.StateUnknown
	}
}

// mapInspectState refines state using the richer inspect result, in particular
// distinguishing a clean stop from a crash so we can report cracked Eggs.
func mapInspectState(info types.ContainerJSON) contract.ContainerState {
	if info.State == nil {
		return contract.StateUnknown
	}
	st := info.State
	switch {
	case st.Running && st.Health != nil && st.Health.Status == "starting":
		return contract.StateStarting
	case st.Running:
		return contract.StateRunning
	case st.Restarting:
		return contract.StateStarting
	case st.Status == "created":
		return contract.StatePending
	case st.ExitCode != 0 || st.OOMKilled:
		// A non-zero exit or an OOM kill is a crash (a cracked Egg).
		return contract.StateCrashed
	default:
		return contract.StateStopped
	}
}

// probeHealth reports whether a running container is actually accepting
// connections on the port the ingress routes to. It dials the container by its
// address on the host bridge, NOT via loopback, so an app that binds only to
// 127.0.0.1 inside the container is correctly seen as unhealthy: the ingress
// cannot reach it either. A plain TCP connect works for every Egg, a web server
// or a database alike, because it only asks "is something listening there". When
// the port or the container IP cannot be determined we fall back to the Docker
// health check, so this never makes a container look worse than before.
func (e *engineClient) probeHealth(ctx context.Context, info types.ContainerJSON) bool {
	if info.State == nil || !info.State.Running {
		return false
	}
	var labels map[string]string
	if info.Config != nil {
		labels = info.Config.Labels
	}
	port := labels[LabelPort]
	ip := containerIP(info)
	if port == "" || port == "0" || ip == "" {
		return isHealthy(info) // nothing to probe against; keep prior behaviour
	}
	return probeTCP(ctx, net.JoinHostPort(ip, port))
}

// probeTCP dials addr and reports whether something is listening. A refused
// connection (the localhost-bind case the docs describe) or a timeout is
// unhealthy.
func probeTCP(ctx context.Context, addr string) bool {
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// containerIP returns the container's IP on the first network that has one, so
// the agent (on the host) can reach it over the bridge.
func containerIP(info types.ContainerJSON) string {
	if info.NetworkSettings == nil {
		return ""
	}
	for _, n := range info.NetworkSettings.Networks {
		if n != nil && n.IPAddress != "" {
			return n.IPAddress
		}
	}
	return info.NetworkSettings.IPAddress
}

// isHealthy reads the Docker health check result if one is configured. With no
// health check configured, a running container is treated as healthy. Used as the
// fallback when an agent-side HTTP probe cannot be performed.
func isHealthy(info types.ContainerJSON) bool {
	if info.State == nil {
		return false
	}
	if info.State.Health != nil {
		return info.State.Health.Status == "healthy"
	}
	return info.State.Running
}

// cgroupPathFor derives the cgroup v2 path for a container. On cgroup v2 the
// path is typically under /sys/fs/cgroup for the container's ID. The concrete
// derivation depends on the daemon's cgroup driver.
//
// TODO(phase-1): resolve the exact cgroup slice from the daemon's cgroup driver
// (systemd vs cgroupfs). For the common cgroupfs driver this is
// /sys/fs/cgroup/<id>. The cgroups package tolerates a missing path and reports
// zero usage rather than failing the status report.
func cgroupPathFor(info types.ContainerJSON) string {
	if info.ID == "" {
		return ""
	}
	return "/sys/fs/cgroup/" + info.ID
}

// int64ptr is a tiny helper for the SDK fields that take a *int64.
func int64ptr(v int64) *int64 { return &v }
