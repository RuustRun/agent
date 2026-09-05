// Package contract holds the Go mirror of packages/api-contract. These structs
// describe the /api/v1 wire surface shared between the Ruust control plane and
// the Go host agent. Reconciliation is pull based: the agent polls
//
//	GET  /api/v1/hosts/:id/desired-state
//
// and reports back with
//
//	POST /api/v1/hosts/:id/status
//
// Vocabulary note. The unit a customer deploys and pays for is an "Egg".
// Internally, and in this schema, that same unit is modelled as a "Blob". The
// Egg-to-Blob mapping stays in code only and must never surface in user-facing
// text. See EggID and BlobID on WorkloadSpec below.
//
// Keep this file in step with packages/api-contract/src/index.ts whenever
// either side changes. Both sides tolerate at least one minor version of skew
// in either direction, so unknown JSON fields are ignored rather than rejected.
package contract

// APIVersion is the current contract version, reflected in the /api/v1 route
// prefix.
const APIVersion = "v1"

// EggLifecycleState is the customer-facing lifecycle of an Egg, surfaced in
// status badges, logs and the dashboard.
//
//	incubating -> building the image
//	hatching   -> deploying / rolling out
//	hatched    -> live and serving traffic
//	cold       -> stopped or suspended
//	cracked    -> crashed
type EggLifecycleState string

const (
	// EggIncubating means the Egg's image is being built.
	EggIncubating EggLifecycleState = "incubating"
	// EggHatching means the Egg is deploying or rolling out.
	EggHatching EggLifecycleState = "hatching"
	// EggHatched means the Egg is live and serving traffic.
	EggHatched EggLifecycleState = "hatched"
	// EggCold means the Egg is stopped or suspended.
	EggCold EggLifecycleState = "cold"
	// EggCracked means the Egg has crashed.
	EggCracked EggLifecycleState = "cracked"
)

// Region enumerates the deployment regions. Each region is an independent cell:
// one region going down never affects another. An Egg lives in exactly one
// region for its whole life and cannot be moved between regions.
type Region string

const (
	// RegionEUWest is London (live).
	RegionEUWest Region = "eu-west"
	// RegionUSEast is Virginia (live).
	RegionUSEast Region = "us-east"
	// RegionEUCentral is Frankfurt (coming soon).
	RegionEUCentral Region = "eu-central"
	// RegionAPSoutheast is Singapore (coming soon).
	RegionAPSoutheast Region = "ap-southeast"
)

// RegionAvailability marks whether a region is live or coming soon.
type RegionAvailability string

const (
	// RegionLive means the region is accepting workloads now.
	RegionLive RegionAvailability = "live"
	// RegionSoon means the region is coming soon.
	RegionSoon RegionAvailability = "soon"
)

// RegionInfo is a static catalogue entry describing a region, its display
// centre and its availability.
type RegionInfo struct {
	ID           Region             `json:"id"`
	Label        string             `json:"label"`
	Centre       string             `json:"centre"`
	Availability RegionAvailability `json:"availability"`
}

// Regions is the static catalogue of regions.
var Regions = []RegionInfo{
	{ID: RegionEUWest, Label: "EU West", Centre: "London", Availability: RegionLive},
	{ID: RegionUSEast, Label: "US East", Centre: "Virginia", Availability: RegionLive},
	{ID: RegionEUCentral, Label: "EU Central", Centre: "Frankfurt", Availability: RegionSoon},
	{ID: RegionAPSoutheast, Label: "Asia Pacific South East", Centre: "Singapore", Availability: RegionSoon},
}

// EggTier enumerates the flat pricing tiers, per Egg, per month, in GBP. Egress
// is unmetered on every tier, so there is deliberately no egress cap anywhere in
// this contract.
type EggTier string

const (
	// TierNano is the Nano tier.
	TierNano EggTier = "nano"
	// TierSmall is the Small tier.
	TierSmall EggTier = "small"
	// TierStandard is the Standard tier (most popular).
	TierStandard EggTier = "standard"
	// TierLarge is the Large tier.
	TierLarge EggTier = "large"
)

// EggTierSpec describes one flat pricing tier. Egress is unmetered on every
// tier, so UnmeteredEgress is always true and no egress cap is modelled.
type EggTierSpec struct {
	ID EggTier `json:"id"`
	// Label is the human-readable tier name.
	Label string `json:"label"`
	// PricePerMonthGbp is the flat price per Egg per month, in whole GBP.
	PricePerMonthGbp int `json:"pricePerMonthGbp"`
	// MemoryMb is the hard memory allowance in megabytes.
	MemoryMb int `json:"memoryMb"`
	// Vcpu is the human-readable vCPU allowance.
	Vcpu string `json:"vcpu"`
	// UnmeteredEgress is always true. Egress is unmetered on every tier.
	UnmeteredEgress bool `json:"unmeteredEgress"`
	// MostPopular flags the tier we highlight in the dashboard.
	MostPopular bool `json:"mostPopular,omitempty"`
}

// EggTiers is the static catalogue of flat pricing tiers, in GBP. Every tier
// has unmetered egress.
var EggTiers = []EggTierSpec{
	{ID: TierNano, Label: "Nano", PricePerMonthGbp: 4, MemoryMb: 256, Vcpu: "⅛ vCPU, bursts to ½", UnmeteredEgress: true},
	{ID: TierSmall, Label: "Small", PricePerMonthGbp: 8, MemoryMb: 512, Vcpu: "¼ vCPU, bursts to 1", UnmeteredEgress: true},
	{ID: TierStandard, Label: "Standard", PricePerMonthGbp: 16, MemoryMb: 1024, Vcpu: "½ vCPU, bursts to 2", UnmeteredEgress: true, MostPopular: true},
	{ID: TierLarge, Label: "Large", PricePerMonthGbp: 30, MemoryMb: 2048, Vcpu: "1 vCPU, bursts to 4", UnmeteredEgress: true},
}

// ContainerState is the low-level runtime state of a single container as
// observed by the agent. It maps up to EggLifecycleState in the control plane,
// but the two are kept distinct on the wire.
type ContainerState string

const (
	// StatePending means scheduled but not yet started.
	StatePending ContainerState = "pending"
	// StateBuilding means the image is being built or pulled.
	StateBuilding ContainerState = "building"
	// StateStarting means the container process is starting.
	StateStarting ContainerState = "starting"
	// StateRunning means running and passing health checks.
	StateRunning ContainerState = "running"
	// StateStopped means stopped on purpose (the Egg is cold).
	StateStopped ContainerState = "stopped"
	// StateCrashed means the container exited unexpectedly (the Egg is cracked).
	StateCrashed ContainerState = "crashed"
	// StateUnknown means the agent cannot determine the state.
	StateUnknown ContainerState = "unknown"
)

// EggLifecycleFor maps a low-level container state up to the customer-facing
// Egg lifecycle state. This is the only place the two vocabularies meet on the
// agent side.
func EggLifecycleFor(s ContainerState) EggLifecycleState {
	switch s {
	case StatePending, StateBuilding:
		return EggIncubating
	case StateStarting:
		return EggHatching
	case StateRunning:
		return EggHatched
	case StateStopped:
		return EggCold
	case StateCrashed:
		return EggCracked
	default:
		return EggCold
	}
}

// ResourceLimits are the hard limits the agent enforces on every axis for a
// workload's containers. Note the deliberate absence of any egress or bandwidth
// limit: egress is unmetered on every tier.
type ResourceLimits struct {
	// MemoryMb is the hard memory limit in megabytes (cgroup memory.max).
	MemoryMb int `json:"memoryMb"`
	// CpuFloor is the guaranteed vCPU share under contention, in fractional cores
	// (for example 0.5). Enforced as cgroup cpu.weight, proportional to this floor.
	CpuFloor float64 `json:"cpuFloor"`
	// CpuBurst is the burst ceiling when the host is idle, in fractional cores
	// (for example 2.0). Enforced as cgroup cpu.max. Always >= CpuFloor.
	CpuBurst float64 `json:"cpuBurst"`
	// PidsLimit is the maximum number of process IDs allowed (cgroup pids.max).
	PidsLimit int `json:"pidsLimit"`
	// ReadOnlyRootfs mounts the root filesystem read only. Defaults to true.
	ReadOnlyRootfs bool `json:"readOnlyRootfs"`
	// DroppedCaps lists the Linux capabilities to drop. Defaults to ["ALL"].
	DroppedCaps []string `json:"droppedCaps"`
}

// WorkloadSpec models a single workload the agent should run on a host. One
// WorkloadSpec models one Egg (the BlobID is the internal identity of that Egg).
type WorkloadSpec struct {
	// ID is the stable identifier for this workload on the host.
	ID string `json:"id"`
	// EggID is the customer-facing Egg identifier. Safe to show in the dashboard
	// and logs.
	EggID string `json:"eggId"`
	// BlobID is the internal Blob identifier for the same unit. Never render this
	// in user-facing text.
	BlobID string `json:"blobId"`
	// ImageRef is the OCI image reference (registry/name@digest or
	// registry/name:tag).
	ImageRef string `json:"imageRef"`
	// Replicas is the desired number of running replicas.
	Replicas int `json:"replicas"`
	// Port is the container port the workload listens on.
	Port int `json:"port"`
	// PublishPort optionally publishes the container Port to this host port, for
	// local development and demos, and for a game-server Egg (which needs a real
	// public host:port). Zero (omitted) for a plain web Egg in production, where the
	// ingress tier routes traffic and the agent publishes nothing.
	PublishPort int `json:"publishPort,omitempty"`
	// PublishProtocol is the transport to publish PublishPort with: "udp" for a
	// native game server (FishNet Tugboat), else "tcp" (the default when empty).
	PublishProtocol string `json:"publishProtocol,omitempty"`
	// Hostnames this Egg serves: its platform subdomain plus any verified custom
	// domains. The agent registers ingress routes for these and answers the ask
	// endpoint for them.
	Hostnames []string `json:"hostnames,omitempty"`
	// Env lists the names of environment variables the workload expects. Only the
	// keys travel over this contract. Secret values are injected out of band and
	// are never included here, and are never logged.
	Env []string `json:"env"`
	// HealthCheckPath is the HTTP path polled for health checks (when HealthProbe
	// is "http").
	HealthCheckPath string `json:"healthCheckPath"`
	// HealthProbe is how the agent decides a container is healthy: "http" (GET the
	// health path), "tcp" (connect to the port) or "running" (healthy whenever it
	// runs, for a database or game-server Egg that speaks a wire protocol). Empty
	// keeps the agent's existing volume/port heuristic.
	HealthProbe string `json:"healthProbe,omitempty"`
	// ReleaseCommand is an optional command from the repo Procfile's release: line
	// (e.g. "bin/rails db:migrate"). The agent runs it once as a one-shot container
	// with this deployment's image, env and private networks BEFORE rolling the
	// workload, and gates the roll on its success. Empty means no release step.
	ReleaseCommand string `json:"releaseCommand,omitempty"`
	// Revision is an opaque deployment revision. It changes on redeploy, which
	// changes the desired-state version and rolls the container even when the
	// image is unchanged. The agent does not interpret it.
	Revision string `json:"revision,omitempty"`
	// EnvHash is a hash over the env set (keys plus ciphertext). It changes when a
	// value changes, rolling the container. Carries no plaintext.
	EnvHash string `json:"envHash,omitempty"`
	// Networks are the private-network peerings this Egg joins: one dedicated
	// two-party bridge per explicitly peered sibling, each with the alias this Egg
	// answers to on it. Deny-by-default: empty means fully isolated. The agent
	// joins the isolated base bridge plus one bridge per entry, so only an
	// explicitly peered sibling shares a network and can route to this Egg.
	Networks []NetworkAttachment `json:"networks,omitempty"`
	// NetworkName is the deprecated single-network form, kept for one version of
	// skew. Preferred is Networks; this is used only when Networks is empty.
	NetworkName string `json:"networkName,omitempty"`
	// InternalAlias is this Egg's alias on the deprecated NetworkName.
	InternalAlias string `json:"internalAlias,omitempty"`
	// Volumes are persistent named volumes to mount, for stateful Eggs (databases).
	// Each survives container recreation, so the data outlives a redeploy or
	// restart. Empty for stateless (web) Eggs.
	Volumes []VolumeMount `json:"volumes,omitempty"`
	// Migration, when set, is a database Egg migration in flight this host has a part
	// in (source: snapshot + upload; target: download + restore). Nil for a normal
	// workload. See MigrationDirective.
	Migration *MigrationDirective `json:"migration,omitempty"`
	// Build, when set, tells this host to build the image from source LOCALLY (bring
	// your own host) before running it, so the customer's code never leaves the box.
	// Nil for a normal, already-built or registry-pulled workload. See BuildDirective.
	Build *BuildDirective `json:"build,omitempty"`
	// Import, when set, is a one-off data import into this database Egg from an
	// external Postgres source (pg_dump piped into the Egg's own container). Nil when
	// there is no active import. See ImportDirective.
	Import *ImportDirective `json:"import,omitempty"`
	// Backup, when set, is a backup capture or restore for this database Egg (pg_dump
	// to a durable host file, or restore of one). Nil when there is no active backup.
	// See BackupDirective.
	Backup *BackupDirective `json:"backup,omitempty"`
	// Limits are the hard resource limits enforced by the agent.
	Limits ResourceLimits `json:"limits"`
	// EnvValues is the decrypted env as "KEY=VALUE" lines, populated locally by the
	// agent from the secrets endpoint. It is never on the wire (json:"-") and is
	// never logged.
	EnvValues []string `json:"-"`
	// BuildToken is a short-lived git clone token for a PRIVATE host-built repo,
	// populated locally from the secrets endpoint. Never on the wire (json:"-"),
	// never logged, and never injected into the running container.
	BuildToken string `json:"-"`
	// ImportSourceURL is the external Postgres source connection string for an
	// in-progress data import, populated locally from the secrets endpoint. Never on
	// the wire (json:"-"), never logged, passed to the import as an env var not argv.
	ImportSourceURL string `json:"-"`
}

// BuildDirective tells a bring-your-own-host agent to build a workload's image from
// its git repository locally (nixpacks, or the repo Dockerfile), tag it ImageTag,
// and then run it. The build is idempotent: if ImageTag already exists locally the
// agent skips it. Build progress and logs are reported back on the status endpoint.
type BuildDirective struct {
	// DeploymentID is echoed back on the status report so the control plane can move
	// the right Deployment through building -> live/failed.
	DeploymentID string `json:"deploymentId"`
	// RepoURL is the https git repository to clone.
	RepoURL string `json:"repoUrl"`
	// Branch to build.
	Branch string `json:"branch"`
	// GitSha is a short revision label; a change rolls the build.
	GitSha string `json:"gitSha"`
	// RootDirectory is the subdirectory to build from, for a monorepo. Empty = root.
	RootDirectory string `json:"rootDirectory,omitempty"`
	// StartCommand overrides the start command baked into the image (Nixpacks).
	StartCommand string `json:"startCommand,omitempty"`
	// BuildEnvKeys are the build-time env var KEYS; values come from EnvValues.
	BuildEnvKeys []string `json:"buildEnvKeys,omitempty"`
	// ImageTag is the local tag the agent must produce (and, for BYO, then run).
	ImageTag string `json:"imageTag"`
	// PushTo, when set, makes this a BUILD-ONLY job on a dedicated build host: the
	// agent builds ImageTag, tags it to PushTo, pushes it to the private registry,
	// and does NOT run a container. Empty = bring-your-own-host (build and run
	// locally, nothing pushed).
	PushTo string `json:"pushTo,omitempty"`
	// Private is true when the repo needs a clone token (delivered via BuildToken).
	Private bool `json:"private,omitempty"`
}

// ImportDirective tells the agent to load data into a database Egg from an external
// source: pg_dump of the source (ImportSourceURL, from secrets) piped into the Egg's
// own running container, so the data never leaves the host. Progress is reported on
// the status endpoint. The agent runs a given ImportID only once per process.
type ImportDirective struct {
	// ImportID is echoed back on the status report so the control plane can move the
	// right Import through importing -> done/failed.
	ImportID string `json:"importId"`
	// Engine is the database engine ("postgres"), so the agent picks the right tool.
	Engine string `json:"engine"`
}

// BackupDirective tells the agent what to do with a database Egg's snapshot. The
// agent runs a given (BackupID, Action) only once per process.
//
//   - capture: run pg_dump inside the Egg's container to a durable host file, report
//     the ref/size/checksum back.
//   - restore: load a snapshot (Ref, a local host file, or GetURL for a Vault copy)
//     back into the Egg's container.
//   - upload: (source host, durable backups) send the local snapshot at Ref to PutURL
//     (control-plane staging) so it can be relayed to the Vault.
//   - store: (Vault host) download the snapshot from GetURL and write it into the
//     Vault volume at Ref, verifying Checksum.
//   - fetch: (Vault host, restore) send the Vault copy at Ref to PutURL so the source
//     host can download and restore it.
type BackupDirective struct {
	// BackupID is echoed back on the status report.
	BackupID string `json:"backupId"`
	// Action is "capture", "restore", "upload", "store", "fetch" or "delete".
	Action string `json:"action"`
	// Engine is the database engine ("postgres").
	Engine string `json:"engine"`
	// Ref is a file path: the snapshot to restore/upload/fetch, or where to write a
	// downloaded snapshot in the Vault volume (store).
	Ref string `json:"ref,omitempty"`
	// Retention, for a capture, is how many newest snapshots to keep on the host
	// (older ones are pruned after this capture).
	Retention int `json:"retention,omitempty"`
	// PutURL, for upload/fetch, is where to PUT the snapshot bytes (control-plane
	// staging, authenticated with the host token).
	PutURL string `json:"putUrl,omitempty"`
	// GetURL, for store and restore-from-vault, is where to GET the snapshot bytes.
	GetURL string `json:"getUrl,omitempty"`
	// Checksum, for store, is the sha256 to verify the downloaded snapshot against.
	Checksum string `json:"checksum,omitempty"`
}

// NetworkAttachment is one private-network peering: a two-party bridge shared with
// exactly one peered sibling, and the alias this Egg answers to on it.
type NetworkAttachment struct {
	// Name is the pair network, identical for both peered Eggs.
	Name string `json:"name"`
	// Alias is this Egg's stable DNS name on that network.
	Alias string `json:"alias"`
}

// MigrationDirective tells this host its part in a database Egg migration, so a node
// can be drained without losing the Egg's host-local volume. It is emitted to the
// SOURCE host (role "source": snapshot the live database and upload the bytes) and to
// the TARGET host (role "target": download and restore into a fresh volume). Every
// step keys on ID so it is idempotent and safely retryable, and the source stays
// authoritative and untouched until the target is verified.
type MigrationDirective struct {
	// ID is the migration id, the idempotency key for every step.
	ID string `json:"id"`
	// Role is this host's part: "source" or "target".
	Role string `json:"role"`
	// Phase is which snapshot this directive is for: "base" or "delta".
	Phase string `json:"phase"`
	// Engine is the database engine: "postgres" or "redis".
	Engine string `json:"engine"`
	// VolumeName is the Egg's persistent volume name, identical on both hosts.
	VolumeName string `json:"volumeName"`
	// PutURL / GetURL are where to move the snapshot bytes. Empty means use the
	// control-plane relay endpoint (authenticated with the host token); a production
	// deploy supplies presigned object-storage URLs instead.
	PutURL string `json:"putUrl,omitempty"`
	GetURL string `json:"getUrl,omitempty"`
	// Checksum is the sha256 the target verifies against the downloaded snapshot.
	Checksum string `json:"checksum,omitempty"`
	// Quiesce (source only) means stop serving writes before the delta snapshot.
	Quiesce bool `json:"quiesce,omitempty"`
}

// MigrationCounts is the engine-specific verification measure an agent reports for
// a migration snapshot or restore, so the control plane can confirm the copy is
// complete before it cuts over. Postgres reports the table count, Redis the key
// count; rows is carried when cheap. Zero fields are omitted.
type MigrationCounts struct {
	Tables int `json:"tables,omitempty"`
	Rows   int `json:"rows,omitempty"`
	Keys   int `json:"keys,omitempty"`
}

// MigrationReport is what an agent POSTs to /api/v1/hosts/:id/migration-status to
// advance a database Egg migration it has a part in. State is one of "started",
// "uploaded" (source finished snapshot + upload), "restored", "verified" (target
// restored and self-checked) or "failed". Never carries secret values.
type MigrationReport struct {
	MigrationID string           `json:"migrationId"`
	Role        string           `json:"role"`
	Phase       string           `json:"phase,omitempty"`
	State       string           `json:"state"`
	Counts      *MigrationCounts `json:"counts,omitempty"`
	Checksum    string           `json:"checksum,omitempty"`
	SizeBytes   int64            `json:"sizeBytes,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// VolumeMount is one persistent named volume mounted into a stateful Egg. The
// volume name is stable across recreations, so a database Egg keeps its data over
// a redeploy or restart.
type VolumeMount struct {
	// Name is the stable Docker volume name for this Egg's data.
	Name string `json:"name"`
	// Path is the absolute mount point inside the container (the engine's data dir).
	Path string `json:"path"`
	// SizeGb is the billed allocation. Advisory to the agent for now (the local
	// volume driver does not hard-enforce a quota).
	SizeGb int `json:"sizeGb,omitempty"`
}

// WorkloadSecrets is the decrypted env for one workload, from the secrets
// endpoint. Plaintext values live only here and in the container; never logged.
type WorkloadSecrets struct {
	WorkloadID string            `json:"workloadId"`
	Env        map[string]string `json:"env"`
	// BuildToken is a short-lived git clone token for a private host-built repo. Used
	// only to clone the source for the build, then discarded; never injected into the
	// running container. Empty for public repos and non host-built workloads.
	BuildToken string `json:"buildToken,omitempty"`
	// ImportSourceURL is the external Postgres source connection string for an
	// in-progress data import. Used only to dump the source; never injected into the
	// running container's env. Empty when there is no active import.
	ImportSourceURL string `json:"importSourceUrl,omitempty"`
}

// SecretsResponse is the out-of-band secrets document for a host, returned by
// GET /api/v1/hosts/:id/secrets. Kept separate from the desired state on purpose.
type SecretsResponse struct {
	HostID    string            `json:"hostId"`
	Workloads []WorkloadSecrets `json:"workloads"`
}

// DesiredState is the full desired state for one host, returned by
// GET /api/v1/hosts/:id/desired-state.
//
// The Version field is a hash of the desired state. The agent stores the last
// version it applied and cheaply no-ops whilst the hash is unchanged, so it only
// reconciles when something has actually moved.
type DesiredState struct {
	// HostID is the host this desired state is for.
	HostID string `json:"hostId"`
	// Version is an opaque content hash of this desired state. The agent compares
	// it against the last applied version to decide whether to reconcile.
	Version string `json:"version"`
	// Workloads are the workloads that should be running on this host.
	Workloads []WorkloadSpec `json:"workloads"`
}

// CgroupUsage is the measured cgroup v2 usage for one container. Egress is
// reported for observability only. It is never used to cap or bill traffic,
// since egress is unmetered.
type CgroupUsage struct {
	// MemoryBytes is current memory usage in bytes (cgroup memory.current).
	MemoryBytes int64 `json:"memoryBytes"`
	// CpuUsage is CPU usage as a fraction of one core (for example 0.25),
	// derived from cpu.stat.
	CpuUsage float64 `json:"cpuUsage"`
	// EgressBytes is observed egress in bytes. Reported for insight only, never
	// metered.
	EgressBytes int64 `json:"egressBytes"`
}

// ContainerHealth is the health and usage of a single container, as reported by
// the agent.
type ContainerHealth struct {
	// WorkloadID is the workload this container belongs to.
	WorkloadID string `json:"workloadId"`
	// BlobID is the internal Blob identity of the Egg.
	BlobID string `json:"blobId"`
	// State is the runtime container state.
	State ContainerState `json:"state"`
	// Healthy reports whether the workload is currently passing its health check.
	Healthy bool `json:"healthy"`
	// RestartCount is the number of times the container has been restarted.
	RestartCount int `json:"restartCount"`
	// Usage is the measured cgroup usage.
	Usage CgroupUsage `json:"usage"`
	// DiskBytes is the on-disk usage of the Egg's persistent volume in bytes, for a
	// stateful (database) Egg. Zero/omitted for a stateless web Egg (no volume), and
	// omitted on reports where it was not measured this cycle (measured on a slower
	// cadence than the poll to keep du off the hot path).
	DiskBytes int64 `json:"diskBytes,omitempty"`
	// Logs are the container output lines produced since the last report
	// (incremental, may be empty).
	Logs []LogLine `json:"logs"`
	// Build, when set, reports host-side build progress for a bring-your-own-host Egg
	// the agent is building locally, so the control plane can move the Deployment
	// through building -> live/failed and show the build output in the Build tab. Nil
	// for a normal (non host-built) workload.
	Build *BuildReport `json:"build,omitempty"`
	// Import, when set, reports data-import progress for a database Egg the agent is
	// importing into, so the control plane can move the Import through
	// importing -> done/failed and show progress. Nil when there is no active import.
	Import *ImportReport `json:"import,omitempty"`
	// Backup, when set, reports backup capture/restore progress for a database Egg.
	// Nil when there is no active backup. See BackupReport.
	Backup *BackupReport `json:"backup,omitempty"`
}

// BuildReport is host-side build progress for a workload the agent is building
// locally. Distinct from the runtime Logs above: this drives the Deployment's build
// status and the dashboard Build tab.
type BuildReport struct {
	// DeploymentID is the deployment this build produces (from the build directive).
	DeploymentID string `json:"deploymentId"`
	// Status is the build phase: "building", "built" or "failed".
	Status string `json:"status"`
	// Log is incremental, redacted build output since the last report (may be empty).
	Log string `json:"log,omitempty"`
}

// ImportReport is data-import progress for a database Egg the agent is importing
// into, driving the Import's status and dashboard progress.
type ImportReport struct {
	// ImportID is the import this report is for (from the import directive).
	ImportID string `json:"importId"`
	// Status is the import phase: "importing", "done" or "failed".
	Status string `json:"status"`
	// Log is incremental, redacted import output since the last report (may be empty).
	Log string `json:"log,omitempty"`
}

// BackupReport is capture/restore progress for a database Egg's backup, driving the
// Backup's status. On a capture "done" it carries the durable file Ref, its size and
// checksum so the control plane can record and later restore the snapshot.
type BackupReport struct {
	// BackupID is the backup this report is for (from the backup directive).
	BackupID string `json:"backupId"`
	// Action is which directive action this report is for ("capture", "restore",
	// "upload", "store", "fetch"), so the control plane can tell them apart for the
	// same BackupID.
	Action string `json:"action,omitempty"`
	// Status is the backup phase: "running", "done" or "failed".
	Status string `json:"status"`
	// Ref, on a capture "done", is the durable host file the snapshot was written to;
	// on a store "done", the file path written inside the Vault volume.
	Ref string `json:"ref,omitempty"`
	// SizeBytes is the snapshot size on a capture "done".
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// Checksum is the snapshot sha256 on a capture "done".
	Checksum string `json:"checksum,omitempty"`
	// Log is incremental, redacted output since the last report (may be empty).
	Log string `json:"log,omitempty"`
}

// LogLine is one line of container output, shipped incrementally by the agent.
type LogLine struct {
	// Ts is the runtime timestamp of the line (RFC3339).
	Ts string `json:"ts"`
	// Stream is which stream the line came from: "stdout" or "stderr".
	Stream string `json:"stream"`
	// Text is the log text, stripped of the runtime timestamp prefix.
	Text string `json:"text"`
}

// HostStatus is the status report from the agent, sent to
// POST /api/v1/hosts/:id/status.
//
// AppliedVersion echoes back the DesiredState.Version the agent has reconciled
// to, so the control plane can tell when a host is up to date.
type HostStatus struct {
	// HostID is the host this report is for.
	HostID string `json:"hostId"`
	// AgentVersion is the version string of the reporting agent.
	AgentVersion string `json:"agentVersion"`
	// AppliedVersion is the desired-state version the agent has applied, if any.
	AppliedVersion string `json:"appliedVersion,omitempty"`
	// CpuCores is the real CPU cores of the physical box (fractional allowed),
	// detected on the host and reported on every heartbeat so a resized VPS is
	// picked up automatically. omitempty: a zero (detection miss) is not sent, so
	// it can never overwrite a known-good Host value (version-skew safety).
	CpuCores float64 `json:"cpuCores,omitempty"`
	// TotalRamMb is total host RAM in MB, agent-detected. Same omitempty rule.
	TotalRamMb int `json:"totalRamMb,omitempty"`
	// OSName and OSVersion identify the host operating system (for example "Ubuntu"
	// and "22.04"), for the operator fleet view. Omitted when not detected.
	OSName    string `json:"osName,omitempty"`
	OSVersion string `json:"osVersion,omitempty"`
	// Kernel is the running kernel release (uname -r), for spotting an unpatched box.
	Kernel string `json:"kernel,omitempty"`
	// SecurityUpdates is the count of pending OS security updates. A pointer so nil
	// (not determined) stays distinct from 0 (checked and patched); omitempty drops
	// only the nil, so a real 0 is still sent and clears a stale count.
	SecurityUpdates *int `json:"securityUpdates,omitempty"`
	// RebootRequired is true when the OS has flagged that a reboot is needed to
	// finish applying updates. Sent even when false (no omitempty), so the control
	// plane can clear a stale flag after the box has been rebooted.
	RebootRequired bool `json:"rebootRequired"`
	// RolledBack lists agent versions this node fetched, failed to run, and rolled
	// back from (quarantined). Always sent (even empty, no omitempty) so the control
	// plane can clear the alert once the node's quarantine is cleared.
	RolledBack []string `json:"rolledBack"`
	// Containers is the health of every container the agent is currently running.
	Containers []ContainerHealth `json:"containers"`
	// AgentLogs is the agent's OWN recent log output (a rolling window, not
	// incremental), so an operator can stream a node's agent logs from the console
	// without shell access. May be empty. Never contains secrets.
	AgentLogs []LogLine `json:"agentLogs,omitempty"`
}
