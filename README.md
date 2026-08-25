# Ruust agent

The open-source host agent for [**Ruust**](https://ruust.run), the flat-priced
container hosting platform by **The Night Project**. It is a single static Go binary
you run on any Linux box you own (bring your own host). Your Ruust panel then turns
that hardware into a proper platform: builds, HTTPS, ingress, private networking and
deploys, all managed, whilst the compute, the network and the data stay yours.

You can read every line that runs on your host before you decide to trust it. That is
the point of this repository.

## Trust model

- **Pull-based.** The agent polls your control plane for desired state and converges
  to it on its own schedule. The control plane never opens a connection to your box:
  no inbound ports, no SSH from us, nothing to expose.
- **Holds only a scoped host token.** Nothing panel-private is baked into the binary.
  Environment secrets arrive encrypted in transit and are never written to logs, and
  neither are environment variable values or decrypted secrets.
- **Containers keep running when the control plane is down.** A fetch failure is
  logged and retried on the next tick; nothing is torn down. Every operation is
  idempotent and safely retryable.
- **Self-healing and self-updating with rollback.** A panicking cycle is recovered
  and retried. The agent updates itself when the control plane serves a newer build,
  keeps the previous binary, and rolls back automatically if the new one fails to
  stay up (quarantining the bad version so it is not re-fetched).

## What it does

Reconciliation is pull-based. The control plane stores desired state; the agent polls
it and converges. On each tick (a base interval plus random jitter so a fleet does not
stampede) the agent:

1. Reads its host token and calls `GET /api/v1/hosts/:id/desired-state`.
2. Short-circuits cheaply whilst the desired-state `version` hash is unchanged, doing
   no container work and only refreshing its status heartbeat.
3. Otherwise diffs desired against actual (it queries Docker for containers carrying
   our label prefix, and nothing else) and converges: start missing, stop extra,
   restart unhealthy or crashed, and roll containers whose image or spec has moved.
4. Reports actual state back via `POST /api/v1/hosts/:id/status` with per-container
   health, restart counts and cgroup v2 resource usage.

## Container hardening

Every container the agent starts gets hard limits on every axis: memory and CPU from
the Egg's size, a PID cap, a read-only root filesystem where the image allows it, all
Linux capabilities dropped (or a minimal set for images that need them), and
`no-new-privileges`. Co-located Eggs share a bridge with inter-container communication
disabled, so they cannot reach one another unless explicitly peered.

There is deliberately no egress or bandwidth cap: egress is unmetered. Bytes are still
reported for observability.

## Configuration

All configuration is via the environment, with the token read from disk.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RUUST_CONTROL_PLANE_URL` | yes | | Base URL of your control plane |
| `RUUST_HOST_ID` | yes | | This host's id |
| `RUUST_HOST_TOKEN_FILE` | no | `/etc/ruust/host.token` | Path to the token file |
| `RUUST_REGION` | no | | Region slug, for structured logging |
| `RUUST_POLL_INTERVAL` | no | `10s` | Base poll interval |
| `RUUST_POLL_JITTER` | no | `3s` | Maximum extra random delay per tick |

## Build

```sh
go build ./cmd/agent
```

A fully static release binary:

```sh
CGO_ENABLED=0 GOOS=linux go build \
  -ldflags "-s -w -X main.agentVersion=$(git describe --tags --always)" \
  -o ruust-agent-linux-amd64 ./cmd/agent
```

Release binaries (`ruust-agent-linux-amd64`, `ruust-agent-linux-arm64`) are built and
published to GitHub Releases by the workflow in `.github/workflows`.

## Running under systemd

See [`ruust-agent.service`](./ruust-agent.service). The unit uses `Restart=always`
with `StartLimitIntervalSec=0`, so a control plane outage never stops the agent.

## Layout

```
cmd/agent            poll loop, HTTP client, status report, self-update + rollback
internal/contract    the wire types (JSON contract spoken with the control plane)
internal/docker      Docker Engine SDK behind an interface
internal/reconcile   diff + converge logic, unit tested
internal/ingress     on-host Caddy ingress configuration
internal/hostcap     host capacity detection (CPU, RAM)
internal/hostfacts   OS and patch facts (for the operator fleet view)
internal/cgroups     cgroup v2 stats read from the filesystem
```

## Licence

Apache-2.0. See [LICENSE](LICENSE).

British English throughout. No em dashes.
