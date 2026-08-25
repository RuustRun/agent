# Ruust agent: a single static Go binary, built for scratch/distroless.
#
# The agent talks to the host's Docker daemon over the mounted socket and needs
# no inbound ports. It is normally run under systemd on the host (see README),
# but this image exists for parity and for build hosts that prefer containers.
#
# Multi-stage build. The final image is FROM scratch with only the static binary
# and CA certificates, so it is tiny and has no shell or package manager.

# --- build stage ---------------------------------------------------------------
FROM golang:1.22-alpine AS build

# git is needed for module fetches that resolve via VCS.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache modules first for faster incremental builds.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Fully static, stripped binary. CGO off so there is no libc dependency, which
# is what lets us ship FROM scratch.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS=-trimpath \
    go build -ldflags "-s -w -X main.agentVersion=${VERSION}" \
    -o /out/ruust-agent ./cmd/agent

# --- runtime stage -------------------------------------------------------------
FROM scratch

# CA roots so the agent can reach the control plane over HTTPS.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ruust-agent /ruust-agent

# The agent reads configuration from the environment and a token file. Mount the
# Docker socket and the token at run time, for example:
#   docker run --rm \
#     -e RUUST_CONTROL_PLANE_URL=https://cp.eu-west.ruust.internal \
#     -e RUUST_HOST_ID=host-123 -e RUUST_REGION=eu-west \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v /etc/ruust/host.token:/etc/ruust/host.token:ro \
#     ghcr.io/thenightproject/ruust-agent:latest
ENTRYPOINT ["/ruust-agent"]
