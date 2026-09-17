package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/RuustRun/agent/internal/provision"
)

// runProvision is the `ruust-agent provision` subcommand: the root host-provisioning
// reconciler, run by ruust-provision.service on a timer, separately from the main
// (unprivileged) agent loop. It reads the same configuration and host token, fetches
// the provisioning manifest and converges the host's privileged substrate (egg-egress
// firewall, packages, agent capabilities). It needs no Docker. Returns a process exit
// code.
func runProvision(logger *slog.Logger) int {
	// Never run privileged host provisioning with a self-updated binary that has not yet
	// proven itself. This timer runs as root every couple of minutes, outside the main
	// agent's crash-loop probation window, so without this gate a bad-but-boot-healthy
	// build would provision the host as root before probation could roll it back. Defer
	// (a clean no-op) until the main agent commits or rolls back the update.
	if isOnProbation() {
		logger.Info("agent is on post-update probation; deferring host provisioning until the new build is proven healthy")
		return 0
	}

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := logger.With("hostId", cfg.hostID, "agentVersion", agentVersion)
	if err := provision.Run(ctx, provision.Options{
		ControlPlaneURL: cfg.controlPlaneURL,
		HostID:          cfg.hostID,
		Token:           cfg.hostToken,
		AgentVersion:    agentVersion,
		HTTP:            &http.Client{Timeout: 30 * time.Second},
		Log:             log,
	}); err != nil {
		log.Error("host provisioning failed", "err", err)
		return 1
	}
	return 0
}
