//go:build !linux

package provision

import (
	"context"

	"github.com/RuustRun/agent/internal/contract"
)

// Apply is a no-op off Linux: host provisioning (iptables, apt, systemd) is
// Linux-only. This keeps the agent building and vetting on a developer's machine.
func Apply(_ context.Context, o Options, _ contract.ProvisioningManifest) error {
	o.Log.Info("host provisioning is a Linux-only operation; skipping on this platform")
	return nil
}
