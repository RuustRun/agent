//go:build linux

package provision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	_ "embed"

	"github.com/RuustRun/agent/internal/contract"
)

//go:embed assets/egg-egress.sh
var eggEgressScript []byte

//go:embed assets/ruust-egg-firewall.service
var eggFirewallUnit []byte

const (
	firewallScriptPath = "/opt/ruust/firewall/egg-egress.sh"
	firewallEnvPath    = "/etc/ruust/egg-firewall.env"
	firewallUnitPath   = "/etc/systemd/system/ruust-egg-firewall.service"
	firewallUnitName   = "ruust-egg-firewall.service"

	agentUnitName   = "ruust-agent.service"
	agentDropInDir  = "/etc/systemd/system/ruust-agent.service.d"
	agentDropInPath = "/etc/systemd/system/ruust-agent.service.d/10-ruust-network-caps.conf"
)

// The capability drop-in layered onto the agent unit. A drop-in is additive, so it
// grants the network capabilities and the setns syscall egress shaping needs without
// rewriting (and risking) the base unit the enrol script wrote.
const agentCapsDropIn = `# Managed by ruust-agent provision. Grants the network capabilities and the setns
# syscall that per-Egg egress shaping (nsenter + tc) needs. Additive drop-in, so it
# never clobbers the base unit. British English. No em dashes.
[Service]
AmbientCapabilities=CAP_NET_ADMIN CAP_SYS_ADMIN
SystemCallFilter=setns
`

// Apply converges the host to the manifest. Each step is independent and idempotent;
// errors are collected so a failure in one does not block the others, and the caller
// only records the generation (stopping retries) when everything succeeded.
func Apply(ctx context.Context, o Options, m contract.ProvisioningManifest) error {
	var errs []error

	if err := applyFirewall(ctx, o, m.Firewall); err != nil {
		errs = append(errs, fmt.Errorf("firewall: %w", err))
	}
	if err := ensurePackages(ctx, o, m.Packages); err != nil {
		errs = append(errs, fmt.Errorf("packages: %w", err))
	}
	if m.AgentNetworkCaps {
		if err := ensureAgentCaps(ctx, o); err != nil {
			errs = append(errs, fmt.Errorf("agent caps: %w", err))
		}
	}
	return errors.Join(errs...)
}

// applyFirewall writes the egg-egress script, its blocked-ports env and the oneshot
// unit, then (re)starts it so a changed port list takes effect.
func applyFirewall(ctx context.Context, o Options, fw *contract.FirewallConfig) error {
	ports := []int{25}
	if fw != nil && len(fw.BlockedOutboundPorts) > 0 {
		ports = fw.BlockedOutboundPorts
	}
	// The egg-egress.sh reads RUUST_BLOCKED_SMTP_PORTS as a whitespace-separated list.
	parts := make([]string, 0, len(ports))
	for _, p := range ports {
		parts = append(parts, strconv.Itoa(p))
	}
	env := fmt.Sprintf("RUUST_BLOCKED_SMTP_PORTS=%q\n", strings.Join(parts, " "))

	if _, err := writeFileIfChanged(firewallScriptPath, eggEgressScript, 0o755); err != nil {
		return err
	}
	if _, err := writeFileIfChanged(firewallEnvPath, []byte(env), 0o644); err != nil {
		return err
	}
	if _, err := writeFileIfChanged(firewallUnitPath, eggFirewallUnit, 0o644); err != nil {
		return err
	}
	if err := run(ctx, o, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if err := run(ctx, o, "systemctl", "enable", firewallUnitName); err != nil {
		return err
	}
	// restart (not just start) so a new blocked-ports list is re-applied on a oneshot.
	return run(ctx, o, "systemctl", "restart", firewallUnitName)
}

// ensurePackages installs any of the named packages that are not already present. It
// runs apt-get only when something is missing, so a converged host pays nothing.
func ensurePackages(ctx context.Context, o Options, pkgs []string) error {
	missing := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		if p == "" {
			continue
		}
		if err := exec.CommandContext(ctx, "dpkg", "-s", p).Run(); err != nil {
			missing = append(missing, p)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	o.Log.Info("installing missing packages", "packages", missing)
	if err := run(ctx, o, "apt-get", "update"); err != nil {
		return err
	}
	args := append([]string{"install", "-y"}, missing...)
	cmd := exec.CommandContext(ctx, "apt-get", args...)
	cmd.Env = append(os.Environ(), "DEBIAN_FRONTEND=noninteractive")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("apt-get install: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ensureAgentCaps lays down the capability drop-in for the agent unit and restarts the
// agent only when the drop-in was newly written or changed, so a converged host never
// bounces the agent.
func ensureAgentCaps(ctx context.Context, o Options) error {
	if err := os.MkdirAll(agentDropInDir, 0o755); err != nil {
		return err
	}
	changed, err := writeFileIfChanged(agentDropInPath, []byte(agentCapsDropIn), 0o644)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	o.Log.Info("granting the agent network capabilities; restarting it once")
	if err := run(ctx, o, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	return run(ctx, o, "systemctl", "restart", agentUnitName)
}

// writeFileIfChanged writes content to path only when it differs from what is already
// there, creating the parent directory. Returns whether it wrote. Avoids needless
// churn (and, for the agent drop-in, a needless restart).
func writeFileIfChanged(path string, content []byte, mode os.FileMode) (bool, error) {
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, content) {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		return false, err
	}
	return true, nil
}

// run executes a command, returning any error with its combined output for diagnosis.
func run(ctx context.Context, o Options, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
