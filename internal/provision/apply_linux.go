//go:build linux

package provision

import (
	"bytes"
	"context"
	"encoding/json"
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

// The capability drop-in layered onto the agent unit. It grants the network capabilities
// and the setns syscall that per-Egg egress shaping (nsenter + tc) needs.
//
// SystemCallFilter is an ALLOW-LIST that MERGES (unions) across drop-ins, so a bare
// "SystemCallFilter=setns" is NOT additive in the intuitive sense: on a base unit that has
// its own filter (a current enrol) it unions harmlessly, but on an OLDER unit with no
// filter it becomes the entire allow-list, restricting the agent to ONLY setns and
// SIGSYS-killing it on the next syscall. So carry the FULL baseline here (matching what
// enrol writes) plus setns, so the drop-in is safe whether or not the base unit has a
// filter. British English. No em dashes.
const agentCapsDropIn = `# Managed by ruust-agent provision. Grants the network capabilities and the setns
# syscall that per-Egg egress shaping (nsenter + tc) needs. Carries the full syscall
# baseline (not a bare setns) so it is safe on a base unit that has no filter of its own.
# British English. No em dashes.
[Service]
AmbientCapabilities=CAP_NET_ADMIN CAP_SYS_ADMIN
SystemCallFilter=@system-service setns
SystemCallFilter=~@debug @mount @swap @reboot @raw-io @clock @cpu-emulation @obsolete
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
	if m.SSH != nil {
		if err := applySSH(ctx, o, m.SSH); err != nil {
			errs = append(errs, fmt.Errorf("ssh: %w", err))
		}
	}
	return errors.Join(errs...)
}

// applySSH installs the operator public keys and, only when a key is present and asked,
// disables password auth. It is deliberately conservative:
//   - the operator keys go in a MANAGED file added alongside each user's own
//     authorized_keys, so a key the operator placed by hand is never clobbered;
//   - password auth is disabled ONLY with a key present, so the fleet cannot be locked out;
//   - the full sshd config is VALIDATED (sshd -t) before anything is reloaded, and on a
//     validation failure the drop-in is rolled back so a later restart or reboot cannot
//     load a bad config;
//   - sshd is RELOADED, never restarted, so a live session is never dropped.
func applySSH(ctx context.Context, o Options, s *contract.SSHConfig) error {
	hasKeys := hasAnyKey(s.AuthorizedKeys)
	if s.DisablePasswordAuth && !hasKeys {
		o.Log.Warn("ssh: refusing to disable password auth with no operator key present")
	}

	keysChanged, err := writeFileIfChanged(sshKeysPath, authorizedKeysFile(s.AuthorizedKeys), 0o600)
	if err != nil {
		return fmt.Errorf("write operator keys: %w", err)
	}
	dropIn := sshDropIn(sshKeysPath, s.DisablePasswordAuth, hasKeys)
	dropChanged, err := writeFileIfChanged(sshDropInPath, dropIn, 0o644)
	if err != nil {
		return fmt.Errorf("write sshd drop-in: %w", err)
	}
	// Retire the pre-ordering-fix drop-in (50-) if it is still on disk, so only the winning
	// 10- file is left. Treat its removal as a change so we validate and reload below.
	oldRemoved := false
	if _, statErr := os.Stat(oldSSHDropInPath); statErr == nil {
		if rmErr := os.Remove(oldSSHDropInPath); rmErr != nil {
			return fmt.Errorf("remove superseded sshd drop-in: %w", rmErr)
		}
		oldRemoved = true
	}
	if !keysChanged && !dropChanged && !oldRemoved {
		return nil // already converged; sshd was validated + reloaded on the change
	}

	// Validate the whole sshd config BEFORE reloading. If our generated drop-in does not
	// validate (it should always, but be safe), roll it back so a later restart/reboot
	// cannot load a broken config and lock us out, and do not reload.
	if err := run(ctx, o, sshdBinary(), "-t"); err != nil {
		if dropChanged {
			_ = os.Remove(sshDropInPath)
		}
		return fmt.Errorf("sshd config invalid, rolled back, not reloading: %w", err)
	}

	// Reload (never restart) so live sessions survive. The unit is `ssh` on Debian/Ubuntu
	// and `sshd` on RHEL-likes; try one then the other.
	if rerr := run(ctx, o, "systemctl", "reload", "ssh"); rerr != nil {
		if rerr2 := run(ctx, o, "systemctl", "reload", "sshd"); rerr2 != nil {
			return fmt.Errorf("reload sshd: %w", rerr)
		}
	}
	// Log the outcome as a fixed message chosen by the branch, so no value derived from
	// the SSH config flows into the log sink (keeps the CodeQL clear-text-logging check
	// happy; a boolean flag is not sensitive, but this is tidier anyway).
	if s.DisablePasswordAuth && hasKeys {
		o.Log.Info("ssh hardening applied: key-only login (password auth disabled)",
			"operatorKeys", len(s.AuthorizedKeys))
	} else {
		o.Log.Info("ssh hardening applied: operator keys installed (password auth unchanged)",
			"operatorKeys", len(s.AuthorizedKeys))
	}
	return nil
}

// sshdBinary resolves the sshd path (not always on the agent's PATH), for `sshd -t`.
func sshdBinary() string {
	for _, p := range []string{"/usr/sbin/sshd", "/sbin/sshd", "/usr/bin/sshd"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "sshd"
}

// writeSSHState probes the host's EFFECTIVE sshd state and the managed operator-key count
// and writes them to sshStatePath, so the unprivileged main agent loop (which cannot run
// sshd -T or read the 0600 key file) can report them for the console SSH health badge. It
// uses `sshd -T`, which merges every drop-in, so it reflects what sshd ACTUALLY resolves
// (catching a cloud-init file that re-enables passwords, not just what we wrote). Root only.
// Best effort: the caller logs and carries on, so a probe failure never fails provisioning.
func writeSSHState(o Options) error {
	out, err := exec.CommandContext(context.Background(), sshdBinary(), "-T").CombinedOutput()
	if err != nil {
		return fmt.Errorf("sshd -T: %w: %s", err, strings.TrimSpace(string(out)))
	}
	passwordAuthEnabled := true
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.ToLower(strings.TrimSpace(line)))
		if len(fields) == 2 && fields[0] == "passwordauthentication" {
			passwordAuthEnabled = fields[1] == "yes"
			break
		}
	}

	// Count the well-formed managed operator keys (skip the header comment and blanks).
	keyCount := 0
	if b, rerr := os.ReadFile(sshKeysPath); rerr == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if t := strings.TrimSpace(line); t != "" && !strings.HasPrefix(t, "#") {
				keyCount++
			}
		}
	}

	data, err := json.Marshal(sshState{PasswordAuthEnabled: passwordAuthEnabled, OperatorKeyCount: keyCount})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(sshStatePath), 0o755); err != nil {
		return err
	}
	// 0644 so the unprivileged main agent (user ruust) can read it.
	return os.WriteFile(sshStatePath, append(data, '\n'), 0o644)
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
