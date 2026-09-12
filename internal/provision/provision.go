// Package provision is the host-side, pull-based OS reconciler. It is run by a small
// root systemd service (ruust-provision.service) on a timer, separately from the
// unprivileged main agent loop, and converges the privileged host substrate the agent
// itself cannot change: the egg-egress firewall, required OS packages, and the agent
// unit's network capabilities.
//
// Like everything else in Ruust it is pull-based: the host fetches a versioned
// provisioning manifest from the control plane (which never dials the host) and makes
// the box match. A generation hash means an unchanged manifest is a cheap no-op, so the
// timer can run often without churn. This is what lets a firewall or fair-use change
// land on every host with no re-enrol.
//
// The actual OS mutation is Linux-only (iptables, apt, systemd); on other platforms
// Apply is a no-op so the agent still builds and vets on a developer's machine.
//
// British English throughout. No em dashes.
package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/RuustRun/agent/internal/contract"
)

// markerPath records the last provisioning state the host applied, as
// "<generation>\n<agentVersion>". The reconciler re-applies when either changes (a new
// manifest, or a new agent binary whose embedded templates may differ), and otherwise
// no-ops.
const markerPath = "/var/lib/ruust/provision-gen"

// Options are the inputs the provision reconciler needs. They mirror the main agent's
// configuration, resolved from the same environment and token file.
type Options struct {
	ControlPlaneURL string
	HostID          string
	Token           string
	AgentVersion    string
	HTTP            *http.Client
	Log             *slog.Logger
}

// Run fetches the provisioning manifest and, when it differs from what this host last
// applied, converges the host to match. It is safe to run on a timer: an unchanged
// manifest returns quickly without touching the system.
func Run(ctx context.Context, o Options) error {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.HTTP == nil {
		o.HTTP = &http.Client{}
	}

	m, err := fetchManifest(ctx, o)
	if err != nil {
		return fmt.Errorf("fetching provisioning manifest: %w", err)
	}

	want := o.AgentVersion + "\n" + m.Generation
	if have, _ := os.ReadFile(markerPath); strings.TrimSpace(string(have)) == strings.TrimSpace(want) {
		o.Log.Info("host provisioning already current", "generation", m.Generation)
		return nil
	}

	o.Log.Info("converging host provisioning", "generation", m.Generation)
	if err := Apply(ctx, o, m); err != nil {
		return fmt.Errorf("applying provisioning: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(markerPath), 0o755); err != nil {
		return fmt.Errorf("creating state dir: %w", err)
	}
	if err := os.WriteFile(markerPath, []byte(want+"\n"), 0o644); err != nil {
		return fmt.Errorf("recording provisioning generation: %w", err)
	}
	o.Log.Info("host provisioning converged", "generation", m.Generation)
	return nil
}

// fetchManifest performs GET /api/v1/hosts/:id/provisioning with the host token.
func fetchManifest(ctx context.Context, o Options) (contract.ProvisioningManifest, error) {
	url := fmt.Sprintf("%s/api/%s/hosts/%s/provisioning",
		strings.TrimRight(o.ControlPlaneURL, "/"), contract.APIVersion, o.HostID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return contract.ProvisioningManifest{}, err
	}
	req.Header.Set("Authorization", "Bearer "+o.Token)
	req.Header.Set("User-Agent", "ruust-agent/"+o.AgentVersion)

	resp, err := o.HTTP.Do(req)
	if err != nil {
		return contract.ProvisioningManifest{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return contract.ProvisioningManifest{}, fmt.Errorf("provisioning returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var m contract.ProvisioningManifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return contract.ProvisioningManifest{}, fmt.Errorf("decoding manifest: %w", err)
	}
	if m.Generation == "" {
		return contract.ProvisioningManifest{}, fmt.Errorf("manifest has no generation")
	}
	return m, nil
}
