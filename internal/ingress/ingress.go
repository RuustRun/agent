// Package ingress reconciles the local Caddy ingress to match desired state.
//
// The agent is the only thing that talks to Caddy: it pushes the full runtime
// config (per-Egg routes and the on-demand TLS policy) through Caddy's admin API,
// and it serves the "ask" endpoint that Caddy calls before issuing a certificate
// for any hostname. The ask endpoint says yes only for hostnames currently in the
// desired state, so a certificate is never minted for a domain that is not ours
// to serve. Nothing configures Caddy by rewriting files and nothing reaches in
// from the control plane; this mirrors the pull-based model of the rest of the
// agent.
//
// Locally Caddy uses its INTERNAL CA (its own issuer), so certificates are real
// TLS but need no Let's Encrypt, which cannot issue for these test hostnames.
package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
)

// Route is one Egg's ingress: the hostnames it serves and the host port its
// container is published on. The upstream host (how Caddy reaches that port) is
// configured on the Reconciler.
type Route struct {
	Hostnames    []string
	UpstreamPort int
}

// Reconciler owns the connection to Caddy's admin API and the ask allow-list.
type Reconciler struct {
	adminURL     string // e.g. http://localhost:2019
	upstreamHost string // how Caddy reaches Egg containers, e.g. host.docker.internal
	askEndpoint  string // the ask URL Caddy calls, e.g. http://host.docker.internal:9700/ask
	http         *http.Client
	log          *slog.Logger

	// localTLS chooses the certificate issuer. False (the default, production) uses
	// ACME (Let's Encrypt) for real, browser-trusted certs on the public Egg
	// hostnames. True uses Caddy's internal self-signed CA, for a local single-box
	// setup where Let's Encrypt cannot issue for the test hostnames.
	localTLS bool

	mu       sync.RWMutex
	allowed  map[string]bool
	lastHash string
}

// New builds a Reconciler. adminURL is the Caddy admin API base; upstreamHost is
// how Caddy dials Egg containers; askEndpoint is the URL (reachable from Caddy)
// of this agent's ask handler; localTLS selects the internal CA for local dev
// instead of Let's Encrypt.
func New(adminURL, upstreamHost, askEndpoint string, localTLS bool, log *slog.Logger) *Reconciler {
	return &Reconciler{
		adminURL:     adminURL,
		upstreamHost: upstreamHost,
		askEndpoint:  askEndpoint,
		localTLS:     localTLS,
		http:         &http.Client{},
		log:          log,
		allowed:      map[string]bool{},
	}
}

// Reconcile pushes the config for the given routes to Caddy if it has changed,
// and updates the ask allow-list. A no-change reconcile is a cheap hash compare.
func (r *Reconciler) Reconcile(ctx context.Context, routes []Route) error {
	cfg, allowed := r.build(routes)
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal caddy config: %w", err)
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])

	// The allow-list must always track the latest desired state, even when the
	// Caddy config itself is unchanged, so update it first.
	r.mu.Lock()
	r.allowed = allowed
	unchanged := hash == r.lastHash
	r.mu.Unlock()
	if unchanged {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.adminURL+"/load", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.http.Do(req)
	if err != nil {
		return fmt.Errorf("posting caddy config: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("caddy load returned %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}

	r.mu.Lock()
	r.lastHash = hash
	r.mu.Unlock()
	r.log.Info("ingress reconciled", "routes", len(routes), "hostnames", len(allowed))
	return nil
}

// Allowed reports whether a hostname is currently permitted a certificate.
func (r *Reconciler) Allowed(host string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.allowed[host]
}

// AskHandler answers Caddy's on-demand TLS ask: 200 for an allowed hostname,
// 403 otherwise. Caddy passes the hostname as ?domain=.
func (r *Reconciler) AskHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		domain := req.URL.Query().Get("domain")
		if domain != "" && r.Allowed(domain) {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Do not log the domain at info; a probe flood would be noisy and it is
		// attacker-controlled input.
		w.WriteHeader(http.StatusForbidden)
	}
}

// build turns routes into a full Caddy config and the ask allow-list. The config
// is deterministic (hostnames and routes sorted) so an unchanged desired state
// hashes identically and skips the reload.
func (r *Reconciler) build(routes []Route) (map[string]any, map[string]bool) {
	allowed := map[string]bool{}
	httpRoutes := make([]map[string]any, 0, len(routes))

	// Sort routes by their first hostname for a stable config hash.
	sorted := append([]Route(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool { return firstHost(sorted[i]) < firstHost(sorted[j]) })

	for _, rt := range sorted {
		if rt.UpstreamPort <= 0 || len(rt.Hostnames) == 0 {
			continue
		}
		hosts := append([]string(nil), rt.Hostnames...)
		sort.Strings(hosts)
		for _, h := range hosts {
			allowed[h] = true
		}
		httpRoutes = append(httpRoutes, map[string]any{
			"match": []map[string]any{{"host": hosts}},
			"handle": []map[string]any{{
				"handler": "reverse_proxy",
				"upstreams": []map[string]any{
					{"dial": fmt.Sprintf("%s:%d", r.upstreamHost, rt.UpstreamPort)},
				},
			}},
		})
	}

	// On-demand issuance is gated by the ask endpoint. In production the issuer is
	// ACME (Let's Encrypt), so Egg hostnames get real, browser-trusted certs; the
	// internal self-signed CA is used only for a local single-box setup.
	policy := map[string]any{"on_demand": true}
	if r.localTLS {
		policy["issuers"] = []map[string]any{{"module": "internal"}}
	}

	cfg := map[string]any{
		"admin": map[string]any{"listen": "0.0.0.0:2019"},
		"apps": map[string]any{
			"tls": map[string]any{
				"automation": map[string]any{
					"policies": []map[string]any{policy},
					"on_demand": map[string]any{
						"permission": map[string]any{"module": "http", "endpoint": r.askEndpoint},
					},
				},
			},
			"http": map[string]any{
				"servers": map[string]any{
					"ruust": map[string]any{
						"listen": []string{":443"},
						"routes": httpRoutes,
					},
				},
			},
		},
	}
	return cfg, allowed
}

func firstHost(r Route) string {
	if len(r.Hostnames) == 0 {
		return ""
	}
	m := r.Hostnames[0]
	for _, h := range r.Hostnames {
		if h < m {
			m = h
		}
	}
	return m
}
