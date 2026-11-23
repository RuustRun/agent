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
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RuustRun/agent/internal/contract"
)

// Route is one Egg's ingress: the hostnames it serves and the host ports its
// replicas are published on. Caddy load-balances across all of an Egg's live
// replica ports (round-robin by default), so a multi-replica Egg spreads traffic
// and survives a single replica dying. The upstream host (how Caddy reaches those
// ports) is configured on the Reconciler.
type Route struct {
	Hostnames []string
	// UpstreamPorts are the healthy replicas' host ports. Empty means the Egg has no
	// healthy replica right now (booting, crashed, stopped): its hostnames are answered
	// with a holding page instead of being dropped from ingress.
	UpstreamPorts []int
	// ProxiedHostnames is the subset of Hostnames served with Caddy's internal self-signed
	// cert, because the customer fronts them with a TLS-terminating proxy (Cloudflare orange
	// cloud in "Full" mode) that ACME cannot pass a challenge through.
	ProxiedHostnames []string
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
	proxied  map[string]bool // hostnames served with the internal cert (behind a proxy)
	lastHash string
	// lastVerified is when we last confirmed Caddy still holds our config (see Reconcile).
	lastVerified time.Time
}

// serverID tags our HTTP server in the pushed config so we can ask Caddy whether it still
// has it. A Caddy that restarted on its own loads its bootstrap config, which has no such
// object, and would otherwise serve nothing until our desired state next changed.
const serverID = "ruust-ingress"

// configCheckInterval bounds how often an unchanged reconcile asks Caddy whether it still
// holds our config. One cheap loopback GET at most this often.
const configCheckInterval = 30 * time.Second

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
		proxied:      map[string]bool{},
	}
}

// Reconcile pushes the config for the given routes to Caddy if it has changed,
// and updates the ask allow-list. A no-change reconcile is a cheap hash compare.
func (r *Reconciler) Reconcile(ctx context.Context, routes []Route, limits *contract.IngressConfig) error {
	cfg, allowed, proxied := r.build(routes, limits)
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal caddy config: %w", err)
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])

	// The allow-list (and the proxied set the cert probe reads) must always track the latest
	// desired state, even when the Caddy config itself is unchanged, so update them first.
	r.mu.Lock()
	r.allowed = allowed
	r.proxied = proxied
	unchanged := hash == r.lastHash
	due := time.Since(r.lastVerified) >= configCheckInterval
	r.mu.Unlock()
	if unchanged {
		if !due {
			return nil
		}
		// Our last push is only still live if Caddy has not restarted since. If it has, it is
		// running its bootstrap config and serving no Egg, so push again. When Caddy cannot be
		// asked (admin down) there is nothing to push to yet; the next check retries.
		present, err := r.configPresent(ctx)
		r.mu.Lock()
		r.lastVerified = time.Now()
		r.mu.Unlock()
		if err != nil || present {
			return nil
		}
		r.log.Warn("ingress config missing from caddy, re-pushing (caddy restarted?)")
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
	r.lastVerified = time.Now()
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
// hashes identically and skips the reload. limits applies the operator's ingress
// hardening (timeouts and a request-body cap); nil (an older control plane) leaves
// Caddy at its defaults.
func (r *Reconciler) build(routes []Route, limits *contract.IngressConfig) (map[string]any, map[string]bool, map[string]bool) {
	allowed := map[string]bool{}
	proxied := map[string]bool{}
	httpRoutes := make([]map[string]any, 0, len(routes))

	// Sort routes by their first hostname for a stable config hash.
	sorted := append([]Route(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool { return firstHost(sorted[i]) < firstHost(sorted[j]) })

	for _, rt := range sorted {
		if len(rt.Hostnames) == 0 {
			continue
		}
		// One upstream per live replica port, sorted for a stable config hash.
		ports := make([]int, 0, len(rt.UpstreamPorts))
		for _, p := range rt.UpstreamPorts {
			if p > 0 {
				ports = append(ports, p)
			}
		}
		hosts := append([]string(nil), rt.Hostnames...)
		sort.Strings(hosts)
		for _, h := range hosts {
			allowed[h] = true
		}
		for _, h := range rt.ProxiedHostnames {
			proxied[h] = true
		}
		if len(ports) == 0 {
			// No healthy replica: keep serving TLS for the hostnames (so the certificate
			// stays issued and a fronting proxy gets a real answer, not a handshake failure)
			// and say plainly that the site is briefly unavailable.
			httpRoutes = append(httpRoutes, map[string]any{
				"match":  []map[string]any{{"host": hosts}},
				"handle": []map[string]any{holdingPage()},
			})
			continue
		}
		sort.Ints(ports)
		upstreams := make([]map[string]any, 0, len(ports))
		for _, p := range ports {
			upstreams = append(upstreams, map[string]any{"dial": fmt.Sprintf("%s:%d", r.upstreamHost, p)})
		}
		proxy := map[string]any{
			"handler":   "reverse_proxy",
			"upstreams": upstreams,
		}
		// Bound how long Caddy waits to dial a backend, so a hung Egg does not tie up
		// the ingress waiting to connect.
		if limits != nil && limits.DialTimeout != "" {
			proxy["transport"] = map[string]any{
				"protocol":     "http",
				"dial_timeout": limits.DialTimeout,
			}
		}
		handlers := make([]map[string]any, 0, 2)
		// Cap the request body before it reaches the backend, so a huge upload cannot
		// be used to exhaust a node. Runs first in the chain.
		if limits != nil && limits.MaxBodyBytes > 0 {
			handlers = append(handlers, map[string]any{
				"handler":  "request_body",
				"max_size": limits.MaxBodyBytes,
			})
		}
		handlers = append(handlers, proxy)
		httpRoutes = append(httpRoutes, map[string]any{
			"match":  []map[string]any{{"host": hosts}},
			"handle": handlers,
		})
	}

	// On-demand issuance is gated by the ask endpoint. Proxied hostnames get a policy of their
	// own that uses the INTERNAL self-signed CA: the customer fronts them with a proxy that
	// terminates public TLS (Cloudflare orange cloud, SSL mode "Full"), which cannot pass the
	// ACME challenge to this host, so a real Let's Encrypt cert can never issue. The default
	// policy issues ACME (Let's Encrypt) in production for direct hostnames; the internal CA is
	// the default only for a local single-box setup.
	policies := make([]map[string]any, 0, 2)
	if len(proxied) > 0 {
		subs := make([]string, 0, len(proxied))
		for h := range proxied {
			subs = append(subs, h)
		}
		sort.Strings(subs)
		policies = append(policies, map[string]any{
			"subjects":  subs,
			"issuers":   []map[string]any{{"module": "internal"}},
			"on_demand": true,
		})
	}
	defaultPolicy := map[string]any{"on_demand": true}
	if r.localTLS {
		defaultPolicy["issuers"] = []map[string]any{{"module": "internal"}}
	}
	policies = append(policies, defaultPolicy)

	cfg := map[string]any{
		// The admin API MUST stay bound to loopback. The agent reaches it over
		// http://localhost:2019 (adminURL), and it is UNAUTHENTICATED: anyone who can
		// reach it has full control of ingress (reroute any Egg hostname, drive TLS,
		// proxy to loopback-only on-box services). Caddy's /load replaces the admin
		// listener with whatever this config says, so binding it to 0.0.0.0 here would
		// silently override the loopback pin set at startup and expose it on the public
		// interface. Keep it 127.0.0.1.
		"admin": map[string]any{"listen": "127.0.0.1:2019"},
		"apps": map[string]any{
			"tls": map[string]any{
				"automation": map[string]any{
					"policies": policies,
					"on_demand": map[string]any{
						"permission": map[string]any{"module": "http", "endpoint": r.askEndpoint},
					},
				},
			},
			"http": map[string]any{
				"servers": map[string]any{
					"ruust": ruustServer(httpRoutes, limits),
				},
			},
		},
	}
	return cfg, allowed, proxied
}

// ruustServer builds the Caddy HTTP server block, applying the operator's timeouts
// when present. The read and idle timeouts are the slowloris defence: a client that
// dribbles a request or holds an idle connection is cut off. Empty strings leave a
// timeout at Caddy's default.
func ruustServer(httpRoutes []map[string]any, limits *contract.IngressConfig) map[string]any {
	server := map[string]any{
		"@id":    serverID,
		"listen": []string{":443"},
		"routes": httpRoutes,
	}
	if limits != nil {
		// Caddy v2 carries these as direct fields on the server, there is NO "timeouts"
		// object (an earlier version used one and Caddy rejected the whole config with a
		// 400 "unknown field timeouts", freezing ingress). read_timeout bounds reading the
		// whole request, read_header_timeout the headers (the slowloris defence); write and
		// idle bound the response and the keep-alive. Durations are Caddy duration strings.
		if limits.ReadTimeout != "" {
			server["read_timeout"] = limits.ReadTimeout
			server["read_header_timeout"] = limits.ReadTimeout
		}
		if limits.WriteTimeout != "" {
			server["write_timeout"] = limits.WriteTimeout
		}
		if limits.IdleTimeout != "" {
			server["idle_timeout"] = limits.IdleTimeout
		}
	}
	return server
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

// Health probes the local Caddy for a heartbeat report: whether the admin API is
// reachable, whether the public HTTPS port is listening, and each served hostname's
// certificate state. Best-effort: every probe degrades to a negative/pending result on
// failure rather than erroring, so a heartbeat is never blocked by ingress health.
func (r *Reconciler) Health(ctx context.Context) contract.IngressHealth {
	h := contract.IngressHealth{
		Ready:     r.adminReachable(ctx),
		Listening: dialable("127.0.0.1:443"),
	}
	r.mu.RLock()
	hosts := make([]string, 0, len(r.allowed))
	for hn := range r.allowed {
		hosts = append(hosts, hn)
	}
	r.mu.RUnlock()
	sort.Strings(hosts)
	for _, hn := range hosts {
		h.Certs = append(h.Certs, contract.CertStatusReport{Hostname: hn, Status: r.probeCert(ctx, hn)})
	}
	return h
}

// configPresent asks Caddy whether it still holds our pushed config, by the server's @id.
// An error means Caddy could not be asked (or answered oddly), not that the config is gone.
func (r *Reconciler) configPresent(ctx context.Context) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.adminURL+"/id/"+serverID, nil)
	if err != nil {
		return false, err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode < 300:
		return true, nil
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("caddy id lookup returned %d", resp.StatusCode)
	}
}

// adminReachable is true when Caddy's admin API answers, which also proves our last
// config push had somewhere to land.
func (r *Reconciler) adminReachable(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.adminURL+"/config/", nil)
	if err != nil {
		return false
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode < 300
}

// dialable reports whether a TCP connection to addr succeeds within a short timeout.
func dialable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// probeCert does a local TLS handshake with SNI = hostname and inspects the served leaf
// certificate. A cert that covers the hostname (and, in production, is not Caddy's
// internal CA) is "issued"; anything else is "pending". The probe legitimately triggers
// on-demand issuance (the ask endpoint already allows the hostname), so a cert gets
// minted proactively rather than waiting for the first real user request. We skip
// verification because we only inspect the cert, we do not trust the connection.
func (r *Reconciler) probeCert(ctx context.Context, hostname string) string {
	dctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	d := &tls.Dialer{Config: &tls.Config{ServerName: hostname, InsecureSkipVerify: true}} //nolint:gosec // inspection only
	conn, err := d.DialContext(dctx, "tcp", "127.0.0.1:443")
	if err != nil {
		return "pending"
	}
	defer func() { _ = conn.Close() }()
	tconn, ok := conn.(*tls.Conn)
	if !ok {
		return "pending"
	}
	certs := tconn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return "pending"
	}
	leaf := certs[0]
	if leaf.VerifyHostname(hostname) != nil {
		return "pending" // the served cert does not (yet) cover this hostname
	}
	// In production a DIRECT hostname expects a real (Let's Encrypt) cert; an internal-CA cert
	// means on-demand ACME has not issued yet. A PROXIED hostname (served behind a TLS proxy)
	// and the local single-box setup both expect the internal CA, so that counts as issued.
	r.mu.RLock()
	proxiedHost := r.proxied[hostname]
	r.mu.RUnlock()
	if !r.localTLS && !proxiedHost && strings.Contains(leaf.Issuer.CommonName, "Caddy Local Authority") {
		return "pending"
	}
	return "issued"
}

// holdingPageHTML is what a visitor sees while an Egg has no healthy replica. It belongs to
// the customer's site, not to us, so it is unbranded and says only what is true: the site
// is briefly unavailable and will be back.
const holdingPageHTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Back shortly</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;font:16px/1.5 system-ui,sans-serif;color:#222;background:#fafafa}
main{max-width:28rem;padding:2rem;text-align:center}h1{font-size:1.4rem;margin:0 0 .5rem}p{margin:0;color:#555}
@media (prefers-color-scheme:dark){body{color:#eee;background:#151515}p{color:#aaa}}</style>
</head><body><main><h1>Back shortly</h1><p>This site is temporarily unavailable. Please try again in a minute or two.</p></main></body></html>
`

// holdingPage is the Caddy handler for an Egg with no healthy replica: a 503 with
// Retry-After, never cached, so a proxy or browser picks the real site up again the
// moment it is back.
func holdingPage() map[string]any {
	return map[string]any{
		"handler":     "static_response",
		"status_code": 503,
		"headers": map[string][]string{
			"Content-Type":  {"text/html; charset=utf-8"},
			"Cache-Control": {"no-store"},
			"Retry-After":   {"30"},
		},
		"body": holdingPageHTML,
	}
}
