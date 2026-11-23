package ingress

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/RuustRun/agent/internal/contract"
)

// TestServerTimeoutSchema locks the Caddy server timeout fields. Caddy v2 has no
// "timeouts" object; an earlier version emitted one and Caddy rejected the whole
// config with a 400, freezing ingress. Keep the timeouts as direct server fields.
func TestServerTimeoutSchema(t *testing.T) {
	s := ruustServer(nil, &contract.IngressConfig{
		ReadTimeout:  "30s",
		WriteTimeout: "30s",
		IdleTimeout:  "120s",
	})
	if _, bad := s["timeouts"]; bad {
		t.Fatal("server must not carry a 'timeouts' object; Caddy rejects it as an unknown field")
	}
	for _, k := range []string{"read_timeout", "read_header_timeout", "write_timeout", "idle_timeout"} {
		if _, ok := s[k]; !ok {
			t.Errorf("server missing direct timeout field %q", k)
		}
	}
}

// TestAdminBindsLoopback locks the Caddy admin API to loopback. The admin API is
// UNAUTHENTICATED and grants full control of ingress, and Caddy's /load replaces the
// admin listener with whatever the posted config says, so a 0.0.0.0 bind here would
// silently expose it on the public interface. It must always be 127.0.0.1.
func TestAdminBindsLoopback(t *testing.T) {
	r := New("http://localhost:2019", "127.0.0.1", "http://127.0.0.1:9700/ask", false, slog.Default())
	cfg, _, _ := r.build([]Route{{Hostnames: []string{"app.example.com"}, UpstreamPorts: []int{32950}}}, nil)

	admin, ok := cfg["admin"].(map[string]any)
	if !ok {
		t.Fatal("config is missing an admin block")
	}
	listen, _ := admin["listen"].(string)
	if listen != "127.0.0.1:2019" {
		t.Fatalf("admin.listen = %q, want 127.0.0.1:2019 (must never bind a non-loopback interface)", listen)
	}

	// Belt and braces: the whole serialised config must not mention 0.0.0.0:2019.
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "0.0.0.0:2019") {
		t.Fatal("generated config exposes the admin API on 0.0.0.0:2019")
	}
}

// TestDumpCaddyConfig writes a full generated config to the path in
// RUUST_DUMP_CADDY_CONFIG, so it can be validated against a real Caddy
// (docker run ... caddy validate). Skipped unless that env var is set.
func TestDumpCaddyConfig(t *testing.T) {
	out := os.Getenv("RUUST_DUMP_CADDY_CONFIG")
	if out == "" {
		t.Skip("set RUUST_DUMP_CADDY_CONFIG to a path to dump the generated config")
	}
	r := New("http://localhost:2019", "127.0.0.1", "http://127.0.0.1:9700/ask", false, slog.Default())
	cfg, _, _ := r.build(
		[]Route{
			{Hostnames: []string{"app.example.com"}, UpstreamPorts: []int{32950}},
			{Hostnames: []string{"down.example.com"}},
		},
		&contract.IngressConfig{
			ReadTimeout:  "30s",
			IdleTimeout:  "120s",
			WriteTimeout: "30s",
			DialTimeout:  "10s",
			MaxBodyBytes: 26214400,
		},
	)
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, b, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d bytes to %s", len(b), out)
}

// TestProxiedHostnameGetsInternalIssuer checks that a proxied custom domain is served with the
// internal self-signed CA (so a Cloudflare "Full" proxy in front works), while a direct domain
// keeps the default (ACME) issuer in production.
func TestProxiedHostnameGetsInternalIssuer(t *testing.T) {
	r := New("http://localhost:2019", "127.0.0.1", "http://127.0.0.1:9700/ask", false, slog.Default())
	cfg, _, proxied := r.build([]Route{{
		Hostnames:        []string{"direct.example.com", "proxied.example.com"},
		UpstreamPorts:    []int{32950},
		ProxiedHostnames: []string{"proxied.example.com"},
	}}, nil)

	if !proxied["proxied.example.com"] || proxied["direct.example.com"] {
		t.Fatalf("proxied set = %v, want only proxied.example.com", proxied)
	}

	policies := cfg["apps"].(map[string]any)["tls"].(map[string]any)["automation"].(map[string]any)["policies"].([]map[string]any)
	var internalSubjects []string
	sawDefault := false
	for _, p := range policies {
		subs, hasSubs := p["subjects"].([]string)
		issuers, hasIssuers := p["issuers"].([]map[string]any)
		if !hasSubs {
			sawDefault = true
			if hasIssuers {
				t.Error("the default policy must not pin an issuer in production (it uses ACME)")
			}
			continue
		}
		if hasIssuers && issuers[0]["module"] == "internal" {
			internalSubjects = append(internalSubjects, subs...)
		}
	}
	if !sawDefault {
		t.Error("missing the default (catch-all) TLS policy")
	}
	if len(internalSubjects) != 1 || internalSubjects[0] != "proxied.example.com" {
		t.Errorf("internal-issuer subjects = %v, want [proxied.example.com]", internalSubjects)
	}
}

// TestUnhealthyEggServesHoldingPage checks that an Egg with no healthy replica keeps its
// hostnames (so TLS still completes and the certificate stays allowed) and is answered with
// a 503 holding page, instead of vanishing from ingress (which a fronting Cloudflare shows as
// a 525 handshake failure).
func TestUnhealthyEggServesHoldingPage(t *testing.T) {
	r := New("http://localhost:2019", "127.0.0.1", "http://127.0.0.1:9700/ask", false, slog.Default())
	cfg, allowed, _ := r.build([]Route{
		{Hostnames: []string{"up.example.com"}, UpstreamPorts: []int{32950}},
		{Hostnames: []string{"down.example.com"}},
	}, nil)
	if !allowed["down.example.com"] || !allowed["up.example.com"] {
		t.Fatalf("both hostnames must stay allowed a certificate, got %v", allowed)
	}
	server := cfg["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["ruust"].(map[string]any)
	if server["@id"] != serverID {
		t.Fatalf("server must carry @id %q so a Caddy restart can be detected", serverID)
	}
	routes := server["routes"].([]map[string]any)
	if len(routes) != 2 {
		t.Fatalf("want 2 routes, got %d", len(routes))
	}
	var down map[string]any
	for _, rt := range routes {
		if rt["match"].([]map[string]any)[0]["host"].([]string)[0] == "down.example.com" {
			down = rt
		}
	}
	if down == nil {
		t.Fatal("the unhealthy Egg must keep a route")
	}
	h := down["handle"].([]map[string]any)[0]
	if h["handler"] != "static_response" || h["status_code"] != 503 {
		t.Fatalf("unhealthy route must answer 503 static_response, got %v", h)
	}
}
