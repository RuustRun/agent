package ingress

import (
	"encoding/json"
	"log/slog"
	"os"
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

// TestDumpCaddyConfig writes a full generated config to the path in
// RUUST_DUMP_CADDY_CONFIG, so it can be validated against a real Caddy
// (docker run ... caddy validate). Skipped unless that env var is set.
func TestDumpCaddyConfig(t *testing.T) {
	out := os.Getenv("RUUST_DUMP_CADDY_CONFIG")
	if out == "" {
		t.Skip("set RUUST_DUMP_CADDY_CONFIG to a path to dump the generated config")
	}
	r := New("http://localhost:2019", "127.0.0.1", "http://127.0.0.1:9700/ask", false, slog.Default())
	cfg, _ := r.build(
		[]Route{{Hostnames: []string{"app.example.com"}, UpstreamPorts: []int{32950}}},
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
