package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestParseSha256(t *testing.T) {
	sum := sha256.Sum256([]byte("ruust-agent-binary"))
	want := hex.EncodeToString(sum[:])

	// Bare digest, sha256sum style, and upper-case all normalise to the digest.
	for _, body := range []string{want, want + "  ruust-agent-linux-amd64\n", strings.ToUpper(want)} {
		got, err := parseSha256(body)
		if err != nil {
			t.Fatalf("parseSha256(%q) error: %v", body, err)
		}
		if got != want {
			t.Fatalf("parseSha256(%q) = %s, want %s", body, got, want)
		}
	}

	// Anything that is not exactly one 64-hex digest is rejected.
	for _, bad := range []string{"", "abc123", strings.Repeat("z", 64), strings.Repeat("a", 63)} {
		if _, err := parseSha256(bad); err == nil {
			t.Errorf("parseSha256(%q) should have been rejected", bad)
		}
	}
}

func TestRequireSecureURL(t *testing.T) {
	cases := []struct {
		url  string
		ok   bool
	}{
		{"https://cp.ruust.run", true},
		{"https://cp.ruust.run/x", true},
		{"http://cp.ruust.run", false},
		{"http://192.0.2.10:3939", false},
		{"http://localhost:3939", true},
		{"http://127.0.0.1:3939", true},
	}
	for _, c := range cases {
		err := requireSecureURL(c.url)
		if (err == nil) != c.ok {
			t.Errorf("requireSecureURL(%q): ok=%v err=%v", c.url, c.ok, err)
		}
	}
	// Explicit opt-out allows a remote http URL.
	t.Setenv("RUUST_ALLOW_INSECURE_CP", "1")
	if err := requireSecureURL("http://cp.internal"); err != nil {
		t.Errorf("opt-out should allow http: %v", err)
	}
}
