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
