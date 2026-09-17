package main

import (
	"testing"

	"github.com/RuustRun/agent/internal/contract"
)

// A well-formed 64-char lowercase-hex digest for the tests.
const validHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestIsHexSha256(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{validHash, true},
		{"", false},
		{"abc", false},                    // too short
		{validHash + "0", false},          // too long
		{"g" + validHash[1:], false},      // non-hex char
		{"ABCDEF" + validHash[6:], false}, // uppercase is not accepted here
	}
	for _, c := range cases {
		if got := isHexSha256(c.in); got != c.want {
			t.Errorf("isHexSha256(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestAgentUpdateHashFor(t *testing.T) {
	h := contract.AgentUpdateHash{Amd64: "a", Arm64: "b"}
	if got := h.For("amd64"); got != "a" {
		t.Errorf(`For("amd64") = %q, want "a"`, got)
	}
	if got := h.For("arm64"); got != "b" {
		t.Errorf(`For("arm64") = %q, want "b"`, got)
	}
	if got := h.For("riscv64"); got != "" {
		t.Errorf(`For("riscv64") = %q, want ""`, got)
	}
}

func TestPinnedUpdateHash(t *testing.T) {
	const target = "v0.2.0"

	t.Run("nil pin falls back", func(t *testing.T) {
		if h, ok := pinnedUpdateHash(nil, target, "amd64"); ok || h != "" {
			t.Fatalf("got (%q, %v), want (\"\", false)", h, ok)
		}
	})

	t.Run("version mismatch falls back", func(t *testing.T) {
		au := &contract.AgentUpdate{Version: "v0.1.0", Sha256: contract.AgentUpdateHash{Amd64: validHash}}
		if h, ok := pinnedUpdateHash(au, target, "amd64"); ok || h != "" {
			t.Fatalf("got (%q, %v), want (\"\", false)", h, ok)
		}
	})

	t.Run("missing arch falls back", func(t *testing.T) {
		au := &contract.AgentUpdate{Version: target, Sha256: contract.AgentUpdateHash{Arm64: validHash}}
		if h, ok := pinnedUpdateHash(au, target, "amd64"); ok || h != "" {
			t.Fatalf("got (%q, %v), want (\"\", false)", h, ok)
		}
	})

	t.Run("malformed hash falls back", func(t *testing.T) {
		au := &contract.AgentUpdate{Version: target, Sha256: contract.AgentUpdateHash{Amd64: "deadbeef"}}
		if h, ok := pinnedUpdateHash(au, target, "amd64"); ok || h != "" {
			t.Fatalf("got (%q, %v), want (\"\", false)", h, ok)
		}
	})

	t.Run("valid pin is used", func(t *testing.T) {
		au := &contract.AgentUpdate{Version: target, Sha256: contract.AgentUpdateHash{Amd64: validHash}}
		if h, ok := pinnedUpdateHash(au, target, "amd64"); !ok || h != validHash {
			t.Fatalf("got (%q, %v), want (%q, true)", h, ok, validHash)
		}
	})

	t.Run("uppercase pin is normalised to lowercase", func(t *testing.T) {
		upper := "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"
		au := &contract.AgentUpdate{Version: target, Sha256: contract.AgentUpdateHash{Amd64: upper}}
		h, ok := pinnedUpdateHash(au, target, "amd64")
		if !ok || h != validHash {
			t.Fatalf("got (%q, %v), want (%q, true)", h, ok, validHash)
		}
	})
}
