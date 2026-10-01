package docker

import "testing"

// pinnedByDigest decides whether EnsureImage may serve an image from the local copy
// forever. Getting it wrong in one direction wastes a manifest request per converge;
// in the other it serves stale bytes after a tag has been rebuilt, which is how
// upstream security patches silently fail to reach a host.
func TestPinnedByDigest(t *testing.T) {
	cases := []struct {
		ref  string
		want bool
	}{
		// Digest-pinned: immutable, so presence is proof of currency.
		{"ghcr.io/ruustrun/postgres@sha256:" + hex64, true},
		{"postgres@sha256:" + hex64, true},
		// Mutable tags: must be re-checked.
		{"ghcr.io/ruustrun/postgres:17", false},
		{"ghcr.io/ruustrun/postgres:latest", false},
		{"postgres", false},
		// A registry port is a colon that is NOT a digest, and reading it as one would
		// pin every image on a private registry to whatever was first pulled.
		{"registry.local:5000/ruust/postgres:17", false},
		{"registry.local:5000/ruust/postgres", false},
		// Both: the digest wins, which is what Docker resolves too.
		{"registry.local:5000/ruust/postgres:17@sha256:" + hex64, true},
	}
	for _, c := range cases {
		if got := pinnedByDigest(c.ref); got != c.want {
			t.Errorf("pinnedByDigest(%q) = %v, want %v", c.ref, got, c.want)
		}
	}
}

const hex64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
