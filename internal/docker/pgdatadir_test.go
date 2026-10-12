package docker

import "testing"

// Postgres 18 moved where its image expects the volume: a single mount at
// /var/lib/postgresql, with the server writing into a major-version subdirectory
// beneath it. Given the pre-18 path, an 18 image refuses to start.
//
// These containers are brought up against an EXISTING volume holding somebody's data,
// so the unparseable cases matter as much as the happy path. Guessing wrong here would
// not merely fail: it would hand a restore an empty directory and look like it worked.
func TestPostgresDataDir(t *testing.T) {
	const legacy = "/var/lib/postgresql/data"
	const modern = "/var/lib/postgresql"

	cases := []struct {
		name  string
		image string
		want  string
	}{
		{"17 keeps the path it has always had", "ghcr.io/ruustrun/postgres:17", legacy},
		{"16 unchanged", "ghcr.io/ruustrun/postgres:16", legacy},
		{"15 unchanged", "ghcr.io/ruustrun/postgres:15", legacy},
		{"18 mounts a level up", "ghcr.io/ruustrun/postgres:18", modern},
		{"19 and later too", "ghcr.io/ruustrun/postgres:19", modern},
		{"a point release still reads its major", "ghcr.io/ruustrun/postgres:18.2", modern},
		{"17 point release stays legacy", "ghcr.io/ruustrun/postgres:17.5", legacy},

		// A registry port is not a tag. Reading "5000" as the version would push an
		// existing Egg onto the wrong layout.
		{"registry port is not mistaken for a version", "registry:5000/postgres", legacy},
		{"registry port with a real tag", "registry:5000/postgres:18", modern},

		// Unparseable: keep what the volume already has.
		{"no tag at all", "ghcr.io/ruustrun/postgres", legacy},
		{"a non-numeric tag", "ghcr.io/ruustrun/postgres:latest", legacy},
		{"empty", "", legacy},

		// Digest-pinned refs carry a colon inside the digest, which is not a tag.
		{"digest pinned, no tag", "ghcr.io/ruustrun/postgres@sha256:abc123", legacy},
		{"digest pinned with an 18 tag", "ghcr.io/ruustrun/postgres:18@sha256:abc123", modern},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := postgresDataDir(c.image); got != c.want {
				t.Fatalf("postgresDataDir(%q) = %q, want %q", c.image, got, c.want)
			}
		})
	}
}
