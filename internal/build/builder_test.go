package build

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A repo with no Nixpacks config gets a nixpacks.toml pinning the archive, so the build
// resolves Node against a current nixpkgs rather than the buildpack's stale snapshot.
func TestEnsureNixpkgsPin_WritesWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	var logged strings.Builder
	ensureNixpkgsPin(dir, func(s string) { logged.WriteString(s) })

	got, err := os.ReadFile(filepath.Join(dir, "nixpacks.toml"))
	if err != nil {
		t.Fatalf("expected nixpacks.toml to be written: %v", err)
	}
	if !strings.Contains(string(got), pinnedNixpkgsArchive) {
		t.Errorf("nixpacks.toml missing pinned archive:\n%s", got)
	}
	if !strings.Contains(string(got), "[phases.setup]") {
		t.Errorf("nixpacks.toml missing [phases.setup]:\n%s", got)
	}
	if !strings.Contains(logged.String(), "pinning nixpkgs archive") {
		t.Errorf("expected a pin log line, got: %q", logged.String())
	}
}

// A repo that ships its own nixpacks.toml owns that file: we must not overwrite it, even
// to add the pin (the customer may have deliberately chosen a different archive).
func TestEnsureNixpkgsPin_RespectsExistingToml(t *testing.T) {
	dir := t.TempDir()
	existing := "[phases.setup]\nnixPkgs = ['nodejs_18']\n"
	if err := os.WriteFile(filepath.Join(dir, "nixpacks.toml"), []byte(existing), 0o644); err != nil {
		t.Fatal(err)
	}
	var logged strings.Builder
	ensureNixpkgsPin(dir, func(s string) { logged.WriteString(s) })

	got, err := os.ReadFile(filepath.Join(dir, "nixpacks.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != existing {
		t.Errorf("existing nixpacks.toml was modified:\n%s", got)
	}
	if strings.Contains(logged.String(), "pinning nixpkgs archive") {
		t.Errorf("should not log a pin when the repo owns its config: %q", logged.String())
	}
}

// A repo with a nixpacks.json (the JSON form of the same config) is likewise left alone.
func TestEnsureNixpkgsPin_RespectsExistingJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "nixpacks.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ensureNixpkgsPin(dir, func(string) {})

	if _, err := os.Stat(filepath.Join(dir, "nixpacks.toml")); !os.IsNotExist(err) {
		t.Errorf("should not write nixpacks.toml when nixpacks.json is present (err=%v)", err)
	}
}
