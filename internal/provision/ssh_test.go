package provision

import (
	"strings"
	"testing"
)

const testKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIexampleexampleexampleexampleexample steve@laptop"

func TestHasAnyKey(t *testing.T) {
	if hasAnyKey(nil) || hasAnyKey([]string{"", "  "}) {
		t.Fatal("blank/empty key lists must report no keys")
	}
	if !hasAnyKey([]string{"", testKey}) {
		t.Fatal("a non-blank key must report present")
	}
}

func TestAuthorizedKeysFile(t *testing.T) {
	out := string(authorizedKeysFile([]string{testKey, "", "  "}))
	if !strings.Contains(out, testKey) {
		t.Fatal("the key must be written")
	}
	// Blank lines are dropped: header + one key = 2 lines.
	if lines := strings.Count(strings.TrimRight(out, "\n"), "\n") + 1; lines != 2 {
		t.Fatalf("expected header + 1 key, got %d lines:\n%s", lines, out)
	}
}

func TestSshDropIn_AlwaysAddsKeyFileNeverBreaksPasswords(t *testing.T) {
	// Keys present but NOT disabling passwords: the key file is added, passwords stay on.
	out := string(sshDropIn(sshKeysPath, false, true))
	if !strings.Contains(out, "AuthorizedKeysFile .ssh/authorized_keys "+sshKeysPath) {
		t.Fatal("must always add the managed key file alongside the user's own")
	}
	if strings.Contains(out, "PasswordAuthentication no") {
		t.Fatal("must NOT disable passwords unless asked")
	}
}

func TestSshDropIn_DisablesOnlyWithAKey(t *testing.T) {
	// Asked to disable but NO key present: must refuse (fail-safe against lockout).
	noKey := string(sshDropIn(sshKeysPath, true, false))
	if strings.Contains(noKey, "PasswordAuthentication no") {
		t.Fatal("must NOT disable password auth with no key present")
	}

	// Asked to disable WITH a key present: disables passwords and key-locks root.
	withKey := string(sshDropIn(sshKeysPath, true, true))
	for _, want := range []string{
		"PasswordAuthentication no",
		"KbdInteractiveAuthentication no",
		"PermitRootLogin prohibit-password",
	} {
		if !strings.Contains(withKey, want) {
			t.Errorf("missing %q in:\n%s", want, withKey)
		}
	}
}
