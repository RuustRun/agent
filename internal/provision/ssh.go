package provision

import (
	"fmt"
	"strings"
)

// SSH hardening rendering, kept OS-independent (no build tag) so it is unit-testable on
// any platform. applySSH (Linux only) writes these to disk, validates sshd and reloads.
//
// British English throughout. No em dashes.

// sshKeysPath is the managed operator-key file, added alongside each user's own
// authorized_keys so we never clobber a key the operator put there by hand.
const sshKeysPath = "/etc/ruust/ssh/operator_authorized_keys"

// sshDropInPath is our sshd_config drop-in. sshd includes /etc/ssh/sshd_config.d/*.conf in
// lexical order and honours the FIRST value for each keyword, and Ubuntu cloud images ship
// 50-cloud-init.conf with PasswordAuthentication yes, so our file MUST sort before it
// (10- < 50-) or our "no" is silently ignored.
const sshDropInPath = "/etc/ssh/sshd_config.d/10-ruust-hardening.conf"

// oldSSHDropInPath is the pre-ordering-fix filename, retired in applySSH so it cannot
// linger and confuse a future audit (it was already shadowed by cloud-init anyway).
const oldSSHDropInPath = "/etc/ssh/sshd_config.d/50-ruust-hardening.conf"

// hasAnyKey reports whether at least one non-blank key is present.
func hasAnyKey(keys []string) bool {
	for _, k := range keys {
		if strings.TrimSpace(k) != "" {
			return true
		}
	}
	return false
}

// authorizedKeysFile renders the managed operator authorized_keys file.
func authorizedKeysFile(keys []string) []byte {
	var b strings.Builder
	b.WriteString("# Managed by ruust-agent provision. Operator SSH keys. Do not edit by hand.\n")
	for _, k := range keys {
		if k = strings.TrimSpace(k); k != "" {
			b.WriteString(k)
			b.WriteByte('\n')
		}
	}
	return []byte(b.String())
}

// sshDropIn renders the sshd_config drop-in. It ALWAYS adds the managed key file
// alongside the user's own authorized_keys (additive, never destructive). It disables
// password auth ONLY when asked AND a key is present, so a mistake cannot lock the fleet
// out; that also key-locks root (prohibit-password) so a swapped-off password cannot be
// reached via root either.
func sshDropIn(keysPath string, disablePassword, hasKeys bool) []byte {
	var b strings.Builder
	b.WriteString("# Managed by ruust-agent provision. British English. No em dashes.\n")
	b.WriteString("# Adds the operator key file next to each user's own authorized_keys.\n")
	fmt.Fprintf(&b, "AuthorizedKeysFile .ssh/authorized_keys %s\n", keysPath)
	if disablePassword && hasKeys {
		b.WriteString("PasswordAuthentication no\n")
		b.WriteString("KbdInteractiveAuthentication no\n")
		b.WriteString("PermitRootLogin prohibit-password\n")
	}
	return []byte(b.String())
}
