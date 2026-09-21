#!/usr/bin/env bash
#
# inbound-fw.sh - Ruust host INBOUND firewall (default-deny INPUT).
#
# Shipped inside the ruust-agent binary and written out by `ruust-agent provision`
# (the root host-provisioning reconciler), so the policy stays current on every managed
# host with no re-enrol. Delivered to MANAGED hosts only; the control plane never touches
# a customer's BYO machine.
#
# This closes what the egress firewall (egg-egress.sh) cannot: it stops the public
# internet reaching any host port other than the ones a Ruust host is meant to expose.
# Everything the host actually needs stays open:
#   - loopback (127.0.0.1/::1): every internal service (Caddy admin 2019, Postgres 5432,
#     the agent ask endpoint 9700, the shell relay 9800, the app on 3000) binds here, so
#     leaving loopback fully open keeps them all working whilst the world cannot reach them.
#   - established/related: applying this NEVER drops the operator's current SSH session, and
#     every reply to a connection the host itself opened keeps flowing.
#   - ICMP: ping plus the error types PMTUD/traceroute need on v4; ALL ICMPv6 on v6 (NDP/RA
#     are mandatory or IPv6 stops working).
#   - 80/443: the public Caddy front door, the whole point of the platform.
#   - 22 (SSH): only from the operator allowlist (see below).
# Everything else destined for the host is dropped.
#
# CRUCIAL: Docker-published Egg ports are NOT governed here. A web Egg binds its port on
# loopback; a game Egg publishes on 0.0.0.0 but Docker DNATs it in PREROUTING and it
# traverses the FORWARD/DOCKER path, never INPUT. So this default-deny cannot break Eggs,
# ingress or private peering. Egg-bridge traffic (ruust+) is returned early and left to the
# egress firewall's RUUST-EGG-HOST chain, so the two never fight and ordering does not matter.
#
# FAIL-SAFE by design (a firewall must never be the thing that locks you out):
#   - Established connections are kept, so applying it cannot drop a live SSH session.
#   - SSH (22) is allowed ONLY from the allowlist. An EMPTY allowlist denies SSH from everywhere
#     (matching the console, which lets you add your IP over HTTPS without needing SSH); a
#     non-empty allowlist that names only one address family denies SSH on the other. This never
#     drops a LIVE session (established is kept), only new connections from non-allowed sources.
#   - The default DROP is the LAST rule and the INPUT jump is only kept when the whole
#     ruleset built cleanly; ANY build error tears the chain down to fully-open (never a
#     half-built chain with the DROP live) and reports failure so the next poll retries.
#   - It only runs at all when the control plane sets enforce; otherwise it tears itself down.
#
# Idempotent: rebuilds its chain from scratch every run, so it always reflects the current
# config and is safe to run repeatedly and on every boot.
#
# British English throughout. No em dashes.

set -euo pipefail

CHAIN="RUUST-INBOUND"
ENFORCE="${RUUST_INBOUND_ENFORCE:-0}"
SSH_ALLOWLIST="${RUUST_SSH_ALLOWLIST:-}"
EXTRA_TCP_PORTS="${RUUST_INBOUND_EXTRA_TCP_PORTS:-}"
# ruust+ matches every Ruust bridge (ruust-eggs0 and per-customer ruust-<hash>).
EGG_IFACE="${RUUST_EGG_BRIDGE:-ruust+}"

log() { printf '[ruust-inbound-firewall] %s\n' "$*"; }

# Lowercase with tr, not the bash-4 ${VAR,,}, so the script is portable to any /bin/sh-ish bash.
case "$(printf '%s' "$ENFORCE" | tr '[:upper:]' '[:lower:]')" in
  1 | on | true | yes) ENFORCE=1 ;;
  *) ENFORCE=0 ;;
esac

# teardown removes our INPUT jump and chain for one iptables binary. Resilient: a racing
# external change to the rule cannot abort it (each step tolerates its own failure), so the
# host always ends fully-open when enforcement is off.
teardown() {
  local ipt="$1"
  command -v "$ipt" >/dev/null 2>&1 || return 0
  while "$ipt" -C INPUT -j "$CHAIN" 2>/dev/null; do
    "$ipt" -D INPUT -j "$CHAIN" 2>/dev/null || break
  done
  if "$ipt" -L "$CHAIN" -n >/dev/null 2>&1; then
    "$ipt" -F "$CHAIN" 2>/dev/null || true
    "$ipt" -X "$CHAIN" 2>/dev/null || true
  fi
}

if [[ "$ENFORCE" != "1" ]]; then
  teardown iptables
  teardown ip6tables
  log "enforce off: inbound firewall removed, host INPUT left open."
  exit 0
fi

# Make sure connection tracking is available before we lean on it: the ESTABLISHED,RELATED
# rule (which keeps the live SSH session and every reply) needs conntrack. Modern kernels
# auto-load it; loading it explicitly is idempotent and stops a first apply failing on a box
# where it is not yet loaded. Best effort: never fatal.
modprobe nf_conntrack 2>/dev/null || true

# is_v6 succeeds when the address/CIDR is IPv6 (contains a colon).
is_v6() { [[ "$1" == *:* ]]; }

# apply builds and installs the chain for one family. $1 = binary, $2 = family (4 or 6).
# It builds the whole ruleset first and only KEEPS the INPUT jump when every rule landed; any
# failure leaves the host fully-open (never half-closed) and returns non-zero so the caller
# reports it and the next poll retries.
apply() {
  local ipt="$1" fam="$2"
  command -v "$ipt" >/dev/null 2>&1 || { log "$ipt not present, skipping IPv$fam."; return 0; }
  # Verify we can actually talk to this family's filter table. ip6tables can exist on a box
  # where IPv6 is disabled in the kernel, where its commands would silently no-op; probing
  # the table means we never believe we are enforcing IPv6 when we are not (and never error
  # on an IPv6-less box). Mirrors egg-egress.sh's table probe.
  if ! "$ipt" -L INPUT -n >/dev/null 2>&1; then
    log "IPv$fam filter table unavailable (IPv6 disabled?), skipping IPv$fam."
    return 0
  fi

  # Build with failures collected rather than aborting, so we never commit a half-built chain.
  set +e
  local ok=1
  add() { "$ipt" -A "$CHAIN" "$@" || ok=0; }

  "$ipt" -N "$CHAIN" 2>/dev/null
  "$ipt" -F "$CHAIN" || ok=0

  # Loopback: every internal service lives here. Always open.
  add -i lo -j ACCEPT
  # Egg-bridge traffic is the egress firewall's job (RUUST-EGG-HOST). Return it early so we
  # never apply the external-inbound policy to Eggs and never depend on INPUT rule ordering.
  add -i "$EGG_IFACE" -j RETURN
  # Keep every live connection: this is what stops an apply dropping the current SSH session.
  add -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
  add -m conntrack --ctstate INVALID -j DROP

  # ICMP. IPv6 needs ALL ICMPv6 (neighbour discovery, router advertisement) or IPv6 breaks
  # entirely. IPv4 gets ping plus the error types PMTUD and traceroute rely on, belt-and-braces
  # to ESTABLISHED,RELATED (which already catches the conntracked ones).
  if [[ "$fam" == "6" ]]; then
    add -p ipv6-icmp -j ACCEPT
    add -p udp --dport 546 -j ACCEPT # DHCPv6 client.
  else
    add -p icmp --icmp-type echo-request -m limit --limit 5/second -j ACCEPT
    add -p icmp --icmp-type destination-unreachable -j ACCEPT # PMTUD (fragmentation-needed).
    add -p icmp --icmp-type time-exceeded -j ACCEPT           # traceroute.
    add -p icmp --icmp-type parameter-problem -j ACCEPT
    add -p udp --sport 67 --dport 68 -j ACCEPT # DHCP client (leased address).
  fi

  # The public front door. Always open: this is what the platform exists to serve.
  add -p tcp --dport 80 -j ACCEPT
  add -p tcp --dport 443 -j ACCEPT

  # SSH (22). Allow ONLY this family's allowlist entries; the other family's entries are handled
  # by the other binary. If there are no entries for this family (an empty list, or an allowlist
  # that names only the other family), SSH is denied over this family (the default DROP below
  # handles it). An empty list therefore denies SSH everywhere, matching the console, which lets
  # an operator add their IP over HTTPS without needing SSH. Established sessions survive
  # regardless, so this never drops a live connection, only new ones from non-allowed sources.
  local have_any=0 have_fam=0 cidr
  for cidr in $SSH_ALLOWLIST; do
    have_any=1
    if is_v6 "$cidr"; then
      if [[ "$fam" == "6" ]]; then
        add -p tcp --dport 22 -s "$cidr" -j ACCEPT
        have_fam=1
      fi
    else
      if [[ "$fam" == "4" ]]; then
        add -p tcp --dport 22 -s "$cidr" -j ACCEPT
        have_fam=1
      fi
    fi
  done
  if [[ "$have_fam" == "0" ]]; then
    if [[ "$have_any" == "0" ]]; then
      log "IPv$fam: SSH allowlist empty, SSH DENIED (add an IP on the HTTPS console to allow it)."
    else
      log "IPv$fam: no IPv$fam entries in the SSH allowlist, SSH DENIED over IPv$fam (add an IPv$fam CIDR to allow it)."
    fi
  fi

  # Operator extras: any additional host-listening TCP ports.
  local port
  for port in $EXTRA_TCP_PORTS; do
    add -p tcp --dport "$port" -j ACCEPT
  done

  # Default-deny everything else destined for the host. LAST, so a build that fails before
  # here leaves no DROP (fail-open), never a chain that drops SSH.
  add -j DROP
  set -e

  if [[ "$ok" != "1" ]]; then
    # Something in the ruleset failed to apply. Do NOT leave a half-built policy live: strip
    # our jump and flush the chain so the host is fully-open, and report failure so the next
    # poll retries. Fail-open, never locked out.
    "$ipt" -D INPUT -j "$CHAIN" 2>/dev/null || true
    "$ipt" -F "$CHAIN" 2>/dev/null || true
    log "ERROR: IPv$fam ruleset build failed; left host INPUT OPEN (fail-open), will retry."
    return 1
  fi

  # Committed cleanly: route INPUT through the chain (once, at the top).
  if ! "$ipt" -C INPUT -j "$CHAIN" 2>/dev/null; then
    "$ipt" -I INPUT 1 -j "$CHAIN"
  fi
}

rc=0
apply iptables 4 || rc=1
apply ip6tables 6 || rc=1
if [[ "$rc" == "0" ]]; then
  log "applied: default-deny host INPUT (80/443 open, SSH per allowlist, loopback and established kept)."
fi
exit "$rc"
