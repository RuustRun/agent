#!/usr/bin/env bash
#
# egg-egress.sh - Ruust Egg egress firewall.
#
# Shipped inside the ruust-agent binary and written out by `ruust-agent provision`
# (the root host-provisioning reconciler), so the firewall stays current on every host
# with no re-enrol. Keep this in step with infra/provisioning/firewall/egg-egress.sh in
# the control-plane repo.
#
# Egg-to-Egg isolation on the bridge itself is handled by the agent
# (enable_icc=false on the ruust-eggs network). This script closes what that
# bridge option cannot: it stops a hostile Egg from reaching the cloud metadata
# endpoint (credential theft) and the private RFC1918 network (lateral movement
# into the VPC, the control plane and other hosts), from opening new connections
# to the host itself, and from opening direct SMTP to spam the internet. General
# internet egress stays open, because egress is unmetered on every tier.
#
# CRUCIAL: the forwarded drops fire only when traffic LEAVES the Ruust bridges
# (! -o ruust+, i.e. out to the host or the physical network). Traffic that stays
# between Ruust containers (a peered Egg reaching its database over a private
# ruust-<hash> bridge, in-in on ruust+) is never touched here, so private
# networking keeps working. Cross-container isolation is Docker's job (ICC).
#
# Forwarded traffic is filtered in Docker's DOCKER-USER chain, which Docker
# guarantees is evaluated first in the FORWARD path and never clobbers.
# Host-destined traffic is filtered in a dedicated INPUT chain that still allows
# established replies, so loopback-published ingress keeps working. Everything
# matches the ruust+ interface wildcard: every Ruust bridge is named with a ruust
# prefix (the shared isolated bridge ruust-eggs0, and each customer's private
# bridge ruust-<hash>), so one rule set covers them all and applies the moment
# Eggs appear, even if this runs before the networks exist.
#
# Requires the Docker daemon to manage iptables (the default). With iptables
# management disabled this, and enable_icc, are both silent no-ops.
#
# Idempotent: safe to run repeatedly and on every boot. ruust-egg-firewall.service
# re-runs it whenever Docker restarts, because Docker recreates DOCKER-USER then.
#
# British English throughout. No em dashes.

set -euo pipefail

# ruust+ matches every Ruust bridge (ruust-eggs0 and per-customer ruust-<hash>).
IFACE="${RUUST_EGG_BRIDGE:-ruust+}"
HOST_CHAIN="RUUST-EGG-HOST"

# Outbound mail ports an Egg must never open directly. Port 25 is direct-to-MX
# SMTP, the classic spam vector: left open, one hostile Egg spams the internet
# and gets our host IPs (and the shared ruust.run reputation) onto blocklists.
# Every serious host blocks it by default. Submission ports 587 and 465
# (authenticated relay) stay OPEN on purpose, so a legitimate app can still send
# through a proper relay. We recommend MailJunky for that. The list is delivered
# by the control plane in the provisioning manifest (firewall.blockedOutboundPorts).
BLOCKED_SMTP_PORTS="${RUUST_BLOCKED_SMTP_PORTS:-25}"

# Destinations an Egg must never reach: link-local (including the cloud metadata
# endpoint 169.254.169.254 and ECS 169.254.170.2) and the private ranges (the
# host, the VPC, the control plane, other Docker networks and other hosts).
BLOCKED_V4=(
  "169.254.0.0/16"
  "10.0.0.0/8"
  "172.16.0.0/12"
  "192.168.0.0/16"
)

log() { printf '[ruust-egg-firewall] %s\n' "$*"; }

if ! iptables -L DOCKER-USER -n >/dev/null 2>&1; then
  log "WARNING: DOCKER-USER chain not found. Is Docker running with iptables management enabled? Egg egress is NOT filtered."
  exit 0
fi

# --- Purge the pre-fix, UNGUARDED form of these drops (no ! -o), which also caught
#     peer-to-peer traffic and broke private networking. A host that ran an earlier
#     version keeps those rules until removed, and the add-only loops below would not
#     replace them. Delete every instance before adding the guarded form. ---
for cidr in "${BLOCKED_V4[@]}"; do
  while iptables -C DOCKER-USER -i "$IFACE" -d "$cidr" -j DROP 2>/dev/null; do
    iptables -D DOCKER-USER -i "$IFACE" -d "$cidr" -j DROP
  done
done
for port in $BLOCKED_SMTP_PORTS; do
  while iptables -C DOCKER-USER -i "$IFACE" -p tcp --dport "$port" -j DROP 2>/dev/null; do
    iptables -D DOCKER-USER -i "$IFACE" -p tcp --dport "$port" -j DROP
  done
done

# --- Forwarded egress: block metadata and the private ranges, but only when the
#     traffic is leaving the Ruust bridges (! -o ruust+). Peer-to-peer traffic
#     that stays on a ruust bridge is left alone, so private networking works. ---
for cidr in "${BLOCKED_V4[@]}"; do
  if ! iptables -C DOCKER-USER -i "$IFACE" ! -o "$IFACE" -d "$cidr" -j DROP 2>/dev/null; then
    iptables -I DOCKER-USER -i "$IFACE" ! -o "$IFACE" -d "$cidr" -j DROP
  fi
done

# --- Forwarded egress: block direct outbound SMTP so an Egg cannot spam
#     straight to recipients' mail servers (anti-abuse, reputation). Submission
#     to an authenticated relay (587/465) stays open. Leaving the bridges only. ---
for port in $BLOCKED_SMTP_PORTS; do
  if ! iptables -C DOCKER-USER -i "$IFACE" ! -o "$IFACE" -p tcp --dport "$port" -j DROP 2>/dev/null; then
    iptables -I DOCKER-USER -i "$IFACE" ! -o "$IFACE" -p tcp --dport "$port" -j DROP
  fi
done

# --- Host-destined: block an Egg initiating connections to the host, whilst
#     allowing established replies so loopback-published ingress still works. ---
iptables -N "$HOST_CHAIN" 2>/dev/null || true
iptables -F "$HOST_CHAIN"
iptables -A "$HOST_CHAIN" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A "$HOST_CHAIN" -j DROP
if ! iptables -C INPUT -i "$IFACE" -j "$HOST_CHAIN" 2>/dev/null; then
  iptables -I INPUT -i "$IFACE" -j "$HOST_CHAIN"
fi

# --- IPv6 (best effort, only if ip6tables and its DOCKER-USER chain exist).
#     Block the IPv6 metadata address, ULA and link-local; general v6 egress
#     stays open. ---
if command -v ip6tables >/dev/null 2>&1 && ip6tables -L DOCKER-USER -n >/dev/null 2>&1; then
  # Purge the pre-fix unguarded v6 form first (see the v4 note above).
  for cidr in "fd00:ec2::254/128" "fc00::/7" "fe80::/10"; do
    while ip6tables -C DOCKER-USER -i "$IFACE" -d "$cidr" -j DROP 2>/dev/null; do
      ip6tables -D DOCKER-USER -i "$IFACE" -d "$cidr" -j DROP
    done
  done
  for port in $BLOCKED_SMTP_PORTS; do
    while ip6tables -C DOCKER-USER -i "$IFACE" -p tcp --dport "$port" -j DROP 2>/dev/null; do
      ip6tables -D DOCKER-USER -i "$IFACE" -p tcp --dport "$port" -j DROP
    done
  done
  for cidr in "fd00:ec2::254/128" "fc00::/7" "fe80::/10"; do
    if ! ip6tables -C DOCKER-USER -i "$IFACE" ! -o "$IFACE" -d "$cidr" -j DROP 2>/dev/null; then
      ip6tables -I DOCKER-USER -i "$IFACE" ! -o "$IFACE" -d "$cidr" -j DROP
    fi
  done
  for port in $BLOCKED_SMTP_PORTS; do
    if ! ip6tables -C DOCKER-USER -i "$IFACE" ! -o "$IFACE" -p tcp --dport "$port" -j DROP 2>/dev/null; then
      ip6tables -I DOCKER-USER -i "$IFACE" ! -o "$IFACE" -p tcp --dport "$port" -j DROP
    fi
  done
fi

log "applied for ${IFACE}: metadata, private ranges and direct SMTP (${BLOCKED_SMTP_PORTS}) blocked, host protected, internet egress open."
