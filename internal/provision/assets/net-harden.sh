#!/usr/bin/env bash
# net-harden.sh: kernel network hardening for a Ruust host.
#
# Applied by ruust-net-hardening.service at boot and by the provisioning reconciler
# whenever the control plane changes the policy. Each switch is read from
# /etc/ruust/net-hardening.env and is independent; a switch that is off removes only
# what this script itself installed, so turning the policy off restores the host.
#
#   RUUST_DROP_TCP_IP_OPTIONS  drop inbound IPv4 TCP packets that carry IP options, in
#                              raw PREROUTING so host and container listeners are both
#                              covered. Internet TCP does not use IP options.
#   RUUST_DISABLE_MPTCP        net.mptcp.enabled=0 now and on every boot.
#   RUUST_BLOCK_SCTP           stop the sctp module loading (nothing here uses SCTP).
#
# Idempotent: safe to run on every poll. British English. No em dashes.

set -uo pipefail

log() { printf '[ruust-net-hardening] %s\n' "$*"; }
on() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1 | on | true | yes) return 0 ;;
    *) return 1 ;;
  esac
}

SYSCTL_FILE=/etc/sysctl.d/90-ruust-net-hardening.conf
MODPROBE_FILE=/etc/modprobe.d/ruust-no-sctp.conf
# IHL (low nibble of the first header byte) above 5 means the header carries options.
RULE=(-p tcp -m u32 --u32 "0>>24&0x0f=6:15" -m comment --comment ruust-tcp-ip-options -j DROP)
status=0

# 1. TCP packets with IP options.
if command -v iptables >/dev/null 2>&1; then
  if on "${RUUST_DROP_TCP_IP_OPTIONS:-0}"; then
    if ! iptables -t raw -C PREROUTING "${RULE[@]}" 2>/dev/null; then
      if iptables -t raw -I PREROUTING 1 "${RULE[@]}"; then
        log "dropping inbound TCP with IP options."
      else
        log "could not add the IP options rule (is the u32 match available?)."
        status=1
      fi
    fi
  else
    while iptables -t raw -C PREROUTING "${RULE[@]}" 2>/dev/null; do
      iptables -t raw -D PREROUTING "${RULE[@]}" || break
    done
  fi
elif on "${RUUST_DROP_TCP_IP_OPTIONS:-0}"; then
  log "iptables not present; cannot drop TCP with IP options."
  status=1
fi

# 2. Multipath TCP.
if on "${RUUST_DISABLE_MPTCP:-0}"; then
  printf '# Managed by ruust-agent provision.\nnet.mptcp.enabled = 0\n' >"$SYSCTL_FILE"
  if [[ -e /proc/sys/net/mptcp/enabled ]]; then
    if ! sysctl -q -w net.mptcp.enabled=0; then
      log "could not disable MPTCP."
      status=1
    fi
  fi
else
  # Leave the running value alone; the kernel default returns on the next boot.
  rm -f "$SYSCTL_FILE"
fi

# 3. SCTP.
if on "${RUUST_BLOCK_SCTP:-0}"; then
  printf '# Managed by ruust-agent provision.\ninstall sctp /bin/false\nblacklist sctp\n' >"$MODPROBE_FILE"
  if grep -q '^sctp ' /proc/modules 2>/dev/null; then
    modprobe -r sctp 2>/dev/null || log "sctp is loaded and in use; it stays blocked from the next boot."
  fi
else
  rm -f "$MODPROBE_FILE"
fi

exit "$status"
