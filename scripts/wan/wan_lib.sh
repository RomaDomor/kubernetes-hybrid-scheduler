#!/usr/bin/env bash
# /usr/local/sbin/wan/wan_lib.sh
set -euo pipefail

# Load persisted env if available
if [[ -f /etc/wan/env ]]; then
  # shellcheck source=/etc/wan/env
  . /etc/wan/env
fi

detect_ifaces() {
  if [[ -n "${EDGE_IF:-}" && -n "${CLOUD_IF:-}" && -n "${FOG_IF:-}" ]]; then
    echo "Using persisted/ENV EDGE_IF=$EDGE_IF CLOUD_IF=$CLOUD_IF FOG_IF=$FOG_IF"
    return
  fi
  echo "ERROR: EDGE_IF/CLOUD_IF/FOG_IF not found. Run installer to generate /etc/wan/env" >&2
  exit 1
}

clear_qdisc() {
  tc qdisc del dev "$EDGE_IF"  root 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" root 2>/dev/null || true
  tc qdisc del dev "$FOG_IF"   root 2>/dev/null || true
}

show_qdisc() {
  echo "== qdisc $EDGE_IF ==";  tc qdisc show dev "$EDGE_IF"  || true
  echo "== qdisc $CLOUD_IF =="; tc qdisc show dev "$CLOUD_IF" || true
  echo "== qdisc $FOG_IF ==";   tc qdisc show dev "$FOG_IF"   || true
}

# Apply identical egress shaping on all three WAN interfaces:
#   EDGE_IF  – shapes traffic coming back from cloud/fog toward edge
#   CLOUD_IF – shapes traffic going from edge toward cloud
#   FOG_IF   – shapes traffic going from edge toward fog
apply_shape_simple() {
  local rate_mbit="$1"
  local one_way_ms="$2"
  local jitter_ms="$3"
  local loss_pct="$4"

  local full_rate="1000mbit"

  _shape_iface() {
    local iface="$1"
    local src_cidr="$2"
    local dst_cidr="$3"

    tc qdisc add dev "$iface" root handle 1: htb default 20 r2q 1
    tc class add dev "$iface" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
    tc class add dev "$iface" parent 1: classid 1:20 htb rate "$full_rate"      ceil "$full_rate"

    if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
      tc qdisc add dev "$iface" parent 1:10 handle 10: netem \
        delay "${one_way_ms}ms" loss "${loss_pct}%"
    else
      tc qdisc add dev "$iface" parent 1:10 handle 10: netem \
        delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
    fi
    tc qdisc add dev "$iface" parent 1:20 handle 20: fq_codel

    tc filter add dev "$iface" protocol ip parent 1: prio 1 u32 \
      match ip src "${src_cidr}" \
      match ip dst "${dst_cidr}" \
      flowid 1:10
  }

  # EDGE_IF egress: return traffic cloud→edge and fog→edge
  # We use two filters on EDGE_IF for both remote CIDRs
  tc qdisc add dev "$EDGE_IF" root handle 1: htb default 20 r2q 1
  tc class add dev "$EDGE_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$EDGE_IF" parent 1: classid 1:20 htb rate "$full_rate"      ceil "$full_rate"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem \
      delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem \
      delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$EDGE_IF" parent 1:20 handle 20: fq_codel
  # Return traffic from cloud
  tc filter add dev "$EDGE_IF" protocol ip parent 1: prio 1 u32 \
    match ip src "${CLOUD_CIDR}" \
    match ip dst "${EDGE_CIDR}" \
    flowid 1:10
  # Return traffic from fog
  tc filter add dev "$EDGE_IF" protocol ip parent 1: prio 2 u32 \
    match ip src "${FOG_CIDR}" \
    match ip dst "${EDGE_CIDR}" \
    flowid 1:10

  # CLOUD_IF egress: forward traffic edge→cloud
  _shape_iface "$CLOUD_IF" "${EDGE_CIDR}" "${CLOUD_CIDR}"

  # FOG_IF egress: forward traffic edge→fog
  _shape_iface "$FOG_IF"   "${EDGE_CIDR}" "${FOG_CIDR}"
}
