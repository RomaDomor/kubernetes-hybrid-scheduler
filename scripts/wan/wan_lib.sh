#!/usr/bin/env bash
# /usr/local/sbin/wan/wan_lib.sh
set -euo pipefail

# Load persisted env if available
if [[ -f /etc/wan/env ]]; then
  # shellcheck source=/etc/wan/env
  . /etc/wan/env
fi

detect_ifaces() {
  if [[ -n "${EDGE_IF:-}" && -n "${CLOUD_IF:-}" ]]; then
    echo "Using persisted/ENV EDGE_IF=$EDGE_IF CLOUD_IF=$CLOUD_IF"
    return
  fi
  echo "ERROR: EDGE_IF/CLOUD_IF not found. Run installer to generate /etc/wan/env" >&2
  exit 1
}

clear_qdisc() {
  # Egress roots
  tc qdisc del dev "$EDGE_IF"  root 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" root 2>/dev/null || true
}

show_qdisc() {
  echo "== qdisc $EDGE_IF ==";  tc qdisc show dev "$EDGE_IF"  || true
  echo "== qdisc $CLOUD_IF =="; tc qdisc show dev "$CLOUD_IF" || true
}

# Simple egress-only shaping with specific bidirectional u32 filters
apply_shape_simple() {
  local rate_mbit="$1"
  local one_way_ms="$2"
  local jitter_ms="$3"
  local loss_pct="$4"

  local full_rate="1000mbit"

  # EDGE_IF egress: impair traffic FROM cloud TO edge (return traffic)
  tc qdisc add dev "$EDGE_IF" root handle 1: htb default 20 r2q 1
  tc class add dev "$EDGE_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$EDGE_IF" parent 1: classid 1:20 htb rate "$full_rate" ceil "$full_rate"

  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$EDGE_IF" parent 1:20 handle 20: fq_codel

  # Match packets: src=cloud AND dst=edge
  tc filter add dev "$EDGE_IF" protocol ip parent 1: prio 1 u32 \
    match ip src "${CLOUD_CIDR}" \
    match ip dst "${EDGE_CIDR}" \
    flowid 1:10

  # CLOUD_IF egress: impair traffic FROM edge TO cloud (forward traffic)
  tc qdisc add dev "$CLOUD_IF" root handle 1: htb default 20 r2q 1
  tc class add dev "$CLOUD_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$CLOUD_IF" parent 1: classid 1:20 htb rate "$full_rate" ceil "$full_rate"

  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$CLOUD_IF" parent 1:20 handle 20: fq_codel

  # Match packets: src=edge AND dst=cloud
  tc filter add dev "$CLOUD_IF" protocol ip parent 1: prio 1 u32 \
    match ip src "${EDGE_CIDR}" \
    match ip dst "${CLOUD_CIDR}" \
    flowid 1:10
}