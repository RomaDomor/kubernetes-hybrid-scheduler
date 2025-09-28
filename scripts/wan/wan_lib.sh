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

  # Ingress qdiscs
  tc qdisc del dev "$EDGE_IF"  ingress 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" ingress 2>/dev/null || true

  # IFBs
  for IFB in ifb0 ifb1; do
    if ip link show "$IFB" >/dev/null 2>&1; then
      tc qdisc del dev "$IFB" root 2>/dev/null || true
      ip link set "$IFB" down 2>/dev/null || true
      ip link del "$IFB" 2>/dev/null || true
    fi
  done
}

show_qdisc() {
  echo "== qdisc $EDGE_IF ==";  tc qdisc show dev "$EDGE_IF"  || true
  echo "== qdisc $CLOUD_IF =="; tc qdisc show dev "$CLOUD_IF" || true
  if ip link show ifb0 >/dev/null 2>&1; then
    echo "== qdisc ifb0 =="; tc qdisc show dev ifb0 || true
  fi
  if ip link show ifb1 >/dev/null 2>&1; then
    echo "== qdisc ifb1 =="; tc qdisc show dev ifb1 || true
  fi
}

# Egress selective shaping (edge->cloud and cloud->edge) with flower
apply_shape_selective() {
  local rate_mbit="$1"   # Mbit/s
  local one_way_ms="$2"  # ms
  local jitter_ms="$3"   # ms
  local loss_pct="$4"    # %

  # EDGE_IF egress
  tc qdisc add dev "$EDGE_IF" root handle 1: htb default 20 r2q 1
  tc class add dev "$EDGE_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$EDGE_IF" parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$EDGE_IF" parent 1:20 handle 20: fq_codel

  # Only dst CLOUD_CIDR goes to 1:10 (impaired); everything else stays default 1:20
  tc filter add dev "$EDGE_IF" protocol ip parent 1: prio 1 flower dst_ip "${CLOUD_CIDR}" skip_hw flowid 1:10

  # CLOUD_IF egress
  tc qdisc add dev "$CLOUD_IF" root handle 1: htb default 20 r2q 1
  tc class add dev "$CLOUD_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$CLOUD_IF" parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$CLOUD_IF" parent 1:20 handle 20: fq_codel

  # Only dst EDGE_CIDR goes to 1:10 (impaired)
  tc filter add dev "$CLOUD_IF" protocol ip parent 1: prio 1 flower dst_ip "${EDGE_CIDR}" skip_hw flowid 1:10
}

# Create IFBs and redirect ingress to them
setup_ifb() {
  # Ensure ifb module available; ignore error if already loaded
  modprobe ifb numifbs=2 >/dev/null 2>&1 || true

  ip link add ifb0 type ifb 2>/dev/null || true
  ip link add ifb1 type ifb 2>/dev/null || true
  ip link set ifb0 up
  ip link set ifb1 up

  # Redirect ingress of EDGE_IF -> ifb0
  tc qdisc add dev "$EDGE_IF" handle ffff: ingress 2>/dev/null || true
  # clear existing filters if any
  tc filter del dev "$EDGE_IF" parent ffff: 2>/dev/null || true
  tc filter add dev "$EDGE_IF" parent ffff: protocol all \
    flower skip_hw action mirred egress redirect dev ifb0

  # Redirect ingress of CLOUD_IF -> ifb1
  tc qdisc add dev "$CLOUD_IF" handle ffff: ingress 2>/dev/null || true
  tc filter del dev "$CLOUD_IF" parent ffff: 2>/dev/null || true
  tc filter add dev "$CLOUD_IF" parent ffff: protocol all \
    flower skip_hw action mirred egress redirect dev ifb1
}

# Ingress selective shaping on IFBs (cloud->edge and edge->cloud) with flower
apply_ingress_shape_selective() {
  local rate_mbit="$1"   # Mbit/s
  local one_way_ms="$2"  # ms
  local jitter_ms="$3"   # ms
  local loss_pct="$4"    # %

  # ifb0 mirrors ingress of EDGE_IF; impair only src CLOUD_CIDR
  tc qdisc add dev ifb0 root handle 1: htb default 20 r2q 1
  tc class add dev ifb0 parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev ifb0 parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev ifb0 parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev ifb0 parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev ifb0 parent 1:20 handle 20: fq_codel

  tc filter add dev ifb0 parent 1: protocol ip prio 1 \
    flower src_ip "${CLOUD_CIDR}" skip_hw flowid 1:10

  # ifb1 mirrors ingress of CLOUD_IF; impair only src EDGE_CIDR
  tc qdisc add dev ifb1 root handle 1: htb default 20 r2q 1
  tc class add dev ifb1 parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev ifb1 parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev ifb1 parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev ifb1 parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev ifb1 parent 1:20 handle 20: fq_codel

  tc filter add dev ifb1 parent 1: protocol ip prio 1 \
    flower src_ip "${EDGE_CIDR}" skip_hw flowid 1:10
}