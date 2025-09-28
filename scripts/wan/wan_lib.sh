#!/usr/local/sbin/wan/wan_lib.sh
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
  tc qdisc del dev "$EDGE_IF"  root 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" root 2>/dev/null || true
  # Ingress qdisc + IFBs (if they exist)
  tc qdisc del dev "$EDGE_IF"  ingress 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" ingress 2>/dev/null || true
  for IFB in ifb0 ifb1; do
    ip link set "$IFB" down 2>/dev/null || true
    tc qdisc del dev "$IFB" root 2>/dev/null || true
    ip link del "$IFB" 2>/dev/null || true
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

# Keep your existing apply_shape if you want the "all egress" mode.
# Below we add selective egress shaping only for inter-cluster flows.
apply_shape_selective() {
  local rate_mbit="$1"   # Mbit/s
  local one_way_ms="$2"  # ms
  local jitter_ms="$3"   # ms
  local loss_pct="$4"    # %

  # EDGE_IF -> impair only dst CLOUD_CIDR
  tc qdisc del dev "$EDGE_IF" root 2>/dev/null || true
  tc qdisc add dev "$EDGE_IF" root handle 1: htb default 20
  tc class add dev "$EDGE_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$EDGE_IF" parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$EDGE_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$EDGE_IF" parent 1:20 handle 20: fq_codel
  tc filter add dev "$EDGE_IF" protocol ip parent 1: prio 1 u32 match ip dst "${CLOUD_CIDR}" flowid 1:10

  # CLOUD_IF -> impair only dst EDGE_CIDR
  tc qdisc del dev "$CLOUD_IF" root 2>/dev/null || true
  tc qdisc add dev "$CLOUD_IF" root handle 1: htb default 20
  tc class add dev "$CLOUD_IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev "$CLOUD_IF" parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev "$CLOUD_IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev "$CLOUD_IF" parent 1:20 handle 20: fq_codel
  tc filter add dev "$CLOUD_IF" protocol ip parent 1: prio 1 u32 match ip dst "${EDGE_CIDR}" flowid 1:10
}

# Setup IFB devices and redirect ingress to them
setup_ifb() {
  # Requires ifb kernel module
  modprobe ifb numifbs=2 >/dev/null 2>&1 || true

  ip link add ifb0 type ifb 2>/dev/null || true
  ip link add ifb1 type ifb 2>/dev/null || true
  ip link set ifb0 up
  ip link set ifb1 up

  # Redirect ingress of EDGE_IF to ifb0
  tc qdisc del dev "$EDGE_IF" ingress 2>/dev/null || true
  tc qdisc add dev "$EDGE_IF" handle ffff: ingress
  tc filter add dev "$EDGE_IF" parent ffff: protocol ip u32 match u32 0 0 \
    action mirred egress redirect dev ifb0

  # Redirect ingress of CLOUD_IF to ifb1
  tc qdisc del dev "$CLOUD_IF" ingress 2>/dev/null || true
  tc qdisc add dev "$CLOUD_IF" handle ffff: ingress
  tc filter add dev "$CLOUD_IF" parent ffff: protocol ip u32 match u32 0 0 \
    action mirred egress redirect dev ifb1
}

# Apply selective ingress shaping on IFBs (mirror of egress logic)
apply_ingress_shape_selective() {
  local rate_mbit="$1"   # Mbit/s
  local one_way_ms="$2"  # ms
  local jitter_ms="$3"   # ms
  local loss_pct="$4"    # %

  # Ingress to EDGE_IF arrives on ifb0; impair only packets coming FROM CLOUD_CIDR
  tc qdisc del dev ifb0 root 2>/dev/null || true
  tc qdisc add dev ifb0 root handle 1: htb default 20
  tc class add dev ifb0 parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev ifb0 parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev ifb0 parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev ifb0 parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev ifb0 parent 1:20 handle 20: fq_codel
  tc filter add dev ifb0 parent 1: protocol ip prio 1 u32 match ip src "${CLOUD_CIDR}" flowid 1:10

  # Ingress to CLOUD_IF arrives on ifb1; impair only packets coming FROM EDGE_CIDR
  tc qdisc del dev ifb1 root 2>/dev/null || true
  tc qdisc add dev ifb1 root handle 1: htb default 20
  tc class add dev ifb1 parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  tc class add dev ifb1 parent 1: classid 1:20 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
  if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
    tc qdisc add dev ifb1 parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
  else
    tc qdisc add dev ifb1 parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
  fi
  tc qdisc add dev ifb1 parent 1:20 handle 20: fq_codel
  tc filter add dev ifb1 parent 1: protocol ip prio 1 u32 match ip src "${EDGE_CIDR}" flowid 1:10
}