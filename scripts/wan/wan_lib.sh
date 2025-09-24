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
  tc qdisc del dev "$EDGE_IF"  root 2>/dev/null || true
  tc qdisc del dev "$CLOUD_IF" root 2>/dev/null || true
}

apply_shape() {
  local rate_mbit="$1"     # Mbit/s
  local one_way_ms="$2"    # ms
  local jitter_ms="$3"     # ms
  local loss_pct="$4"      # %
  for IF in "$EDGE_IF" "$CLOUD_IF"; do
    tc qdisc add dev "$IF" root handle 1: htb default 10
    tc class add dev "$IF" parent 1: classid 1:10 htb rate "${rate_mbit}mbit" ceil "${rate_mbit}mbit"
    if [[ "$jitter_ms" = "0" || "$jitter_ms" = "0.0" ]]; then
      tc qdisc add dev "$IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" loss "${loss_pct}%"
    else
      tc qdisc add dev "$IF" parent 1:10 handle 10: netem delay "${one_way_ms}ms" "${jitter_ms}ms" distribution normal loss "${loss_pct}%"
    fi
  done
}

show_qdisc() {
  echo "== qdisc $EDGE_IF =="; tc qdisc show dev "$EDGE_IF" || true
  echo "== qdisc $CLOUD_IF =="; tc qdisc show dev "$CLOUD_IF" || true
}