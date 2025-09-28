#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/wan_lib.sh"

detect_ifaces
clear_qdisc

RATE=10
RTT=200
LOSS=2.0
JITTER=20

ONE_WAY=$(( RTT / 2 ))

# Egress: only edge->cloud and cloud->edge
apply_shape_selective "$RATE" "$ONE_WAY" "$JITTER" "$LOSS"

# Ingress: mirror with IFB, only cloud->edge and edge->cloud
setup_ifb
apply_ingress_shape_selective "$RATE" "$ONE_WAY" "$JITTER" "$LOSS"

show_qdisc
echo "Applied GOOD selective profile (egress+ingress): ${RATE}Mbit, RTT~${RTT}ms, loss ${LOSS}%"