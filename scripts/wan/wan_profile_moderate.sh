#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/wan_lib.sh"

detect_ifaces
clear_qdisc

RATE=50
RTT=80
LOSS=0.5
JITTER=8

ONE_WAY=$(( RTT / 2 ))

# Egress: only edge->cloud and cloud->edge
apply_shape_simple "$RATE" "$ONE_WAY" "$JITTER" "$LOSS"

show_qdisc
echo "Applied MODERATE profile: ${RATE}Mbit, RTT~${RTT}ms, loss ${LOSS}%"