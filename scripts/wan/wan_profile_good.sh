#!/usr/bin/env bash
set -euo pipefail
# Usage: EDGE_IF=ens18 CLOUD_IF=ens19 ./wan_profile_good.sh
# Or export EDGE_IP/CLOUD_IP for auto-detect.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=wan_lib.sh
. "$DIR/wan_lib.sh"

detect_ifaces
clear_qdisc

RATE=200     # Mbit/s
RTT=20       # ms (total round-trip)
LOSS=0.1     # %
JITTER=2     # ms (per direction)

ONE_WAY=$(( RTT / 2 ))

# Egress: only edge->cloud and cloud->edge
apply_shape_simple "$RATE" "$ONE_WAY" "$JITTER" "$LOSS"

show_qdisc
echo "Applied GOOD profile: ${RATE}Mbit, RTT~${RTT}ms, loss ${LOSS}%"