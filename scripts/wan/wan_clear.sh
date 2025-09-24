#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/wan_lib.sh"

detect_ifaces
clear_qdisc
show_qdisc
echo "Cleared WAN shaping (best-effort)."