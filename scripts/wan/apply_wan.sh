#!/usr/bin/env bash
set -euo pipefail
PROFILE="${1:-good}"  # good|moderate|poor|clear
LOG_DIR="${LOG_DIR:-/var/log/wan-profiles}"
mkdir -p "$LOG_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case "$PROFILE" in
  good)     "$SCRIPT_DIR/wan_profile_good.sh" ;;
  moderate) "$SCRIPT_DIR/wan_profile_moderate.sh" ;;
  poor)     "$SCRIPT_DIR/wan_profile_poor.sh" ;;
  clear)    "$SCRIPT_DIR/wan_clear.sh" ;;
  *) echo "Unknown profile: $PROFILE" >&2; exit 1 ;;
esac

ts="$(date -Iseconds)"
echo "$ts profile=$PROFILE EDGE_IF=${EDGE_IF:-?} CLOUD_IF=${CLOUD_IF:-?}" | tee -a "$LOG_DIR/history.log"