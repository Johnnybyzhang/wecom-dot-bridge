#!/bin/sh
# Operator CLI. Always runs as the service identity so state files keep its ownership.
set -eu
export PYTHONPATH=/opt/dot-bridge PYTHONDONTWRITEBYTECODE=1 DOT_BRIDGE_STATE=/var/lib/dot-bridge
if [ "$(id -u)" = 0 ]; then
  exec runuser -u dot-bridge -- env PYTHONPATH="$PYTHONPATH" \
    PYTHONDONTWRITEBYTECODE=1 DOT_BRIDGE_STATE="$DOT_BRIDGE_STATE" \
    /usr/bin/python3 -P -m dot_bridge "$@"
fi
exec /usr/bin/python3 -P -m dot_bridge "$@"
