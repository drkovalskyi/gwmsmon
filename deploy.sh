#!/bin/bash
# Deploy gwmsmon2 to vocms860.cern.ch
#
# Usage: ./deploy.sh [--restart]
#   --restart   restart collector and web after sync (default: sync only)
#
# What it does:
#   1. Syncs src/gwmsmon/ to the server with correct permissions
#   2. Clears __pycache__ so new code is picked up
#   3. Optionally stops old processes, starts new ones, verifies health

set -euo pipefail

HOST="gwmsmon@vocms860.cern.ch"
REMOTE_BASE="/opt/gwmsmon2"
REMOTE_SRC="$REMOTE_BASE/src/gwmsmon"
LOCAL_SRC="$(dirname "$0")/src/gwmsmon"
RESTART=false

for arg in "$@"; do
  case "$arg" in
    --restart) RESTART=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# --- 1. Sync source ---
echo "==> Syncing source to $HOST:$REMOTE_SRC"
rsync -rlpt --delete --chmod=Do+rx,Fo+r \
  --exclude='__pycache__' \
  "$LOCAL_SRC/" "$HOST:$REMOTE_SRC/"

# --- 2. Clear bytecode cache ---
echo "==> Clearing __pycache__"
ssh "$HOST" "rm -rf $REMOTE_SRC/__pycache__"

echo "==> Sync complete"

if [ "$RESTART" = false ]; then
  echo "Use --restart to restart collector and web"
  exit 0
fi

# --- 3. Stop existing processes ---
echo "==> Stopping existing processes"

# Collector — send SIGTERM (graceful), wait, then SIGKILL if needed
ssh "$HOST" bash <<'STOP'
pids=$(pgrep -u gwmsmon -f 'gwmsmon\.collector' || true)
if [ -n "$pids" ]; then
  echo "  Killing collector PIDs: $pids"
  kill $pids 2>/dev/null || true
  for i in $(seq 1 10); do
    sleep 1
    remaining=$(pgrep -u gwmsmon -f 'gwmsmon\.collector' || true)
    [ -z "$remaining" ] && break
    if [ "$i" -eq 10 ]; then
      echo "  Force killing: $remaining"
      kill -9 $remaining 2>/dev/null || true
      sleep 1
    fi
  done
fi

pids=$(pgrep -u gwmsmon -f 'gwmsmon\.web' || true)
if [ -n "$pids" ]; then
  echo "  Killing web PIDs: $pids"
  kill $pids 2>/dev/null || true
  sleep 2
  remaining=$(pgrep -u gwmsmon -f 'gwmsmon\.web' || true)
  if [ -n "$remaining" ]; then
    kill -9 $remaining 2>/dev/null || true
    sleep 1
  fi
fi

# Verify clean
remaining=$(pgrep -u gwmsmon -f 'gwmsmon\.(collector|web)' || true)
if [ -n "$remaining" ]; then
  echo "  ERROR: processes still running: $remaining"
  exit 1
fi
echo "  All processes stopped"
STOP

# --- 4. Start new processes ---
echo "==> Starting collector"
ssh -f "$HOST" "cd $REMOTE_BASE && PYTHONPATH=src setsid /usr/bin/python3 -m gwmsmon.collector --verbose > /tmp/collector.log 2>&1 < /dev/null"
sleep 2

echo "==> Starting web"
ssh -f "$HOST" "cd $REMOTE_BASE && PYTHONPATH=src setsid /usr/bin/python3 -m gwmsmon.web --host 127.0.0.1 --port 5000 > /tmp/web.log 2>&1 < /dev/null"
sleep 2

# --- 5. Verify ---
echo "==> Verifying processes"
procs=$(ssh "$HOST" "pgrep -u gwmsmon -af 'gwmsmon\.(collector|web)' | grep -v bash || true")
collector_ok=false
web_ok=false
while IFS= read -r line; do
  [[ "$line" == *gwmsmon.collector* ]] && collector_ok=true
  [[ "$line" == *gwmsmon.web* ]] && web_ok=true
  echo "  $line"
done <<< "$procs"

if [ "$collector_ok" = false ]; then
  echo "  ERROR: collector not running"
  ssh "$HOST" "tail -20 /tmp/collector.log" 2>/dev/null || true
  exit 1
fi
if [ "$web_ok" = false ]; then
  echo "  ERROR: web not running"
  ssh "$HOST" "tail -20 /tmp/web.log" 2>/dev/null || true
  exit 1
fi

# --- 6. Health check web ---
echo "==> Health check"
http_code=$(ssh "$HOST" "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:5000/prodview/")
if [ "$http_code" = "200" ]; then
  echo "  Web OK (HTTP $http_code)"
else
  echo "  WARNING: web returned HTTP $http_code"
  ssh "$HOST" "tail -10 /tmp/web.log" 2>/dev/null || true
fi

echo "==> Deploy complete"
