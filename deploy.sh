#!/bin/bash
# Deploy gwmsmon to vocms860.cern.ch
#
# Usage: ./deploy.sh [--restart]
#   --restart   restart collector and web after sync (default: sync only)
#
# What it does:
#   1. Syncs src/gwmsmon/ and systemd/ to the server with correct permissions
#   2. Clears __pycache__ so new code is picked up
#   3. Optionally stops services, kills orphans, installs service files,
#      starts services via systemd, verifies health

set -euo pipefail

HOST="gwmsmon@vocms860.cern.ch"
REMOTE_BASE="/opt/gwmsmon"
REMOTE_SRC="$REMOTE_BASE/src/gwmsmon"
LOCAL_SRC="$(dirname "$0")/src/gwmsmon"
LOCAL_SYSTEMD="$(dirname "$0")/systemd"
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

echo "==> Syncing systemd service files"
rsync -rlpt --chmod=Fo+r \
  "$LOCAL_SYSTEMD/" "$HOST:$REMOTE_BASE/systemd/"

# --- 2. Clear bytecode cache ---
echo "==> Clearing __pycache__"
ssh "$HOST" "find $REMOTE_SRC -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true"

echo "==> Sync complete"

if [ "$RESTART" = false ]; then
  echo "Use --restart to restart collector and web"
  exit 0
fi

# --- 3. Stop services and kill orphans ---
echo "==> Stopping services"
ssh "$HOST" bash <<'STOP'
sudo systemctl stop gwmsmon-collect gwmsmon-web 2>/dev/null || true

# Kill ANY orphan gwmsmon processes (from old setsid deploys)
orphans=$(pgrep -u gwmsmon -f 'gwmsmon\.(collector|web)' || true)
if [ -n "$orphans" ]; then
  echo "  Killing orphan PIDs: $orphans"
  kill -9 $orphans 2>/dev/null || true
  sleep 1
fi

# Release port 5000 if still held
sudo fuser -k 5000/tcp 2>/dev/null || true
sleep 1

# Verify clean
remaining=$(pgrep -u gwmsmon -f 'gwmsmon\.(collector|web)' || true)
if [ -n "$remaining" ]; then
  echo "  ERROR: processes still running: $remaining"
  exit 1
fi
echo "  All processes stopped"
STOP

# --- 4. Install service files if changed ---
echo "==> Installing service files"
ssh "$HOST" bash <<INSTALL
RELOAD=false
for svc in gwmsmon-collect gwmsmon-web; do
  if ! diff -q "$REMOTE_BASE/systemd/\$svc.service" "/etc/systemd/system/\$svc.service" >/dev/null 2>&1; then
    echo "  Updating \$svc.service"
    sudo cp "$REMOTE_BASE/systemd/\$svc.service" "/etc/systemd/system/\$svc.service"
    RELOAD=true
  fi
done
if [ "\$RELOAD" = true ]; then
  echo "  Reloading systemd daemon"
  sudo systemctl daemon-reload
fi
INSTALL

# --- 5. Start services ---
echo "==> Starting services"
ssh "$HOST" bash <<'START'
sudo systemctl start gwmsmon-collect
sudo systemctl start gwmsmon-web
START

# --- 6. Verify ---
echo "==> Verifying services"
ssh "$HOST" bash <<'VERIFY'
ok=true
if sudo systemctl is-active --quiet gwmsmon-collect; then
  echo "  collector: active"
else
  echo "  ERROR: collector not running"
  sudo journalctl -u gwmsmon-collect -n 20 --no-pager
  ok=false
fi
if sudo systemctl is-active --quiet gwmsmon-web; then
  echo "  web: active"
else
  echo "  ERROR: web not running"
  sudo journalctl -u gwmsmon-web -n 20 --no-pager
  ok=false
fi
[ "$ok" = true ] || exit 1
VERIFY

# --- 7. Health check web ---
echo "==> Health check"
sleep 2
http_code=$(ssh "$HOST" "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:5000/prodview/")
if [ "$http_code" = "200" ]; then
  echo "  Web OK (HTTP $http_code)"
else
  echo "  WARNING: web returned HTTP $http_code"
fi

echo "==> Deploy complete"
