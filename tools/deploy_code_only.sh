#!/usr/bin/env bash
set -Eeuo pipefail

REPO="/opt/reyt-github"
LIVE="/opt/reyt"
PYTHON="/opt/reyt/venv/bin/python"
BACKUP_ROOT="/opt/reyt-code-backups"
STAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="$BACKUP_ROOT/$STAMP"

FILES=(
  "collector/ReyT_collector_unified_optimized.py"
  "strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py"
  "strategy/ReyT_strategy_engine_covered_call_only.py"
  "bale/ReyT_bale_notifier_unified.py"
)

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: run as root"
  exit 1
fi

cd "$REPO"

# Never deploy from a dirty Git working tree.
if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "ERROR: /opt/reyt-github has uncommitted changes."
  exit 1
fi

# Syntax-check source files without importing or executing them.
for rel in "${FILES[@]}"; do
  "$PYTHON" - "$REPO/$rel" <<'PY'
import sys
from pathlib import Path

p = Path(sys.argv[1])
source = p.read_text(encoding="utf-8")
compile(source, str(p), "exec")
print(f"SYNTAX_OK: {p}")
PY
done

CHANGED=()

for rel in "${FILES[@]}"; do
  if ! cmp -s "$REPO/$rel" "$LIVE/$rel"; then
    CHANGED+=("$rel")
  fi
done

if [[ ${#CHANGED[@]} -eq 0 ]]; then
  echo "NO_CODE_CHANGES"
  exit 0
fi

echo "Files to deploy:"
printf '  %s\n' "${CHANGED[@]}"

# Backup ONLY the live code files that are about to change.
for rel in "${CHANGED[@]}"; do
  mkdir -p "$BACKUP_DIR/$(dirname "$rel")"
  cp -a "$LIVE/$rel" "$BACKUP_DIR/$rel"
done

rollback() {
  trap - ERR
  echo "DEPLOY FAILED - restoring code backup from $BACKUP_DIR"

  for rel in "${CHANGED[@]}"; do
    cp -a "$BACKUP_DIR/$rel" "$LIVE/$rel"
  done

  systemctl restart reyt-collector.service || true
  systemctl restart reyt-strategy.service || true
  systemctl restart reyt-bale.service || true

  echo "ROLLBACK_FINISHED"
  exit 1
}

trap rollback ERR

# Copy code only. No settings, database, venv or systemd files.
for rel in "${CHANGED[@]}"; do
  case "$rel" in
    collector/*)
      install -o reyt -g reyt -m 0644 "$REPO/$rel" "$LIVE/$rel"
      ;;
    strategy/*|bale/*)
      install -o reyt -g reyt -m 0750 "$REPO/$rel" "$LIVE/$rel"
      ;;
  esac
done

# Restart only services whose code changed.
COLLECTOR_CHANGED=0
STRATEGY_CHANGED=0
BALE_CHANGED=0

for rel in "${CHANGED[@]}"; do
  case "$rel" in
    collector/*) COLLECTOR_CHANGED=1 ;;
    strategy/*) STRATEGY_CHANGED=1 ;;
    bale/*) BALE_CHANGED=1 ;;
  esac
done

if [[ $COLLECTOR_CHANGED -eq 1 ]]; then
  systemctl restart reyt-collector.service
fi

if [[ $STRATEGY_CHANGED -eq 1 ]]; then
  systemctl restart reyt-strategy.service
fi

if [[ $BALE_CHANGED -eq 1 ]]; then
  systemctl restart reyt-bale.service
fi

sleep 3

if [[ $COLLECTOR_CHANGED -eq 1 ]]; then
  systemctl is-active --quiet reyt-collector.service
fi

if [[ $STRATEGY_CHANGED -eq 1 ]]; then
  systemctl is-active --quiet reyt-strategy.service
fi

if [[ $BALE_CHANGED -eq 1 ]]; then
  systemctl is-active --quiet reyt-bale.service
fi

trap - ERR

echo "CODE_ONLY_DEPLOY_OK"
echo "Backup: $BACKUP_DIR"
