#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB="${MYSQL_DATABASE:-ghazali1_ReyTOption}"
CFG_DIR="/etc/reyt/short-premium"
CFG_FILE="${CFG_DIR}/settings.ini"
STATE_DIR="/var/lib/reyt/short-premium"

echo "== ReyT Short Premium isolated install =="

# Only stop the NEW services. Existing collector/strategy/notifier remain untouched.
systemctl stop reyt-short-premium-notifier.service 2>/dev/null || true
systemctl stop reyt-short-premium.service 2>/dev/null || true

install -d -o root -g reyt -m 0750 "$CFG_DIR"
install -d -o reyt -g reyt -m 0750 "$STATE_DIR"

if [[ ! -f "$CFG_FILE" ]]; then
  install -o root -g reyt -m 0640 \
    "$ROOT_DIR/config/SHORT_PREMIUM_SETTINGS_REFERENCE.ini" "$CFG_FILE"
  echo "Created $CFG_FILE from reference."
  echo "IMPORTANT: add MYSQL_PASSWORD and the NEW Telegram BOT_TOKEN / CHAT_ID before start."
else
  echo "Preserving existing $CFG_FILE (secrets/config not overwritten)."
fi

echo "Applying additive short-premium schema only..."
mysql "$DB" < "$ROOT_DIR/database/02_create_short_premium_isolated.sql"

install -o root -g root -m 0644 \
  "$ROOT_DIR/systemd/reyt-short-premium.service" \
  /etc/systemd/system/reyt-short-premium.service
install -o root -g root -m 0644 \
  "$ROOT_DIR/systemd/reyt-short-premium-notifier.service" \
  /etc/systemd/system/reyt-short-premium-notifier.service

systemctl daemon-reload
systemctl enable reyt-short-premium.service
systemctl enable reyt-short-premium-notifier.service

echo
echo "Installed and enabled NEW services only."
echo "Existing reyt-collector/reyt-strategy/reyt-notifier were not changed."
echo
echo "Next:"
echo "  1) edit $CFG_FILE"
echo "  2) $ROOT_DIR/tools/verify_short_premium.sh"
echo "  3) sudo -u reyt env SHORT_PREMIUM_CONFIG_FILE=$CFG_FILE /opt/reyt-venv/bin/python $ROOT_DIR/strategy/ReyT_short_premium_engine.py --once"
echo "  4) systemctl start reyt-short-premium.service"
echo "  5) systemctl start reyt-short-premium-notifier.service"
