#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "\${BASH_SOURCE[0]}")/.." && pwd)"
DB="\${MYSQL_DATABASE:-ghazali1_ReyTOption}"
STATE_DB="/var/lib/reyt/short-premium/notifier_state.sqlite3"

echo "Stopping ONLY short-premium services..."
systemctl stop reyt-short-premium-notifier.service 2>/dev/null || true
systemctl stop reyt-short-premium.service 2>/dev/null || true

echo "Resetting ONLY isolated short-premium paper state..."
mysql "$DB" <<'SQL'
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE short_premium_valuations;
TRUNCATE TABLE short_premium_fills;
TRUNCATE TABLE short_premium_events;
TRUNCATE TABLE short_premium_legs;
TRUNCATE TABLE short_premium_positions;
TRUNCATE TABLE short_straddle_signals;
TRUNCATE TABLE short_strangle_signals;
TRUNCATE TABLE short_premium_engine_runs;
TRUNCATE TABLE short_premium_accounts;
SET FOREIGN_KEY_CHECKS=1;
SQL

rm -f "$STATE_DB" "$STATE_DB-shm" "$STATE_DB-wal"

echo "Recreating only short-premium accounts/schema state..."
mysql "$DB" < "$ROOT_DIR/database/02_create_short_premium_isolated.sql"

echo "SHORT_PREMIUM_STATE_RESET_OK"
echo "Legacy ReyT paper tables and collector market data were not touched."
