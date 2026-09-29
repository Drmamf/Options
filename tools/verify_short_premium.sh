#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB="${MYSQL_DATABASE:-ghazali1_ReyTOption}"
CFG="${SHORT_PREMIUM_CONFIG_FILE:-/etc/reyt/short-premium/settings.ini}"
PY="/opt/reyt-venv/bin/python"

echo "========== OLD SERVICES (must remain independent) =========="
for s in reyt-collector reyt-strategy reyt-notifier; do
  printf '%-28s active=%-10s enabled=%s\n' \
    "$s" "$(systemctl is-active "$s.service" 2>/dev/null || true)" \
    "$(systemctl is-enabled "$s.service" 2>/dev/null || true)"
done

echo
echo "========== NEW SERVICES =========="
for s in reyt-short-premium reyt-short-premium-notifier; do
  printf '%-28s active=%-10s enabled=%s\n' \
    "$s" "$(systemctl is-active "$s.service" 2>/dev/null || true)" \
    "$(systemctl is-enabled "$s.service" 2>/dev/null || true)"
done

echo
echo "========== PYTHON SYNTAX =========="
"$PY" -m py_compile "$ROOT_DIR/strategy/ReyT_short_premium_engine.py"
"$PY" -m py_compile "$ROOT_DIR/bale/ReyT_short_premium_telegram_notifier.py"
echo "SYNTAX_OK"

echo
echo "========== CORE MATH SELFTEST =========="
cd "$ROOT_DIR/strategy"
PYTHONPATH="$ROOT_DIR/strategy" "$PY" "$ROOT_DIR/tools/selftest_short_premium_math.py"

echo
echo "========== ISOLATED TABLES =========="
mysql "$DB" <<'SQL'
SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA=DATABASE()
  AND TABLE_NAME IN (
    'short_premium_accounts',
    'short_premium_positions',
    'short_premium_legs',
    'short_premium_fills',
    'short_premium_valuations',
    'short_premium_events',
    'short_premium_engine_runs',
    'short_straddle_signals',
    'short_strangle_signals'
  )
ORDER BY TABLE_NAME;

SELECT account_name,strategy_code,
       initial_equity_rial/10 AS initial_toman,
       entry_bucket_target_rial/10 AS entry_target_toman,
       adjustment_bucket_target_rial/10 AS adjustment_target_toman
FROM short_premium_accounts
ORDER BY strategy_code;
SQL

echo
echo "========== CONFIG =========="
test -f "$CFG" || { echo "MISSING_CONFIG: $CFG"; exit 1; }
grep -E '^(STRADDLE_ACCOUNT_NAME|STRANGLE_ACCOUNT_NAME|INITIAL_CAPITAL_TOMAN|MIN_DTE|MAX_DTE|ENTRY_BUCKET_PCT|ADJUSTMENT_BUCKET_PCT|MIN_POSITION_VALUE_TOMAN|MAX_POSITION_MARGIN_TOMAN|MIN_NET_PREMIUM_TO_MARGIN_PCT|MAX_IMMEDIATE_CLOSE_LOSS_PCT|STRADDLE_STRESS_PCT|STRANGLE_STRESS_PCT|MAX_LOSS_ZONE_DISTANCE_PCT)[[:space:]]*=' "$CFG"

"$PY" - "$CFG" <<'PYCFG'
import configparser
import sys

cfg = configparser.ConfigParser(interpolation=None, strict=False)
cfg.read(sys.argv[1], encoding="utf-8")

token = cfg.get("short_premium_bale", "BOT_TOKEN", fallback="").strip()
chat = cfg.get("short_premium_bale", "CHAT_ID", fallback="").strip()

if bool(token) ^ bool(chat):
    print("SHORT_PREMIUM_BALE=INCOMPLETE_CONFIG")
    raise SystemExit(1)

if token and chat:
    print("SHORT_PREMIUM_BALE=CONFIGURED")
else:
    print("SHORT_PREMIUM_BALE=DISABLED_NOT_CONFIGURED")
PYCFG

echo
echo "========== LEGACY SCHEMA SAFETY =========="
if grep -Eiq 'DROP[[:space:]]+TABLE|TRUNCATE[[:space:]]+TABLE|ALTER[[:space:]]+TABLE[[:space:]]+(paper_|covered_call|protective_put|bull_call|bear_put|long_straddle)' \
  "$ROOT_DIR/database/02_create_short_premium_isolated.sql"; then
  echo "LEGACY_SCHEMA_SAFETY_FAILED"
  exit 1
fi
echo "LEGACY_SCHEMA_UNTOUCHED_OK"

echo
echo "SHORT_PREMIUM_VERIFY_OK"
