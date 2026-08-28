#!/usr/bin/env bash
set -euo pipefail
DB="ghazali1_ReyTOption"
PY="/opt/reyt/venv/bin/python"
STRAT_DIR="/opt/reyt/strategy"

printf '%s\n' "Stopping Strategy and Bale (Collector/history remain untouched)..."
systemctl stop reyt-bale.service || true
systemctl stop reyt-strategy.service || true
systemctl reset-failed reyt-bale.service reyt-strategy.service || true

printf '%s\n' "Resetting ONLY paper/signal/runtime trading state in ${DB}..."
mysql "$DB" <<'SQL'
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE paper_position_valuations;
TRUNCATE TABLE paper_position_legs;
TRUNCATE TABLE paper_positions;
TRUNCATE TABLE paper_account_equity_history;
TRUNCATE TABLE covered_call_signals;
TRUNCATE TABLE protective_put_signals;
TRUNCATE TABLE bull_call_spread_signals;
TRUNCATE TABLE bear_put_spread_signals;
TRUNCATE TABLE long_straddle_signals;
TRUNCATE TABLE engine_runs;
TRUNCATE TABLE paper_strategy_account;
SET FOREIGN_KEY_CHECKS=1;
SQL

rm -f /opt/reyt/bale/reyt_bale_notifier_state.sqlite3 \
      /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-shm \
      /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-wal

printf '%s\n' "Initializing fresh configured paper account (NO scan/trade)..."
cd "$STRAT_DIR"
sudo -u reyt env \
  OPTIONS_CONFIG_FILE=/opt/reyt/strategy/settings.ini \
  PYTHONPATH="$STRAT_DIR" \
  PYTHONUNBUFFERED=1 \
  "$PY" /opt/reyt/strategy/initialize_paper_account.py

printf '%s\n' "TRADING_STATE_RESET_OK"
mysql -NBe "SELECT 'paper_positions',COUNT(*) FROM ${DB}.paper_positions UNION ALL SELECT 'daily_market_data',COUNT(*) FROM ${DB}.daily_market_data UNION ALL SELECT 'underlying_assets',COUNT(*) FROM ${DB}.underlying_assets UNION ALL SELECT 'option_contracts',COUNT(*) FROM ${DB}.option_contracts;"
