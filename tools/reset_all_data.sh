#!/usr/bin/env bash
set -euo pipefail
DB="ghazali1_ReyTOption"

echo "WARNING: DESTRUCTIVE FULL RESET — this deletes market/history tables too."
echo "Preferred normal reset: /opt/reyt/tools/reset_trading_state_preserve_market.sh"

echo "Stopping ReyT services..."
systemctl stop reyt-bale.service || true
systemctl stop reyt-strategy.service || true
systemctl stop reyt-collector.service || true
systemctl reset-failed reyt-bale.service reyt-strategy.service reyt-collector.service || true

echo "Truncating ALL ReyT runtime/data tables in ${DB}..."
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
TRUNCATE TABLE option_greeks;
TRUNCATE TABLE order_book_depth;
TRUNCATE TABLE market_data_ticks;
TRUNCATE TABLE daily_market_data;
TRUNCATE TABLE history_sync_state;
TRUNCATE TABLE collector_state;
TRUNCATE TABLE option_contracts;
TRUNCATE TABLE underlying_assets;
SET FOREIGN_KEY_CHECKS=1;
SQL

rm -f /opt/reyt/bale/reyt_bale_notifier_state.sqlite3 \
      /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-shm \
      /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-wal

echo "RESET_ALL_OK"
mysql -NBe "SELECT 'underlying_assets',COUNT(*) FROM ${DB}.underlying_assets UNION ALL SELECT 'option_contracts',COUNT(*) FROM ${DB}.option_contracts UNION ALL SELECT 'daily_market_data',COUNT(*) FROM ${DB}.daily_market_data UNION ALL SELECT 'paper_positions',COUNT(*) FROM ${DB}.paper_positions;"
