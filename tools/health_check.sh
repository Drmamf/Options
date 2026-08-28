#!/usr/bin/env bash
set -euo pipefail
DB="ghazali1_ReyTOption"

echo "========== SERVICES =========="
for s in reyt-collector reyt-strategy reyt-bale; do
  printf '%-18s active=%-10s enabled=%s\n' "$s" "$(systemctl is-active "$s.service" || true)" "$(systemctl is-enabled "$s.service" || true)"
done

echo
echo "========== DATABASE =========="
mysql "$DB" <<'SQL'
SELECT 'underlying_assets' AS item, COUNT(*) AS n FROM underlying_assets
UNION ALL SELECT 'option_contracts', COUNT(*) FROM option_contracts
UNION ALL SELECT 'market_data_ticks', COUNT(*) FROM market_data_ticks
UNION ALL SELECT 'order_book_depth', COUNT(*) FROM order_book_depth
UNION ALL SELECT 'option_greeks', COUNT(*) FROM option_greeks
UNION ALL SELECT 'daily_market_data', COUNT(*) FROM daily_market_data
UNION ALL SELECT 'paper_strategy_account', COUNT(*) FROM paper_strategy_account
UNION ALL SELECT 'paper_positions', COUNT(*) FROM paper_positions;

SELECT account_name,
       initial_equity_rial/10 AS initial_toman,
       current_equity_rial/10 AS current_toman,
       (initial_equity_rial + realized_pnl_rial - allocated_capital_rial)/10 AS available_cash_toman,
       allocated_capital_rial/10 AS allocated_toman,
       reserved_risk_rial/10 AS reserved_max_loss_toman,
       realized_pnl_rial/10 AS realized_pnl_toman,
       unrealized_pnl_rial/10 AS unrealized_pnl_toman,
       open_positions_count
FROM paper_strategy_account;
SQL

echo
echo "========== RECENT ERRORS =========="
n=0
for u in reyt-collector.service reyt-strategy.service reyt-bale.service; do
  echo "--- $u ---"
  out="$(journalctl -u "$u" --since '30 minutes ago' --no-pager | grep -Ei 'traceback|exception|critical|failed|error([^s]|$)|errors=[1-9][0-9]*' || true)"
  if [[ -n "$out" ]]; then
    printf '%s\n' "$out" | tail -n 30
    n=$((n+1))
  else
    echo "NO_ERRORS"
  fi
done
[[ "$n" -eq 0 ]] && echo "HEALTH_CHECK_NO_RECENT_ERRORS"
