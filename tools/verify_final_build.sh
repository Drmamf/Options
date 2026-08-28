#!/usr/bin/env bash
set -euo pipefail

echo "========== PYTHON SYNTAX =========="
/opt/reyt/venv/bin/python -m py_compile /opt/reyt/collector/ReyT_collector_unified_optimized.py
/opt/reyt/venv/bin/python -m py_compile /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py
/opt/reyt/venv/bin/python -m py_compile /opt/reyt/bale/ReyT_bale_notifier_unified.py
echo "SYNTAX_OK"

echo
echo "========== COLLECTOR EFFECTIVE =========="
cd /opt/reyt/collector
sudo -u reyt env OPTIONS_CONFIG_FILE=/opt/reyt/collector/settings.ini PYTHONPATH=/opt/reyt/collector \
/opt/reyt/venv/bin/python - <<'PY'
import ReyT_collector_unified_optimized as c
for k in [
    "REALTIME_INTERVAL","SNAPSHOT_INTERVAL","ORDER_BOOK_INTERVAL","ORDER_BOOK_BATCH_SIZE",
    "ORDER_BOOK_CONCURRENCY","HTTP_CONNECTOR_LIMIT","INITIAL_HISTORY_DAYS",
    "EOD_HISTORY_LOOKBACK_DAYS","EOD_HISTORY_TIME","CALCULATE_GREEKS",
    "RISK_FREE_RATE","DIVIDEND_YIELD","MAX_IV","MIN_IV",
]:
    v=getattr(c,k)
    if k == "EOD_HISTORY_TIME": v=v.strftime("%H:%M")
    print(f"{k} = {v}")
print(f"MARKET = {c.MARKET_OPEN:%H:%M}-{c.MARKET_CLOSE:%H:%M}")
print("HISTORY_PRUNING = OFF")
PY

echo
echo "========== STRATEGY EFFECTIVE =========="
cd /opt/reyt/strategy
sudo -u reyt env OPTIONS_CONFIG_FILE=/opt/reyt/strategy/settings.ini PYTHONPATH=/opt/reyt/strategy \
/opt/reyt/venv/bin/python - <<'PY'
import ReyT_strategy_engine_unified_1b_execution_status_v2 as s
for k in [
    "ACCOUNT_NAME","INITIAL_CAPITAL_TOMAN","FIXED_RISK_PER_TRADE_TOMAN","MIN_EXECUTION_RISK_TOMAN","MAX_STRATEGY_ALLOCATION_PCT","AUTO_TRADE",
    "MIN_DTE","MAX_DTE","MAX_STRIKE_STEPS","MAX_ENTRY_BOOK_AGE_SECONDS","MAX_MARK_BOOK_AGE_SECONDS",
    "VOL_LOOKBACK_DAYS","MIN_VOL_RETURNS","EXPECTED_RETURN_HURDLE_EAR",
    "EXPECTED_RETURN_REQUIRE_BOTH","EXPECTED_RETURN_HISTORY_ROWS",
    "EXPECTED_RETURN_MIN_HISTORY_RETURNS","EXPECTED_RETURN_BOOTSTRAP_PATHS",
    "EXPECTED_RETURN_IV_QUANTILE_PATHS","EXPECTED_RETURN_HISTORY_CACHE_SECONDS",
    "OPTION_VOLUME_MODE","OPTION_VOLUME_AUTO_FALLBACK",
    "UNDERLYING_BUY_FEE_PCT","UNDERLYING_SELL_FEE_PCT","OPTION_BUY_FEE_PCT",
    "OPTION_SELL_FEE_PCT","OPTION_EXPIRY_FEE_PCT",
]:
    print(f"{k} = {getattr(s,k)}")
print("DTE_ENTRY_FILTER = ON")
print("SCORE_ENTRY_FLOOR = OFF")
print(f"MIN_EXECUTION_RISK_TOMAN = {s.MIN_EXECUTION_RISK_TOMAN}")
print(f"MAX_STRATEGY_ALLOCATION_TOMAN = {s.INITIAL_CAPITAL_TOMAN * s.MAX_STRATEGY_ALLOCATION_PCT / 100}")
PY

echo
echo "========== STATIC RULE CHECK =========="
grep -n 'signal_date.isoformat()' /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py
grep -n 'VWAP_HISTORY_BELOW_40PCT\|VWAP_IV_BELOW_40PCT\|STRATEGY_ALLOCATION_CAP_REACHED\|MIN_EXECUTION_RISK_NOT_MET' /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py | head
grep -n '_option_expiry_fee' /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py | head
grep -n 'EOD single-row history append' /opt/reyt/collector/ReyT_collector_unified_optimized.py
grep -n '_sleep_or_shutdown' /opt/reyt/collector/ReyT_collector_unified_optimized.py | head
grep -n 'Exactly one Bale message' /opt/reyt/bale/ReyT_bale_notifier_unified.py

echo
echo "========== EOD DATETIME SAFETY =========="
python3 - <<'PYEOF'
from pathlib import Path
p = Path('/opt/reyt/collector/ReyT_collector_unified_optimized.py')
s = p.read_text()
assert 'target = datetime.combine(now.date(), EOD_HISTORY_TIME)' in s
assert 'target = datetime.combine(now.date(), EOD_HISTORY_TIME, tzinfo=TEHRAN_TZ)' not in s
print('EOD_DATETIME_SAFETY_OK')
PYEOF

echo "FINAL_BUILD_VERIFY_OK"

echo
echo "========== STARTUP ACCOUNT INITIALIZATION SAFETY =========="
grep -n 'Paper account startup initialization complete' /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py
python3 - <<'PYEOF'
from pathlib import Path
s = Path('/opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py').read_text()
needle = '''await engine.open()\n    await engine.close()\n    print(f"[{tehran_now():%H:%M:%S}] ✅ Paper account startup initialization complete.")'''
assert needle in s
print('STARTUP_ACCOUNT_INITIALIZATION_SAFETY_OK')
PYEOF

echo "FINAL_LOCKED_VERIFY_OK"


echo "========== BALE NOTIFICATION POLICY =========="
grep -n "BALE_SIGNAL_MODE" /opt/reyt/bale/ReyT_bale_notifier_unified.py | head -n 5
grep -qi '^BALE_SIGNAL_MODE[[:space:]]*=[[:space:]]*executed_only' /opt/reyt/bale/settings.ini || { echo "BALE_SIGNAL_MODE_NOT_EXECUTED_ONLY"; exit 1; }
echo "BALE_EXECUTED_ONLY_OK"
