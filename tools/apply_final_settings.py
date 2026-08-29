#!/usr/bin/env python3
from pathlib import Path
import configparser

FILES = {
    "collector": Path("/opt/reyt/collector/settings.ini"),
    "strategy": Path("/opt/reyt/strategy/settings.ini"),
    "bale": Path("/opt/reyt/bale/settings.ini"),
}

def load(path: Path):
    if not path.exists():
        raise SystemExit(f"Missing live settings file: {path}")
    cfg = configparser.ConfigParser(interpolation=None, strict=False)
    cfg.read(path, encoding="utf-8")
    return cfg

def ensure(cfg, section):
    if not cfg.has_section(section):
        cfg.add_section(section)
    return cfg[section]

def save(cfg, path):
    with path.open("w", encoding="utf-8") as f:
        cfg.write(f)

# Collector: normalize duplicates and pin the final operating parameters.
cfg = load(FILES["collector"])
c = ensure(cfg, "collector")
for legacy in ["collector_daily_history_refresh_days", "collector_history_retention_rows"]:
    c.pop(legacy, None)
for k, v in {
    "collector_db_pool_min":"1", "collector_db_pool_max":"8",
    "collector_http_timeout":"20", "collector_http_retries":"3",
    "collector_http_connector_limit":"60",
    "collector_realtime_interval":"5", "collector_underlying_interval":"60",
    "collector_order_book_interval":"15", "collector_order_book_batch_size":"300",
    "collector_order_book_concurrency":"30", "collector_order_book_include_underlyings":"yes",
    "collector_underlying_concurrency":"8",
    "collector_initial_history_days":"252", "collector_eod_history_lookback_days":"7",
    "collector_eod_history_time":"13:00", "collector_history_concurrency":"4",
    "collector_history_request_delay":"0.20", "collector_db_deadlock_retries":"5",
    "collector_db_deadlock_base_delay":"0.50", "collector_calculate_greeks":"yes",
    "market_open":"09:00", "market_close":"12:30",
}.items(): c[k] = v
g = ensure(cfg, "greeks")
for k, v in {
    "greeks_risk_free_rate":"0.40", "greeks_dividend_yield":"0.0",
    "greeks_max_iv":"5.0", "greeks_min_iv":"0.000001",
    "greeks_max_newton_iter":"30", "greeks_newton_tol":"0.0000001",
    "greeks_bisection_iter":"60", "greeks_spread_threshold":"0.10",
}.items(): g[k] = v
save(cfg, FILES["collector"])

# Strategy: preserve MySQL credentials, normalize the final calculation/execution rules.
cfg = load(FILES["strategy"])
p = ensure(cfg, "paper")
for legacy in ["paper_option_settlement_fee_pct", "paper_option_exercise_fee_pct"]:
    p.pop(legacy, None)
for k, v in {
    "paper_account_name":"paper_100m_toman",
    "paper_initial_capital_toman":"100000000",
    "paper_max_strategy_allocation_pct":"30",
    "paper_fixed_risk_per_trade_toman":"1000000",
    "paper_min_execution_risk_toman":"200000",
    "paper_auto_trade":"yes",
    "paper_allowed_underlyings":"اهرم,وبملت,شپنا,فملی,شستا",
    "paper_auto_trade_min_score":"60",
    "paper_min_days_to_expiry":"5", "paper_max_days_to_expiry":"180",
    "paper_max_strike_steps":"4",
    "paper_max_entry_book_age_seconds":"150", "paper_max_mark_book_age_seconds":"900",
    "paper_option_volume_mode":"auto", "paper_option_volume_auto_fallback":"contracts",
    "paper_volatility_lookback_days":"252", "paper_min_volatility_returns":"90",
    "paper_expected_return_hurdle_ear":"0.40",
    "paper_expected_return_require_both_models":"yes",
    "paper_expected_return_history_rows":"252",
    "paper_expected_return_min_history_returns":"90",
    "paper_expected_return_bootstrap_paths":"2500",
    "paper_expected_return_iv_quantile_paths":"2001",
    "paper_expected_return_trading_days":"252",
    "paper_expected_return_calendar_days":"365",
    "paper_expected_return_history_cache_seconds":"300",
    "paper_underlying_buy_fee_pct":"0.3712", "paper_underlying_sell_fee_pct":"0.88",
    "paper_option_buy_fee_pct":"0.103", "paper_option_sell_fee_pct":"0.103",
    "paper_option_expiry_fee_pct":"0.05",
}.items(): p[k] = v
st = ensure(cfg, "strategy")
st["strategy_db_pool_min"] = "1"
st["strategy_db_pool_max"] = "5"
save(cfg, FILES["strategy"])

# Bale: preserve token/chat/mysql values, normalize operational values only.
cfg = load(FILES["bale"])
b = ensure(cfg, "bale")
for k, v in {
    "paper_account_name":"paper_100m_toman",
    "bale_http_timeout":"30", "bale_http_retries":"4",
    "bale_signal_poll_seconds":"2", "bale_signal_overlap_seconds":"5",
    "bale_signal_mode":"executed_only",
    "bale_query_batch_size":"2000",
    "bale_state_db_path":"/opt/reyt/bale/reyt_bale_notifier_state.sqlite3",
    "bale_report_dir":"/opt/reyt/bale/reports",
}.items(): b[k] = v
save(cfg, FILES["bale"])

print("FINAL_SETTINGS_APPLIED")
