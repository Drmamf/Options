# -*- coding: utf-8 -*-
"""ReyT unified options strategy + paper-trading engine (MySQL/MariaDB).

Designed for the rebuilt ReyT schema with explicit signal execution tracking and the optimized collector:
  00_create_all_ReyT_mysql_tables_REBUILT_v2_execution_status.sql
  ReyT_collector_unified_optimized.py

Inputs (collector-owned):
  underlying_assets, option_contracts, market_data_ticks, order_book_depth,
  daily_market_data, option_greeks, collector_state

Outputs (engine-owned):
  engine_runs, paper_strategy_account,
  covered_call_signals, protective_put_signals, bull_call_spread_signals,
  bear_put_spread_signals, long_straddle_signals,
  paper_positions, paper_position_legs, paper_position_valuations,
  paper_account_equity_history

Risk / capital / signal policy implemented here:
  * One shared paper account: 1,000,000,000 toman initial capital.
  * Fixed MAXIMUM theoretical loss per new position: 10,000,000 toman.
  * Final execution Max Loss must be between 2M and 10M toman per position.
  * Signal hurdle: BOTH empirical-history and current-IV expected return must beat
    a 40% effective-annual benchmark, converted to the candidate's time to expiry.
  * Expected return is re-checked at actual five-level VWAP before paper execution.
  * No score floor and no DTE entry filter. Score/DTE remain informational/ranking context.
  * No aggregate open-risk cap. No daily trade-count cap, no per-underlying cap,
    max 30% initial-capital allocation per strategy; cash is also a portfolio cap.
  * Same exact strategy signature is not duplicated while it is OPEN.
  * No automatic stop-loss or take-profit. Positions are held to expiry unless
    explicitly closed with the manual override.
  * Entry execution uses order-book depth, not last price. Up to five levels are
    walked to obtain a VWAP; size is reduced to the shared executable depth across
    all legs. No partial / unbalanced multi-leg fill is permitted.
  * Simultaneous paper entries consume an in-memory copy of displayed depth so
    the same liquidity is not reused multiple times in one scan.
  * Covered Call additionally checks that the gross underlying purchase can be
    funded before the option premium is credited.
  * Paper P/L and expected-return payoff are fee-aware. Exact fee percentages are
    configurable; defaults are zero because no fee schedule was supplied.

The engine never creates or alters tables.

Examples:
  python ReyT_strategy_engine_unified_1b.py --once
  python ReyT_strategy_engine_unified_1b.py --watch --interval 1
  python ReyT_strategy_engine_unified_1b.py --report
  python ReyT_strategy_engine_unified_1b.py --close-position 12
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
import hashlib
import json
import math
import os
import signal
import ssl
import statistics
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Any, AsyncIterator, DefaultDict, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import aiomysql
import pymysql


# =============================================================================
# Configuration
# =============================================================================

CONFIG_FILE = Path(
    os.getenv("OPTIONS_CONFIG_FILE", str(Path(__file__).with_name("settings.ini")))
)
_CONFIG = configparser.ConfigParser(interpolation=None)
if CONFIG_FILE.exists():
    _CONFIG.read(CONFIG_FILE, encoding="utf-8")


def _setting(name: str, default: str = "") -> str:
    env = os.getenv(name)
    if env is not None:
        return env.strip()
    key = name.lower()
    for section in ("paper", "strategy", "mysql", "mariadb", "database", "sql"):
        if _CONFIG.has_option(section, key):
            return _CONFIG.get(section, key).strip()
    return default


def _bool_setting(name: str, default: bool) -> bool:
    return _setting(name, "yes" if default else "no").lower() in {
        "1", "true", "yes", "y", "on"
    }


def _int_setting(name: str, default: int, minimum: Optional[int] = None) -> int:
    value = int(_setting(name, str(default)))
    return max(value, minimum) if minimum is not None else value


def _decimal_setting(name: str, default: str) -> Decimal:
    return Decimal(_setting(name, default))


MYSQL_HOST = _setting("MYSQL_HOST", _setting("SQL_SERVER", "127.0.0.1"))
MYSQL_PORT = _int_setting("MYSQL_PORT", 3306, 1)
MYSQL_DATABASE = _setting("MYSQL_DATABASE", _setting("SQL_DATABASE", "ghazali1_ReyTOption"))
MYSQL_USER = _setting("MYSQL_USER", _setting("SQL_USERNAME", "reyt_app"))
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", _setting("SQL_PASSWORD"))
MYSQL_CHARSET = _setting("MYSQL_CHARSET", "utf8mb4")
MYSQL_CONNECT_TIMEOUT = _int_setting("MYSQL_CONNECT_TIMEOUT", 20, 1)
MYSQL_SSL = _bool_setting("MYSQL_SSL", False)
MYSQL_POOL_MIN = _int_setting("STRATEGY_DB_POOL_MIN", 1, 1)
MYSQL_POOL_MAX = _int_setting("STRATEGY_DB_POOL_MAX", 5, MYSQL_POOL_MIN)
MYSQL_TIME_ZONE = _setting("MYSQL_TIME_ZONE", "+03:30")

TOMAN_TO_RIAL = Decimal("10")
ACCOUNT_NAME = _setting("PAPER_ACCOUNT_NAME", "paper_1b_toman")
INITIAL_CAPITAL_TOMAN = _decimal_setting("PAPER_INITIAL_CAPITAL_TOMAN", "1000000000")
FIXED_RISK_PER_TRADE_TOMAN = _decimal_setting("PAPER_FIXED_RISK_PER_TRADE_TOMAN", "10000000")
MIN_EXECUTION_RISK_TOMAN = _decimal_setting("PAPER_MIN_EXECUTION_RISK_TOMAN", "2000000")
MAX_STRATEGY_ALLOCATION_PCT = _decimal_setting("PAPER_MAX_STRATEGY_ALLOCATION_PCT", "30")
INITIAL_CAPITAL_RIAL = INITIAL_CAPITAL_TOMAN * TOMAN_TO_RIAL
FIXED_RISK_PER_TRADE_RIAL = FIXED_RISK_PER_TRADE_TOMAN * TOMAN_TO_RIAL
MIN_EXECUTION_RISK_RIAL = MIN_EXECUTION_RISK_TOMAN * TOMAN_TO_RIAL
INITIAL_RISK_PCT = FIXED_RISK_PER_TRADE_RIAL / INITIAL_CAPITAL_RIAL * Decimal("100")

AUTO_TRADE = _bool_setting("PAPER_AUTO_TRADE", True)
# Score remains ranking/context only. Minimum DTE is an actual entry/signal filter.
# MAX_DTE is retained as the configured upper bound.
AUTO_TRADE_MIN_SCORE = _decimal_setting("PAPER_AUTO_TRADE_MIN_SCORE", "60")
MIN_DTE = _int_setting("PAPER_MIN_DAYS_TO_EXPIRY", 5, 0)
MAX_DTE = _int_setting("PAPER_MAX_DAYS_TO_EXPIRY", 180, max(0, MIN_DTE))
MAX_STRIKE_STEPS = _int_setting("PAPER_MAX_STRIKE_STEPS", 4, 1)
MAX_ENTRY_BOOK_AGE_SECONDS = _int_setting("PAPER_MAX_ENTRY_BOOK_AGE_SECONDS", 150, 1)
MAX_MARK_BOOK_AGE_SECONDS = _int_setting("PAPER_MAX_MARK_BOOK_AGE_SECONDS", 900, 1)
VOL_LOOKBACK_DAYS = _int_setting("PAPER_VOLATILITY_LOOKBACK_DAYS", 252, 2)
MIN_VOL_RETURNS = _int_setting("PAPER_MIN_VOLATILITY_RETURNS", 90, 2)

# Economic signal hurdle. 0.40 means 40% effective annual.
EXPECTED_RETURN_HURDLE_EAR = _decimal_setting("PAPER_EXPECTED_RETURN_HURDLE_EAR", "0.40")
EXPECTED_RETURN_REQUIRE_BOTH = _bool_setting("PAPER_EXPECTED_RETURN_REQUIRE_BOTH_MODELS", True)
EXPECTED_RETURN_HISTORY_ROWS = _int_setting("PAPER_EXPECTED_RETURN_HISTORY_ROWS", 252, 91)
EXPECTED_RETURN_MIN_HISTORY_RETURNS = _int_setting("PAPER_EXPECTED_RETURN_MIN_HISTORY_RETURNS", 90, 2)
EXPECTED_RETURN_BOOTSTRAP_PATHS = _int_setting("PAPER_EXPECTED_RETURN_BOOTSTRAP_PATHS", 2500, 100)
EXPECTED_RETURN_IV_QUANTILE_PATHS = _int_setting("PAPER_EXPECTED_RETURN_IV_QUANTILE_PATHS", 2001, 101)
EXPECTED_RETURN_TRADING_DAYS = _int_setting("PAPER_EXPECTED_RETURN_TRADING_DAYS", 252, 1)
EXPECTED_RETURN_CALENDAR_DAYS = _int_setting("PAPER_EXPECTED_RETURN_CALENDAR_DAYS", 365, 1)
EXPECTED_RETURN_HISTORY_CACHE_SECONDS = _int_setting("PAPER_EXPECTED_RETURN_HISTORY_CACHE_SECONDS", 300, 1)

# TSETMC option depth volume has historically been encountered in more than one
# representation. Auto mode preserves the diagnostic behavior of the corrected
# engine: obvious contract-like values are treated as contracts; ambiguous exact
# multiples use the configured fallback.
OPTION_VOLUME_MODE = _setting("PAPER_OPTION_VOLUME_MODE", "auto").lower()
OPTION_VOLUME_AUTO_FALLBACK = _setting("PAPER_OPTION_VOLUME_AUTO_FALLBACK", "contracts").lower()
if OPTION_VOLUME_MODE not in {"auto", "underlying_units", "contracts"}:
    OPTION_VOLUME_MODE = "auto"
if OPTION_VOLUME_AUTO_FALLBACK not in {"underlying_units", "contracts"}:
    OPTION_VOLUME_AUTO_FALLBACK = "contracts"

# Standard fee schedule defaults; all values are percentage points (e.g. 0.103 = 0.103%).
# They remain configurable in settings.ini. Expiry/exercise fee is charged once on exercise value.
UNDERLYING_BUY_FEE_PCT = _decimal_setting("PAPER_UNDERLYING_BUY_FEE_PCT", "0.3712")
UNDERLYING_SELL_FEE_PCT = _decimal_setting("PAPER_UNDERLYING_SELL_FEE_PCT", "0.88")
OPTION_BUY_FEE_PCT = _decimal_setting("PAPER_OPTION_BUY_FEE_PCT", "0.103")
OPTION_SELL_FEE_PCT = _decimal_setting("PAPER_OPTION_SELL_FEE_PCT", "0.103")
OPTION_EXPIRY_FEE_PCT = _decimal_setting("PAPER_OPTION_EXPIRY_FEE_PCT", "0.05")
# Backward-compatible aliases used in report/details fields.
OPTION_SETTLEMENT_FEE_PCT = OPTION_EXPIRY_FEE_PCT
OPTION_EXERCISE_FEE_PCT = OPTION_EXPIRY_FEE_PCT

MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(12, 30)
MARKET_WEEKDAYS = {0, 1, 2, 5, 6}  # Saturday through Wednesday
TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))

D0 = Decimal("0")
D1 = Decimal("1")
D100 = Decimal("100")
MONEY_Q = Decimal("0.01")
PCT_Q = Decimal("0.0001")

_EXPECTED_RETURN_NORMAL = statistics.NormalDist()
_EXPECTED_RETURN_Z_GRID = tuple(
    _EXPECTED_RETURN_NORMAL.inv_cdf((i + 0.5) / EXPECTED_RETURN_IV_QUANTILE_PATHS)
    for i in range(EXPECTED_RETURN_IV_QUANTILE_PATHS)
)

STRATEGY_TABLES: Dict[str, str] = {
    "COVERED_CALL": "covered_call_signals",
    "PROTECTIVE_PUT": "protective_put_signals",
    "BULL_CALL_SPREAD": "bull_call_spread_signals",
    "BEAR_PUT_SPREAD": "bear_put_spread_signals",
    "LONG_STRADDLE": "long_straddle_signals",
}

REQUIRED_TABLES = (
    "engine_runs",
    "paper_strategy_account",
    "paper_positions",
    "paper_position_legs",
    "paper_position_valuations",
    "paper_account_equity_history",
    "covered_call_signals",
    "protective_put_signals",
    "bull_call_spread_signals",
    "bear_put_spread_signals",
    "long_straddle_signals",
    "underlying_assets",
    "option_contracts",
    "market_data_ticks",
    "order_book_depth",
    "daily_market_data",
    "option_greeks",
    "collector_state",
)


def _parse_holidays(raw: str) -> set[date]:
    out: set[date] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.add(datetime.strptime(item, "%Y-%m-%d").date())
    return out


MARKET_HOLIDAYS = _parse_holidays(_setting("MARKET_HOLIDAYS", ""))


# =============================================================================
# Generic helpers
# =============================================================================


def tehran_now() -> datetime:
    return datetime.now(TEHRAN_TZ).replace(tzinfo=None)


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = now or tehran_now()
    return (
        now.weekday() in MARKET_WEEKDAYS
        and now.date() not in MARKET_HOLIDAYS
        and MARKET_OPEN <= now.time() <= MARKET_CLOSE
    )


def next_market_open() -> datetime:
    now_aware = datetime.now(TEHRAN_TZ)
    for offset in range(370):
        day = now_aware.date() + timedelta(days=offset)
        candidate = datetime.combine(day, MARKET_OPEN)
        if (
            day.weekday() in MARKET_WEEKDAYS
            and day not in MARKET_HOLIDAYS
            and candidate.replace(tzinfo=TEHRAN_TZ) > now_aware
        ):
            return candidate
    return tehran_now() + timedelta(hours=1)


def dec(value: Any, default: Decimal = D0) -> Decimal:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else default
    except (InvalidOperation, TypeError, ValueError):
        return default


def opt_dec(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def money(value: Any) -> Decimal:
    return dec(value).quantize(MONEY_Q, rounding=ROUND_HALF_UP)


def pct(value: Any) -> Decimal:
    return dec(value).quantize(PCT_Q, rounding=ROUND_HALF_UP)


def safe_div(a: Decimal, b: Decimal) -> Decimal:
    return D0 if b == D0 else a / b


def floor_int(value: Decimal) -> int:
    return 0 if value <= D0 else int(value.to_integral_value(rounding=ROUND_FLOOR))


def clamp(value: Decimal, lo: Decimal = D0, hi: Decimal = D100) -> Decimal:
    return max(lo, min(hi, value))


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def fee(gross: Decimal, rate_pct: Decimal) -> Decimal:
    return max(D0, gross) * max(D0, rate_pct) / D100


def qname(table: str) -> str:
    if table not in set(REQUIRED_TABLES) | set(STRATEGY_TABLES.values()):
        raise RuntimeError(f"Unsafe/unexpected table name: {table}")
    return f"`{table}`"


def normalize_option_volume(raw_volume: int, contract_size: int) -> Tuple[int, int]:
    """Return (contract_capacity, raw_units_consumed_per_contract)."""
    if raw_volume <= 0 or contract_size <= 0:
        return 0, 1
    value = int(raw_volume)
    size = int(contract_size)
    mode = OPTION_VOLUME_MODE
    if mode == "auto":
        if value < size or value % size != 0:
            mode = "contracts"
        else:
            mode = OPTION_VOLUME_AUTO_FALLBACK
    if mode == "contracts":
        return value, 1
    return value // size, size


def freshness_score(age_seconds: int) -> Decimal:
    if age_seconds <= 15:
        return D100
    if age_seconds <= 60:
        return Decimal("90")
    if age_seconds <= 180:
        return Decimal("75")
    if age_seconds <= 300:
        return Decimal("60")
    if age_seconds <= 900:
        return Decimal("35")
    return Decimal("5")


def dte_score(days: int) -> Decimal:
    if 14 <= days <= 60:
        return D100
    if 7 <= days <= 90:
        return Decimal("80")
    if MIN_DTE <= days <= MAX_DTE:
        return Decimal("55")
    return Decimal("10")


def validate_configuration() -> None:
    if not MYSQL_HOST or not MYSQL_DATABASE or not MYSQL_USER:
        raise RuntimeError("MySQL host/database/user must be configured.")
    if not MYSQL_PASSWORD:
        raise RuntimeError(
            "MYSQL_PASSWORD is empty. Set it in settings.ini or MYSQL_PASSWORD."
        )
    if not (1 <= MYSQL_PORT <= 65535):
        raise RuntimeError("MYSQL_PORT must be between 1 and 65535.")
    if INITIAL_CAPITAL_RIAL <= D0 or FIXED_RISK_PER_TRADE_RIAL <= D0:
        raise RuntimeError("Paper initial capital and fixed risk must be positive.")
    if FIXED_RISK_PER_TRADE_RIAL > INITIAL_CAPITAL_RIAL:
        raise RuntimeError("Fixed risk per trade cannot exceed initial capital.")
    if MAX_STRATEGY_ALLOCATION_PCT <= D0 or MAX_STRATEGY_ALLOCATION_PCT > D100:
        raise RuntimeError("PAPER_MAX_STRATEGY_ALLOCATION_PCT must be > 0 and <= 100.")
    if MIN_DTE < 0 or MAX_DTE < MIN_DTE:
        raise RuntimeError("Invalid score-context DTE configuration.")
    if EXPECTED_RETURN_HURDLE_EAR < D0 or EXPECTED_RETURN_HURDLE_EAR > Decimal("10"):
        raise RuntimeError("PAPER_EXPECTED_RETURN_HURDLE_EAR must be between 0 and 10 as a decimal fraction.")
    if EXPECTED_RETURN_MIN_HISTORY_RETURNS >= EXPECTED_RETURN_HISTORY_ROWS:
        raise RuntimeError("Expected-return minimum history returns must be smaller than retained history rows.")
    for name, value in {
        "PAPER_UNDERLYING_BUY_FEE_PCT": UNDERLYING_BUY_FEE_PCT,
        "PAPER_UNDERLYING_SELL_FEE_PCT": UNDERLYING_SELL_FEE_PCT,
        "PAPER_OPTION_BUY_FEE_PCT": OPTION_BUY_FEE_PCT,
        "PAPER_OPTION_SELL_FEE_PCT": OPTION_SELL_FEE_PCT,
        "PAPER_OPTION_EXPIRY_FEE_PCT": OPTION_EXPIRY_FEE_PCT,
    }.items():
        if value < D0 or value > Decimal("10"):
            raise RuntimeError(f"{name} must be between 0 and 10 percent.")


def connection_kwargs() -> Dict[str, Any]:
    ssl_context = ssl.create_default_context() if MYSQL_SSL else None
    return {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "db": MYSQL_DATABASE,
        "charset": MYSQL_CHARSET,
        "autocommit": False,
        "connect_timeout": MYSQL_CONNECT_TIMEOUT,
        "cursorclass": aiomysql.DictCursor,
        "ssl": ssl_context,
        "init_command": f"SET time_zone = '{MYSQL_TIME_ZONE}'",
    }


# =============================================================================
# Database wrapper
# =============================================================================


class DB:
    def __init__(self, raw: aiomysql.Connection) -> None:
        self.raw = raw

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> int:
        async with self.raw.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return int(cur.rowcount)

    async def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        if not rows:
            return 0
        async with self.raw.cursor() as cur:
            await cur.executemany(sql, rows)
            return int(cur.rowcount)

    async def fetch(self, sql: str, args: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        async with self.raw.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return list(await cur.fetchall())

    async def fetchrow(self, sql: str, args: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
        async with self.raw.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return await cur.fetchone()

    async def fetchval(self, sql: str, args: Sequence[Any] = ()) -> Any:
        row = await self.fetchrow(sql, args)
        return None if row is None else next(iter(row.values()))

    async def insert_id(self, sql: str, args: Sequence[Any] = ()) -> int:
        async with self.raw.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return int(cur.lastrowid)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["DB"]:
        await self.raw.begin()
        try:
            yield self
        except Exception:
            await self.raw.rollback()
            raise
        else:
            await self.raw.commit()


# =============================================================================
# Market / execution models
# =============================================================================


@dataclass(frozen=True)
class Account:
    account_id: int
    account_name: str
    initial_equity: Decimal
    equity: Decimal
    realized: Decimal
    unrealized: Decimal
    reserved_risk: Decimal
    allocated_capital: Decimal
    open_count: int
    high_watermark: Decimal

    @property
    def fixed_trade_risk_budget(self) -> Decimal:
        return FIXED_RISK_PER_TRADE_RIAL

    @property
    def available_cash(self) -> Decimal:
        # Unrealized P/L is not spendable cash. Open debit positions have already
        # consumed entry_capital; realized P/L is spendable.
        return max(D0, self.initial_equity + self.realized - self.allocated_capital)


@dataclass(frozen=True)
class OptionQuote:
    ins_code: str
    ua_ins_code: str
    underlying_symbol: str
    option_type: str
    symbol: str
    expiry: date
    strike: Decimal
    contract_size: int
    days_to_expiry: int
    spot: Decimal
    option_price: Decimal
    tick_bid: Decimal
    tick_bid_volume: int
    tick_ask: Decimal
    tick_ask_volume: int
    last: Decimal
    closing: Decimal
    fetch_datetime: datetime
    implied_volatility: Optional[Decimal]
    delta: Optional[Decimal]
    gamma: Optional[Decimal]
    theta: Optional[Decimal]
    vega: Optional[Decimal]
    rho: Optional[Decimal]
    leverage: Optional[Decimal]
    moneyness_ratio: Optional[Decimal]
    greek_spread_percent: Optional[Decimal]
    open_interest: int


@dataclass
class BookLevel:
    level: int
    bid_price: Decimal
    bid_volume: int
    ask_price: Decimal
    ask_volume: int


@dataclass
class OrderBook:
    ins_code: str
    snapshot_time: datetime
    levels: List[BookLevel] = field(default_factory=list)

    def clone(self) -> "OrderBook":
        return OrderBook(
            self.ins_code,
            self.snapshot_time,
            [replace(x) for x in self.levels],
        )

    def age_seconds(self, now: datetime) -> int:
        return max(0, int((now - self.snapshot_time).total_seconds()))

    def is_fresh(self, now: datetime, max_age: int) -> bool:
        return self.age_seconds(now) <= max_age

    def best_price(self, action: str) -> Decimal:
        if action == "BUY":
            prices = [x.ask_price for x in self.levels if x.ask_price > D0 and x.ask_volume > 0]
            return min(prices) if prices else D0
        prices = [x.bid_price for x in self.levels if x.bid_price > D0 and x.bid_volume > 0]
        return max(prices) if prices else D0

    def spread_pct(self) -> Decimal:
        bid = self.best_price("SELL")
        ask = self.best_price("BUY")
        mid = (bid + ask) / Decimal("2")
        if bid <= D0 or ask <= D0 or mid <= D0:
            return Decimal("999")
        return (ask - bid) / mid * D100

    def capacity_units(self, leg: "Leg") -> int:
        action = "BUY" if leg.side == "LONG" else "SELL"
        if leg.kind == "UNDERLYING":
            raw = sum(
                (x.ask_volume if action == "BUY" else x.bid_volume)
                for x in self.levels
                if (x.ask_price > D0 if action == "BUY" else x.bid_price > D0)
            )
            return raw // max(1, leg.contract_size)

        total_contracts = 0
        for x in self.levels:
            price = x.ask_price if action == "BUY" else x.bid_price
            raw = x.ask_volume if action == "BUY" else x.bid_volume
            if price <= D0 or raw <= 0:
                continue
            contracts, _ = normalize_option_volume(raw, leg.contract_size)
            total_contracts += contracts
        return total_contracts

    def quote_vwap(self, leg: "Leg", units: int) -> Optional[Decimal]:
        if units <= 0:
            return None
        action = "BUY" if leg.side == "LONG" else "SELL"
        ordered = sorted(
            self.levels,
            key=lambda x: (x.ask_price if action == "BUY" else -x.bid_price, x.level),
        )
        remaining = units * leg.contract_size if leg.kind == "UNDERLYING" else units
        weighted = D0
        filled = 0

        for lvl in ordered:
            price = lvl.ask_price if action == "BUY" else lvl.bid_price
            raw = lvl.ask_volume if action == "BUY" else lvl.bid_volume
            if price <= D0 or raw <= 0:
                continue
            if leg.kind == "UNDERLYING":
                capacity = raw
            else:
                capacity, _ = normalize_option_volume(raw, leg.contract_size)
            if capacity <= 0:
                continue
            take = min(remaining, capacity)
            weighted += price * Decimal(take)
            filled += take
            remaining -= take
            if remaining <= 0:
                break

        if remaining > 0 or filled <= 0:
            return None
        return weighted / Decimal(filled)

    def consume(self, leg: "Leg", units: int) -> bool:
        """Consume displayed depth after a paper execution has been accepted."""
        if units <= 0:
            return False
        action = "BUY" if leg.side == "LONG" else "SELL"
        ordered = sorted(
            self.levels,
            key=lambda x: (x.ask_price if action == "BUY" else -x.bid_price, x.level),
        )
        remaining = units * leg.contract_size if leg.kind == "UNDERLYING" else units

        for lvl in ordered:
            price = lvl.ask_price if action == "BUY" else lvl.bid_price
            raw = lvl.ask_volume if action == "BUY" else lvl.bid_volume
            if price <= D0 or raw <= 0:
                continue
            if leg.kind == "UNDERLYING":
                capacity = raw
                raw_per_unit = 1
            else:
                capacity, raw_per_unit = normalize_option_volume(raw, leg.contract_size)
            if capacity <= 0:
                continue
            take = min(remaining, capacity)
            raw_take = take * raw_per_unit
            if action == "BUY":
                lvl.ask_volume = max(0, lvl.ask_volume - raw_take)
            else:
                lvl.bid_volume = max(0, lvl.bid_volume - raw_take)
            remaining -= take
            if remaining <= 0:
                return True
        return False


@dataclass(frozen=True)
class Leg:
    no: int
    kind: str  # UNDERLYING / OPTION
    ins_code: str
    symbol: str
    side: str  # LONG / SHORT
    option_type: Optional[str]
    strike: Optional[Decimal]
    expiry: date
    contract_size: int
    entry_price: Decimal


@dataclass(frozen=True)
class StrategyMetrics:
    unit_capital: Decimal
    unit_loss: Decimal
    unit_profit: Optional[Decimal]
    be_low: Optional[Decimal]
    be_high: Optional[Decimal]
    reward_risk: Optional[Decimal]
    pretrade_cash: Decimal
    extras: Dict[str, Any]


@dataclass
class Candidate:
    strategy: str
    table: str
    key: str
    source_revision: str
    scan_time: datetime
    ua_ins_code: str
    underlying_symbol: str
    expiry: date
    days_to_expiry: int
    spot: Decimal
    legs: List[Leg]
    unit_capital: Decimal
    unit_loss: Decimal
    unit_profit: Optional[Decimal]
    be_low: Optional[Decimal]
    be_high: Optional[Decimal]
    reward_risk: Optional[Decimal]
    pretrade_cash: Decimal
    executable_units: int
    recommended_units: int
    recommended_risk: Decimal
    recommended_capital: Decimal
    liquidity: Decimal
    score: Decimal
    final_signal: str
    reason: str
    details: Dict[str, Any]
    signal_id: Optional[int] = None
    opened_position_id: Optional[int] = None

    @property
    def signature(self) -> str:
        legs = "|".join(f"{x.side}:{x.ins_code}" for x in self.legs)
        return f"{self.strategy}|{self.ua_ins_code}|{legs}"

    @property
    def priority_tuple(self) -> Tuple[int, Decimal, Decimal, Decimal]:
        edge = opt_dec(self.details.get("conservative_excess_return_pp"))
        if edge is not None:
            return (2, edge, self.score, self.liquidity)
        if self.reward_risk is not None and self.reward_risk.is_finite():
            return (1, self.reward_risk, self.score, self.liquidity)
        return (0, Decimal("-1"), self.score, self.liquidity)


@dataclass(frozen=True)
class ExecutionPlan:
    units: int
    leg_prices: Tuple[Decimal, ...]
    metrics: StrategyMetrics


@dataclass(frozen=True)
class ExecutionDecision:
    plan: Optional[ExecutionPlan]
    reason_code: str
    reason: str


# =============================================================================
# Fee / payoff calculations
# =============================================================================


def _underlying_buy_cost(price: Decimal, quantity: int) -> Decimal:
    gross = price * Decimal(quantity)
    return gross + fee(gross, UNDERLYING_BUY_FEE_PCT)


def _underlying_sell_value(price: Decimal, quantity: int) -> Decimal:
    gross = price * Decimal(quantity)
    return gross - fee(gross, UNDERLYING_SELL_FEE_PCT)


def _option_buy_cost(price: Decimal, quantity_units: int) -> Decimal:
    gross = price * Decimal(quantity_units)
    return gross + fee(gross, OPTION_BUY_FEE_PCT)


def _option_sell_value(price: Decimal, quantity_units: int) -> Decimal:
    gross = price * Decimal(quantity_units)
    return gross - fee(gross, OPTION_SELL_FEE_PCT)


def _option_expiry_fee(strike: Decimal, quantity_units: int) -> Decimal:
    # Exercise/settlement commission is assessed on exercise value, once per
    # exercised option leg, rather than on intrinsic payoff.
    gross_exercise_value = max(D0, strike) * Decimal(max(0, quantity_units))
    return fee(gross_exercise_value, OPTION_EXPIRY_FEE_PCT)


def strategy_metrics(strategy: str, spot: Decimal, legs: Sequence[Leg], prices: Sequence[Decimal]) -> StrategyMetrics:
    if len(legs) != 2 or len(prices) != 2:
        raise ValueError("All current ReyT strategies require two legs.")
    a, b = legs
    pa, pb = prices
    cs = a.contract_size
    if cs <= 0 or b.contract_size != cs:
        raise ValueError("Mismatched/invalid contract sizes.")
    qty = cs

    if strategy == "COVERED_CALL":
        if a.kind != "UNDERLYING" or b.option_type != "CALL" or b.side != "SHORT":
            raise ValueError("Invalid covered-call legs.")
        stock_cost = _underlying_buy_cost(pa, qty)
        premium_net = _option_sell_value(pb, qty)
        capital = stock_cost - premium_net
        max_loss = max(D0, capital)
        strike = dec(b.strike)
        terminal_at_cap = _underlying_sell_value(strike, qty) - _option_expiry_fee(strike, qty)
        max_profit = max(D0, terminal_at_cap - capital)
        sell_factor = D1 - UNDERLYING_SELL_FEE_PCT / D100
        be = safe_div(capital, Decimal(qty) * sell_factor) if sell_factor > D0 else None
        rr = safe_div(max_profit, max_loss) if max_loss > D0 else None
        gross_stock = pa * Decimal(qty)
        gross_premium = pb * Decimal(qty)
        ret = safe_div(max_profit, gross_stock) * D100 if gross_stock > D0 else D0
        return StrategyMetrics(
            money(capital), money(max_loss), money(max_profit),
            money(be) if be is not None else None, None,
            pct(rr) if rr is not None else None,
            money(stock_cost),
            {
                "premium_income_rial": money(gross_premium),
                "capital_required_gross_rial": money(gross_stock),
                "net_debit_rial": money(capital),
                "return_to_expiry_pct": pct(ret),
            },
        )

    if strategy == "PROTECTIVE_PUT":
        if a.kind != "UNDERLYING" or b.option_type != "PUT" or b.side != "LONG":
            raise ValueError("Invalid protective-put legs.")
        stock_cost = _underlying_buy_cost(pa, qty)
        put_cost = _option_buy_cost(pb, qty)
        capital = stock_cost + put_cost
        strike = dec(b.strike)
        floor_gross = strike * Decimal(qty)
        terminal_fees = fee(floor_gross, UNDERLYING_SELL_FEE_PCT) + _option_expiry_fee(strike, qty)
        protected_floor = max(D0, floor_gross - terminal_fees)
        max_loss = max(D0, capital - protected_floor)
        sell_factor = D1 - UNDERLYING_SELL_FEE_PCT / D100
        be = safe_div(capital, Decimal(qty) * sell_factor) if sell_factor > D0 else None
        insurance_pct = safe_div(pb, pa) * D100 if pa > D0 else D0
        gap = safe_div(pa - strike, pa) * D100 if pa > D0 else D0
        return StrategyMetrics(
            money(capital), money(max_loss), None,
            money(be) if be is not None else None, None, None,
            money(capital),
            {
                "insurance_cost_rial": money(pb * Decimal(qty)),
                "insurance_cost_pct": pct(insurance_pct),
                "protection_gap_pct": pct(gap),
            },
        )

    if strategy in {"BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"}:
        if a.kind != "OPTION" or b.kind != "OPTION" or a.side != "LONG" or b.side != "SHORT":
            raise ValueError("Invalid debit-spread legs.")
        long_cost = _option_buy_cost(pa, qty)
        short_credit = _option_sell_value(pb, qty)
        capital = long_cost - short_credit
        width = abs(dec(b.strike) - dec(a.strike))
        payoff = width * Decimal(qty)
        expiry_fees = _option_expiry_fee(dec(a.strike), qty) + _option_expiry_fee(dec(b.strike), qty)
        max_profit = max(D0, payoff - expiry_fees - capital)
        max_loss = max(D0, capital)
        rr = safe_div(max_profit, max_loss) if max_loss > D0 else None
        debit_per_share = safe_div(capital, Decimal(qty))
        if strategy == "BULL_CALL_SPREAD":
            be = dec(a.strike) + debit_per_share
            extras = {
                "net_debit_per_unit_rial": money(debit_per_share),
                "strike_width_rial": money(width),
                "required_spot_move_pct": pct(safe_div(be - spot, spot) * D100) if spot > D0 else D0,
            }
        else:
            be = dec(a.strike) - debit_per_share
            extras = {
                "net_debit_per_unit_rial": money(debit_per_share),
                "strike_width_rial": money(width),
                "required_spot_drop_pct": pct(safe_div(spot - be, spot) * D100) if spot > D0 else D0,
            }
        return StrategyMetrics(
            money(capital), money(max_loss), money(max_profit), money(be), None,
            pct(rr) if rr is not None else None,
            money(long_cost), extras,
        )

    if strategy == "LONG_STRADDLE":
        if a.kind != "OPTION" or b.kind != "OPTION" or a.side != "LONG" or b.side != "LONG":
            raise ValueError("Invalid straddle legs.")
        call_leg = a if a.option_type == "CALL" else b
        call_price = pa if a.option_type == "CALL" else pb
        put_price = pb if b.option_type == "PUT" else pa
        call_cost = _option_buy_cost(call_price, qty)
        put_cost = _option_buy_cost(put_price, qty)
        capital = call_cost + put_cost
        strike = dec(call_leg.strike)
        debit_per_share = safe_div(capital, Decimal(qty))
        be_low = max(D0, strike - debit_per_share)
        be_high = strike + debit_per_share
        return StrategyMetrics(
            money(capital), money(capital), None, money(be_low), money(be_high), None,
            money(capital),
            {
                "total_premium_per_unit_rial": money(call_price + put_price),
                "required_move_pct": pct(safe_div(call_price + put_price, spot) * D100) if spot > D0 else D0,
            },
        )

    raise ValueError(f"Unsupported strategy: {strategy}")


# =============================================================================
# Expected-return hurdle models
# =============================================================================


def _daily_log_returns(prices: Sequence[float]) -> List[float]:
    return [
        math.log(b / a)
        for a, b in zip(prices, prices[1:])
        if a > 0 and b > 0
    ]


def _terminal_net_value(c: Candidate, terminal_spot: float) -> float:
    """One-unit terminal value using the SAME per-leg settlement conventions as the engine."""
    spot = Decimal(str(terminal_spot))
    total = D0
    for leg in c.legs:
        qty = int(leg.contract_size)
        if qty <= 0:
            return float("nan")
        if leg.kind == "UNDERLYING":
            total += _underlying_sell_value(spot, qty)
            continue
        strike = dec(leg.strike)
        intrinsic = max(D0, spot - strike) if leg.option_type == "CALL" else max(D0, strike - spot)
        gross = intrinsic * Decimal(qty)
        expiry_fee = _option_expiry_fee(strike, qty) if intrinsic > D0 else D0
        if leg.side == "LONG":
            total += gross - expiry_fee
        else:
            total += -gross - expiry_fee
    return float(total)


def _required_return_to_expiry(dte: int) -> float:
    # DTE=0 still uses a one-calendar-day hurdle, matching the one-trading-day
    # minimum scenario horizon instead of silently accepting any positive edge.
    days = max(1, int(dte))
    return (1.0 + float(EXPECTED_RETURN_HURDLE_EAR)) ** (
        days / float(EXPECTED_RETURN_CALENDAR_DAYS)
    ) - 1.0


def _effective_candidate_iv(c: Candidate) -> Optional[float]:
    d = c.details or {}
    vals: List[Any] = []
    if c.strategy in {"COVERED_CALL", "PROTECTIVE_PUT"}:
        vals.append(d.get("implied_volatility"))
    elif c.strategy in {"BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"}:
        vals.extend([d.get("long_iv"), d.get("short_iv")])
    elif c.strategy == "LONG_STRADDLE":
        vals.extend([d.get("call_iv"), d.get("put_iv")])
    clean: List[float] = []
    for value in vals:
        try:
            x = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(x) and 0.0 < x <= 5.0:
            clean.append(x)
    return sum(clean) / len(clean) if clean else None


def _evaluate_expected_return(
    c: Candidate,
    terminal_multipliers: Sequence[float],
    capital_rial: Decimal,
) -> Optional[Dict[str, float]]:
    if not terminal_multipliers or capital_rial <= D0 or c.spot <= D0:
        return None
    capital = float(capital_rial)
    spot = float(c.spot)
    pnl_sum = 0.0
    for mult in terminal_multipliers:
        terminal_value = _terminal_net_value(c, spot * mult)
        if not math.isfinite(terminal_value):
            return None
        pnl_sum += terminal_value - capital
    expected_pnl = pnl_sum / len(terminal_multipliers)
    expected_return = expected_pnl / capital
    required = _required_return_to_expiry(c.days_to_expiry)
    return {
        "expected_pnl_rial": expected_pnl,
        "expected_return": expected_return,
        "required_return": required,
        "passes": expected_return > required,
        "scenarios": float(len(terminal_multipliers)),
    }


# =============================================================================
# Candidate helpers
# =============================================================================


def book_age_for_legs(legs: Sequence[Leg], books: Mapping[str, OrderBook], now: datetime) -> int:
    ages: List[int] = []
    for leg in legs:
        book = books.get(leg.ins_code)
        if book is None:
            return 10**9
        ages.append(book.age_seconds(now))
    return max(ages) if ages else 10**9


def executable_units_for_legs(legs: Sequence[Leg], books: Mapping[str, OrderBook], now: datetime) -> int:
    capacities: List[int] = []
    for leg in legs:
        book = books.get(leg.ins_code)
        if book is None or not book.is_fresh(now, MAX_ENTRY_BOOK_AGE_SECONDS):
            return 0
        capacities.append(book.capacity_units(leg))
    return min(capacities) if capacities else 0


def liquidity_score(legs: Sequence[Leg], books: Mapping[str, OrderBook], now: datetime) -> Decimal:
    scores: List[Decimal] = []
    for leg in legs:
        book = books.get(leg.ins_code)
        if book is None or not book.is_fresh(now, MAX_ENTRY_BOOK_AGE_SECONDS):
            return D0
        depth = book.capacity_units(leg)
        depth_score = (
            D0 if depth <= 0 else
            Decimal("35") if depth == 1 else
            Decimal("55") if depth <= 3 else
            Decimal("75") if depth <= 10 else D100
        )
        spread = book.spread_pct()
        spread_score = (
            D100 if spread <= 2 else
            Decimal("80") if spread <= 5 else
            Decimal("55") if spread <= 10 else
            Decimal("30") if spread <= 20 else Decimal("5")
        )
        scores.append(depth_score * Decimal("0.65") + spread_score * Decimal("0.35"))
    return pct(min(scores)) if scores else D0


def make_signal_key(strategy: str, ua: str, legs: Sequence[Leg], signal_date: date) -> str:
    # One logical structure per Tehran trading day. Collector refreshes UPSERT
    # the same row rather than creating a new row every market-data revision.
    payload = strategy + "|" + ua + "|" + "|".join(
        f"{x.side}:{x.ins_code}" for x in legs
    ) + "|" + signal_date.isoformat()
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"{strategy[:12]}|{digest}"


def signal_label(
    valid: bool,
    dte: int,
    unit_loss: Decimal,
    executable: int,
    recommended_units: int,
    score: Decimal,
) -> str:
    # Economic execution constraints stay outside signal creation, but DTE is a
    # structural eligibility rule: contracts must remain within the configured window.
    if not valid:
        return "REJECT"
    if dte < MIN_DTE or dte > MAX_DTE:
        return "REJECT"
    return "STRONG_CANDIDATE" if score >= Decimal("75") else "CANDIDATE"


# =============================================================================
# Engine
# =============================================================================


class PaperEngine:
    def __init__(self) -> None:
        self.pool: Optional[aiomysql.Pool] = None
        self._shutdown = False
        self._er_history_date: Optional[date] = None
        self._er_history_loaded_at: Optional[datetime] = None
        self._er_history_returns: Dict[str, List[float]] = {}
        self._er_hist_multiplier_cache: Dict[Tuple[str, int], List[float]] = {}
        self._er_iv_multiplier_cache: Dict[Tuple[str, int, float], List[float]] = {}

    async def open(self) -> None:
        validate_configuration()
        self.pool = await aiomysql.create_pool(
            minsize=MYSQL_POOL_MIN,
            maxsize=MYSQL_POOL_MAX,
            **connection_kwargs(),
        )
        async with self.pool.acquire() as raw:
            db = DB(raw)
            async with db.transaction():
                await self._verify_required_tables(db)
                await self._verify_signal_execution_columns(db)
                await self._ensure_account(db)
                await self._reconcile_legacy_signal_execution_status(db)
        print(
            f"[{tehran_now():%H:%M:%S}] ✅ Strategy engine ready | "
            f"{MYSQL_HOST}:{MYSQL_PORT} | {MYSQL_DATABASE}"
        )
        print(
            f"   Account={ACCOUNT_NAME} | initial={INITIAL_CAPITAL_TOMAN:,.0f} toman | "
            f"max risk/position={FIXED_RISK_PER_TRADE_TOMAN:,.0f} toman | min final risk={MIN_EXECUTION_RISK_TOMAN:,.0f} toman"
        )
        print(
            f"   Signal hurdle: History AND IV expected return > "
            f"{EXPECTED_RETURN_HURDLE_EAR*D100:.2f}% effective annual | DTE filter=5-180 days | score floor=OFF"
        )
        print(
            f"   Portfolio caps: cash + max {MAX_STRATEGY_ALLOCATION_PCT}% initial-capital allocation per strategy "
            "| no total-risk / daily-trade / underlying cap"
        )
        print(
            f"   Execution: order-book 5-level VWAP | max entry-book age={MAX_ENTRY_BOOK_AGE_SECONDS}s"
        )
        if all(
            x == D0
            for x in (
                UNDERLYING_BUY_FEE_PCT, UNDERLYING_SELL_FEE_PCT,
                OPTION_BUY_FEE_PCT, OPTION_SELL_FEE_PCT,
                OPTION_EXPIRY_FEE_PCT,
            )
        ):
            print(
                "   ⚠ Fee logic is enabled, but all configured fee rates are currently zero."
            )

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _verify_required_tables(self, db: DB) -> None:
        missing: List[str] = []
        for table in REQUIRED_TABLES:
            try:
                await db.fetchval(f"SELECT 1 FROM `{table}` LIMIT 0")
            except pymysql.err.ProgrammingError as exc:
                if exc.args and int(exc.args[0]) == 1146:
                    missing.append(table)
                else:
                    raise
        if missing:
            raise RuntimeError(
                "Missing tables: " + ", ".join(missing)
                + ". Run 00_create_all_ReyT_mysql_tables_REBUILT_v2_execution_status.sql first."
            )

    async def _verify_signal_execution_columns(self, db: DB) -> None:
        required = (
            "executed_on_paper_account",
            "paper_execution_status",
            "paper_execution_reason_code",
            "paper_execution_reason",
            "paper_execution_checked_at",
            "paper_executed_at",
        )
        missing: List[str] = []
        for table in STRATEGY_TABLES.values():
            try:
                await db.fetchval(
                    f"SELECT {','.join(required)} FROM {qname(table)} LIMIT 0"
                )
            except pymysql.err.ProgrammingError as exc:
                if exc.args and int(exc.args[0]) == 1054:
                    missing.append(table)
                else:
                    raise
        if missing:
            raise RuntimeError(
                "Signal execution-tracking columns are missing from: "
                + ", ".join(missing)
                + ". Run 01_migrate_ReyT_signal_execution_status_RUN_ONCE.sql "
                  "or create the database with the V2 execution-status schema first."
            )

    async def _reconcile_legacy_signal_execution_status(self, db: DB) -> None:
        """Backfill rows created before execution-status columns existed.

        A signal linked to a paper position is definitively EXECUTED. Historical
        rows with no linked paper position are definitively not executed on the
        shared account. New signals are inserted later in the scan, so this
        startup reconciliation cannot race with a fresh PENDING decision.
        """
        for table_name in STRATEGY_TABLES.values():
            table = qname(table_name)
            await db.execute(
                f"""
                UPDATE {table} s
                JOIN {qname('paper_positions')} p ON p.position_id=s.opened_position_id
                SET s.executed_on_paper_account=1,
                    s.paper_execution_status='EXECUTED',
                    s.paper_execution_reason_code='POSITION_OPENED',
                    s.paper_execution_reason='Paper position exists for this signal.',
                    s.paper_execution_checked_at=COALESCE(s.paper_execution_checked_at,p.opened_at),
                    s.paper_executed_at=COALESCE(s.paper_executed_at,p.opened_at)
                WHERE s.opened_position_id IS NOT NULL
                  AND (s.paper_execution_status<>'EXECUTED' OR s.executed_on_paper_account<>1)
                """
            )
            await db.execute(
                f"""
                UPDATE {table}
                SET executed_on_paper_account=0,
                    paper_execution_status='NOT_EXECUTED',
                    paper_execution_reason_code='LEGACY_NO_PAPER_POSITION',
                    paper_execution_reason='Historical signal has no linked paper position.',
                    paper_execution_checked_at=COALESCE(paper_execution_checked_at,updated_at)
                WHERE opened_position_id IS NULL
                  AND paper_execution_status='PENDING'
                """
            )

    async def _ensure_account(self, db: DB) -> None:
        # Schema still contains percentage-cap columns for compatibility. They are
        # populated with descriptive/sentinel values; this engine's allocation
        # logic intentionally uses fixed risk + available cash + per-strategy allocation cap.
        await db.execute(
            f"""
            INSERT INTO {qname('paper_strategy_account')} (
                account_name, currency, initial_equity_rial, current_equity_rial,
                risk_per_trade_pct, capital_per_position_pct,
                max_total_open_risk_pct, max_capital_usage_pct,
                max_open_positions, high_watermark_rial
            ) VALUES (%s,'IRR',%s,%s,%s,100,999999,100,4294967295,%s)
            ON DUPLICATE KEY UPDATE
                risk_per_trade_pct=VALUES(risk_per_trade_pct),
                capital_per_position_pct=100,
                max_total_open_risk_pct=999999,
                max_capital_usage_pct=100,
                max_open_positions=4294967295,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                ACCOUNT_NAME,
                money(INITIAL_CAPITAL_RIAL),
                money(INITIAL_CAPITAL_RIAL),
                pct(INITIAL_RISK_PCT),
                money(INITIAL_CAPITAL_RIAL),
            ),
        )
        row = await db.fetchrow(
            f"SELECT initial_equity_rial FROM {qname('paper_strategy_account')} WHERE account_name=%s",
            (ACCOUNT_NAME,),
        )
        if not row:
            raise RuntimeError("Paper account could not be created/read.")
        existing = dec(row["initial_equity_rial"])
        if existing != money(INITIAL_CAPITAL_RIAL):
            raise RuntimeError(
                f"Existing account {ACCOUNT_NAME!r} has initial equity {existing/TOMAN_TO_RIAL:,.0f} toman, "
                f"but configured initial equity is {INITIAL_CAPITAL_TOMAN:,.0f}. "
                "Use a new PAPER_ACCOUNT_NAME or reset the paper tables intentionally."
            )

    async def _load_account(self, db: DB) -> Account:
        row = await db.fetchrow(
            f"""
            SELECT account_id, account_name, initial_equity_rial, current_equity_rial,
                   realized_pnl_rial, unrealized_pnl_rial, reserved_risk_rial,
                   allocated_capital_rial, open_positions_count, high_watermark_rial
            FROM {qname('paper_strategy_account')}
            WHERE account_name=%s
            """,
            (ACCOUNT_NAME,),
        )
        if not row:
            raise RuntimeError("Paper account missing.")
        return Account(
            int(row["account_id"]), str(row["account_name"]),
            dec(row["initial_equity_rial"]), dec(row["current_equity_rial"]),
            dec(row["realized_pnl_rial"]), dec(row["unrealized_pnl_rial"]),
            dec(row["reserved_risk_rial"]), dec(row["allocated_capital_rial"]),
            int(row["open_positions_count"] or 0), dec(row["high_watermark_rial"]),
        )

    async def source_revision(self) -> Optional[str]:
        if not self.pool:
            raise RuntimeError("Engine not open.")
        async with self.pool.acquire() as raw:
            db = DB(raw)
            try:
                row = await db.fetchrow(
                    f"SELECT state_value,updated_at FROM {qname('collector_state')} WHERE state_key='live_source_revision'"
                )
                if row and row.get("state_value"):
                    return str(row["state_value"])
                row = await db.fetchrow(
                    f"""
                    SELECT MAX(updated_at) AS updated_at, MAX(tick_time) AS tick_time, COUNT(*) AS n
                    FROM {qname('market_data_ticks')}
                    """
                )
                if not row or not row.get("updated_at") or int(row.get("n") or 0) <= 0:
                    return None
                return f"fallback|{row['updated_at']}|{row.get('tick_time')}|{int(row['n'])}"
            finally:
                # aiomysql autocommit is disabled: end the read transaction after
                # every poll so the next poll sees a fresh MVCC snapshot.
                await raw.rollback()

    async def _start_run(self, revision: str, mode: str = "SCAN") -> int:
        if not self.pool:
            raise RuntimeError("Engine not open.")
        now = tehran_now().replace(microsecond=0)
        async with self.pool.acquire() as raw:
            db = DB(raw)
            async with db.transaction():
                return await db.insert_id(
                    f"""
                    INSERT INTO {qname('engine_runs')} (started_at,run_mode,source_revision,status)
                    VALUES (%s,%s,%s,'RUNNING')
                    """,
                    (now, mode, revision),
                )

    async def _finish_run(
        self,
        run_id: int,
        status: str,
        quotes: int = 0,
        signals: int = 0,
        opened: int = 0,
        closed: int = 0,
        error: Optional[str] = None,
    ) -> None:
        if not self.pool or run_id <= 0:
            return
        async with self.pool.acquire() as raw:
            db = DB(raw)
            async with db.transaction():
                await db.execute(
                    f"""
                    UPDATE {qname('engine_runs')}
                    SET finished_at=%s,status=%s,quotes_count=%s,signals_count=%s,
                        opened_positions_count=%s,closed_positions_count=%s,error_message=%s
                    WHERE run_id=%s
                    """,
                    (
                        tehran_now().replace(microsecond=0), status, quotes, signals,
                        opened, closed, (error or None), run_id,
                    ),
                )

    async def _load_market_snapshot(
        self, db: DB
    ) -> Tuple[List[OptionQuote], Dict[str, OptionQuote], Dict[str, Decimal], Dict[str, OrderBook]]:
        rows = await db.fetch(
            f"""
            SELECT
                g.ins_code,g.fetch_datetime,g.ua_ins_code,g.option_type,g.symbol,
                COALESCE(g.ua_symbol,u.symbol,g.ua_ins_code) AS underlying_symbol,
                g.expiry_date,g.strike,g.days_to_expiry,g.underlying_price,g.option_price,
                g.implied_volatility,g.delta,g.gamma,g.theta,g.vega,g.rho,g.leverage,
                g.moneyness,g.spread_percent AS greek_spread_percent,g.open_interest,
                c.contract_size,
                t.last_price,t.closing_price,t.bid_price,t.bid_volume,t.ask_price,t.ask_volume
            FROM {qname('option_greeks')} g
            JOIN {qname('option_contracts')} c ON c.ins_code=g.ins_code
            JOIN {qname('underlying_assets')} u ON u.ua_ins_code=g.ua_ins_code
            LEFT JOIN {qname('market_data_ticks')} t ON t.ins_code=g.ins_code
            WHERE c.contract_size>0
              AND g.strike>0
              AND g.expiry_date IS NOT NULL
              AND g.days_to_expiry>=0
              AND g.underlying_price>0
            """
        )
        quotes: List[OptionQuote] = []
        qmap: Dict[str, OptionQuote] = {}
        umap: Dict[str, Decimal] = {}
        fallback_time = tehran_now() - timedelta(days=365)
        for x in rows:
            expiry = x["expiry_date"]
            if not isinstance(expiry, date):
                continue
            q = OptionQuote(
                ins_code=str(x["ins_code"]),
                ua_ins_code=str(x["ua_ins_code"]),
                underlying_symbol=str(x["underlying_symbol"] or x["ua_ins_code"]),
                option_type=str(x["option_type"]).upper(),
                symbol=str(x["symbol"] or x["ins_code"]),
                expiry=expiry,
                strike=dec(x["strike"]),
                contract_size=int(x["contract_size"] or 0),
                days_to_expiry=int(x["days_to_expiry"] or 0),
                spot=dec(x["underlying_price"]),
                option_price=dec(x["option_price"]),
                tick_bid=dec(x["bid_price"]),
                tick_bid_volume=int(x["bid_volume"] or 0),
                tick_ask=dec(x["ask_price"]),
                tick_ask_volume=int(x["ask_volume"] or 0),
                last=dec(x["last_price"]),
                closing=dec(x["closing_price"]),
                fetch_datetime=x["fetch_datetime"] if isinstance(x["fetch_datetime"], datetime) else fallback_time,
                implied_volatility=opt_dec(x["implied_volatility"]),
                delta=opt_dec(x["delta"]), gamma=opt_dec(x["gamma"]),
                theta=opt_dec(x["theta"]), vega=opt_dec(x["vega"]),
                rho=opt_dec(x["rho"]), leverage=opt_dec(x["leverage"]),
                moneyness_ratio=opt_dec(x["moneyness"]),
                greek_spread_percent=opt_dec(x["greek_spread_percent"]),
                open_interest=int(x["open_interest"] or 0),
            )
            if q.contract_size <= 0 or q.option_type not in {"CALL", "PUT"}:
                continue
            quotes.append(q)
            qmap[q.ins_code] = q
            umap[q.ua_ins_code] = q.spot

        book_rows = await db.fetch(
            f"""
            SELECT ins_code,`level`,snapshot_time,bid_price,bid_volume,ask_price,ask_volume
            FROM {qname('order_book_depth')}
            WHERE `level` BETWEEN 1 AND 5
            ORDER BY ins_code,`level`
            """
        )
        grouped: DefaultDict[str, List[BookLevel]] = defaultdict(list)
        times: Dict[str, datetime] = {}
        for r in book_rows:
            code = str(r["ins_code"])
            grouped[code].append(
                BookLevel(
                    int(r["level"]), dec(r["bid_price"]), int(r["bid_volume"] or 0),
                    dec(r["ask_price"]), int(r["ask_volume"] or 0),
                )
            )
            snap = r["snapshot_time"]
            if isinstance(snap, datetime):
                times[code] = max(times.get(code, snap), snap)
        books = {
            code: OrderBook(code, times.get(code, fallback_time), levels)
            for code, levels in grouped.items()
        }
        return quotes, qmap, umap, books

    async def _load_historical_volatility(self, db: DB) -> Dict[str, Tuple[Decimal, int]]:
        lookback_days = max(VOL_LOOKBACK_DAYS * 3, 180)
        rows = await db.fetch(
            f"""
            SELECT d.ins_code,d.trade_date,
                   COALESCE(NULLIF(d.close_price,0),NULLIF(d.last_price,0)) AS price
            FROM {qname('daily_market_data')} d
            JOIN {qname('underlying_assets')} u ON u.ua_ins_code=d.ins_code
            WHERE COALESCE(NULLIF(d.close_price,0),NULLIF(d.last_price,0))>0
              AND d.trade_date>=DATE_SUB(CURDATE(),INTERVAL %s DAY)
            ORDER BY d.ins_code,d.trade_date
            """,
            (lookback_days,),
        )
        prices: DefaultDict[str, List[float]] = defaultdict(list)
        for r in rows:
            prices[str(r["ins_code"])].append(float(r["price"]))
        result: Dict[str, Tuple[Decimal, int]] = {}
        for code, vals in prices.items():
            vals = vals[-(VOL_LOOKBACK_DAYS + 1):]
            returns = [
                math.log(vals[i] / vals[i - 1])
                for i in range(1, len(vals))
                if vals[i] > 0 and vals[i - 1] > 0
            ]
            if len(returns) >= MIN_VOL_RETURNS:
                annual_pct = Decimal(str(statistics.stdev(returns) * math.sqrt(252) * 100))
                result[code] = (pct(annual_pct), len(returns))
        return result

    async def _load_expected_return_history(self, db: DB, now: datetime) -> Dict[str, List[float]]:
        if (
            self._er_history_date == now.date()
            and self._er_history_loaded_at is not None
            and self._er_history_returns
            and (now - self._er_history_loaded_at).total_seconds() < EXPECTED_RETURN_HISTORY_CACHE_SECONDS
        ):
            return self._er_history_returns
        rows = await db.fetch(
            f"""
            SELECT d.ins_code,d.trade_date,
                   COALESCE(NULLIF(d.close_price,0),NULLIF(d.last_price,0)) AS price
            FROM {qname('daily_market_data')} d
            JOIN {qname('underlying_assets')} u ON u.ua_ins_code=d.ins_code
            WHERE COALESCE(NULLIF(d.close_price,0),NULLIF(d.last_price,0))>0
            ORDER BY d.ins_code,d.trade_date
            """
        )
        prices: DefaultDict[str, List[float]] = defaultdict(list)
        for row in rows:
            prices[str(row["ins_code"])].append(float(row["price"]))
        self._er_history_returns = {
            code: _daily_log_returns(vals[-EXPECTED_RETURN_HISTORY_ROWS:])
            for code, vals in prices.items()
        }
        self._er_history_date = now.date()
        self._er_history_loaded_at = now
        self._er_hist_multiplier_cache.clear()
        return self._er_history_returns

    def _history_terminal_multipliers(self, ua: str, dte: int) -> List[float]:
        key = (ua, int(dte))
        cached = self._er_hist_multiplier_cache.get(key)
        if cached is not None:
            return cached
        daily_returns = self._er_history_returns.get(ua, [])
        if len(daily_returns) < EXPECTED_RETURN_MIN_HISTORY_RETURNS:
            self._er_hist_multiplier_cache[key] = []
            return []
        horizon = max(
            1,
            round(
                max(1, int(dte))
                * EXPECTED_RETURN_TRADING_DAYS
                / EXPECTED_RETURN_CALENDAR_DAYS
            ),
        )
        n = len(daily_returns)
        window_count = n - horizon + 1
        if horizon <= n and window_count >= 12:
            multipliers = [
                math.exp(math.fsum(daily_returns[i:i + horizon]))
                for i in range(window_count)
            ]
        else:
            import random
            seed_bytes = hashlib.sha256(f"{ua}|{dte}|reyt40hist-final".encode()).digest()[:8]
            rng = random.Random(int.from_bytes(seed_bytes, "big"))
            multipliers = []
            for _ in range(EXPECTED_RETURN_BOOTSTRAP_PATHS):
                total = math.fsum(daily_returns[rng.randrange(n)] for _day in range(horizon))
                multipliers.append(math.exp(total))
        self._er_hist_multiplier_cache[key] = multipliers
        return multipliers

    def _iv_terminal_multipliers(self, c: Candidate, iv: float) -> List[float]:
        key = (c.ua_ins_code, int(c.days_to_expiry), round(float(iv), 6))
        cached = self._er_iv_multiplier_cache.get(key)
        if cached is not None:
            return cached
        daily_returns = self._er_history_returns.get(c.ua_ins_code, [])
        if len(daily_returns) < EXPECTED_RETURN_MIN_HISTORY_RETURNS or not (iv > 0):
            self._er_iv_multiplier_cache[key] = []
            return []
        t = max(1, int(c.days_to_expiry)) / float(EXPECTED_RETURN_CALENDAR_DAYS)
        mean_log_daily = math.fsum(daily_returns) / len(daily_returns)
        if len(daily_returns) > 1:
            var_daily = math.fsum((x - mean_log_daily) ** 2 for x in daily_returns) / (len(daily_returns) - 1)
        else:
            var_daily = 0.0
        hist_sigma_ann = math.sqrt(max(0.0, var_daily) * EXPECTED_RETURN_TRADING_DAYS)
        mu_arith_ann = mean_log_daily * EXPECTED_RETURN_TRADING_DAYS + 0.5 * hist_sigma_ann * hist_sigma_ann
        log_mean = (mu_arith_ann - 0.5 * iv * iv) * t
        vol_term = iv * math.sqrt(t)
        multipliers = [
            math.exp(log_mean + vol_term * z)
            for z in _EXPECTED_RETURN_Z_GRID
        ]
        self._er_iv_multiplier_cache[key] = multipliers
        return multipliers

    def _expected_return_pair(
        self,
        c: Candidate,
        capital_rial: Decimal,
    ) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, float]], Optional[float]]:
        hm = self._history_terminal_multipliers(c.ua_ins_code, c.days_to_expiry)
        history_eval = _evaluate_expected_return(c, hm, capital_rial)
        iv = _effective_candidate_iv(c)
        if iv is None:
            return history_eval, None, None
        im = self._iv_terminal_multipliers(c, iv)
        iv_eval = _evaluate_expected_return(c, im, capital_rial)
        return history_eval, iv_eval, iv

    def _apply_expected_return_hurdle(self, candidates: Dict[str, List[Candidate]]) -> None:
        for items in candidates.values():
            for c in items:
                if c.final_signal not in {"CANDIDATE", "STRONG_CANDIDATE"}:
                    c.details["expected_return_filter"] = "STRATEGY_INVALID"
                    continue
                history_eval, iv_eval, iv = self._expected_return_pair(c, c.unit_capital)
                if history_eval is None:
                    c.final_signal = "REJECT"
                    c.details["expected_return_filter"] = "NO_USABLE_HISTORY"
                    c.reason += " | Expected-return filter: no usable historical sample."
                    continue
                if iv_eval is None or iv is None:
                    c.final_signal = "REJECT"
                    c.details["expected_return_filter"] = "NO_USABLE_IV"
                    c.reason += " | Expected-return filter: no usable implied volatility."
                    continue
                h_pass = bool(history_eval["passes"])
                i_pass = bool(iv_eval["passes"])
                required = float(history_eval["required_return"])
                h_ret = float(history_eval["expected_return"])
                i_ret = float(iv_eval["expected_return"])
                both_pass = h_pass and i_pass if EXPECTED_RETURN_REQUIRE_BOTH else (h_pass or i_pass)
                conservative_edge = min(h_ret - required, i_ret - required)
                c.details.update({
                    "expected_return_filter": "PASS" if both_pass else "FAIL",
                    "expected_return_hurdle_ear_pct": float(EXPECTED_RETURN_HURDLE_EAR * D100),
                    "required_return_to_expiry_pct": required * 100.0,
                    "history_expected_return_to_expiry_pct": h_ret * 100.0,
                    "iv_expected_return_to_expiry_pct": i_ret * 100.0,
                    "history_expected_pnl_rial_per_unit": history_eval["expected_pnl_rial"],
                    "iv_expected_pnl_rial_per_unit": iv_eval["expected_pnl_rial"],
                    "history_scenarios": int(history_eval["scenarios"]),
                    "iv_scenarios": int(iv_eval["scenarios"]),
                    "effective_iv_pct": iv * 100.0,
                    "conservative_excess_return_pp": conservative_edge * 100.0,
                })
                if both_pass:
                    c.final_signal = "STRONG_CANDIDATE" if c.score >= Decimal("75") else "CANDIDATE"
                    c.reason += (
                        f" | Expected return PASS: history={h_ret*100:.2f}%, "
                        f"IV={i_ret*100:.2f}%, required={required*100:.2f}% to expiry."
                    )
                else:
                    c.final_signal = "REJECT"
                    c.reason += (
                        f" | Expected return FAIL: history={h_ret*100:.2f}%, "
                        f"IV={i_ret*100:.2f}%, required={required*100:.2f}% to expiry."
                    )

    def _best_signal_price(
        self,
        leg: Leg,
        quote: Optional[OptionQuote],
        books: Mapping[str, OrderBook],
        now: datetime,
        spot: Decimal,
    ) -> Decimal:
        book = books.get(leg.ins_code)
        if book and book.is_fresh(now, MAX_ENTRY_BOOK_AGE_SECONDS):
            px = book.best_price("BUY" if leg.side == "LONG" else "SELL")
            if px > D0:
                return px
        # Signal fallback is still a bid/ask, never an option last price. Paper
        # execution itself does NOT use this fallback; it requires fresh depth.
        if leg.kind == "OPTION" and quote:
            return quote.tick_ask if leg.side == "LONG" else quote.tick_bid
        return spot if leg.kind == "UNDERLYING" else D0

    def _make_candidate(
        self,
        account: Account,
        strategy: str,
        revision: str,
        now: datetime,
        ua: str,
        underlying_symbol: str,
        expiry: date,
        dte: int,
        spot: Decimal,
        legs: List[Leg],
        metrics: StrategyMetrics,
        valid: bool,
        score: Decimal,
        books: Mapping[str, OrderBook],
        reason: str,
        details: Dict[str, Any],
    ) -> Candidate:
        executable = executable_units_for_legs(legs, books, now)
        liquidity = liquidity_score(legs, books, now)
        risk_units = floor_int(FIXED_RISK_PER_TRADE_RIAL / metrics.unit_loss) if metrics.unit_loss > D0 else 0
        cash_units = floor_int(account.available_cash / metrics.unit_capital) if metrics.unit_capital > D0 else 0
        pretrade_units = floor_int(account.available_cash / metrics.pretrade_cash) if metrics.pretrade_cash > D0 else 0
        recommended = min(executable, risk_units, cash_units, pretrade_units)
        label = signal_label(valid, dte, metrics.unit_loss, executable, recommended, score)
        if valid and dte < MIN_DTE:
            reason = f"{reason} | REJECT: DTE {dte} < minimum {MIN_DTE}."
        elif valid and dte > MAX_DTE:
            reason = f"{reason} | REJECT: DTE {dte} > maximum {MAX_DTE}."
        merged = dict(details)
        merged.update(metrics.extras)
        merged.update({
            "source_revision": revision,
            "fixed_risk_budget_rial": money(FIXED_RISK_PER_TRADE_RIAL),
            "available_cash_at_scan_rial": money(account.available_cash),
            "pretrade_cash_per_unit_rial": money(metrics.pretrade_cash),
            "max_book_age_seconds": book_age_for_legs(legs, books, now),
            "fee_rates_pct": {
                "underlying_buy": UNDERLYING_BUY_FEE_PCT,
                "underlying_sell": UNDERLYING_SELL_FEE_PCT,
                "option_buy": OPTION_BUY_FEE_PCT,
                "option_sell": OPTION_SELL_FEE_PCT,
                "option_expiry": OPTION_EXPIRY_FEE_PCT,
            },
        })
        return Candidate(
            strategy=strategy,
            table=STRATEGY_TABLES[strategy],
            key=make_signal_key(strategy, ua, legs, now.date()),
            source_revision=revision,
            scan_time=now,
            ua_ins_code=ua,
            underlying_symbol=underlying_symbol,
            expiry=expiry,
            days_to_expiry=dte,
            spot=money(spot),
            legs=legs,
            unit_capital=money(metrics.unit_capital),
            unit_loss=money(metrics.unit_loss),
            unit_profit=money(metrics.unit_profit) if metrics.unit_profit is not None else None,
            be_low=money(metrics.be_low) if metrics.be_low is not None else None,
            be_high=money(metrics.be_high) if metrics.be_high is not None else None,
            reward_risk=pct(metrics.reward_risk) if metrics.reward_risk is not None else None,
            pretrade_cash=money(metrics.pretrade_cash),
            executable_units=executable,
            recommended_units=max(0, recommended),
            recommended_risk=money(metrics.unit_loss * Decimal(max(0, recommended))),
            recommended_capital=money(metrics.unit_capital * Decimal(max(0, recommended))),
            liquidity=liquidity,
            score=pct(score),
            final_signal=label,
            reason=reason,
            details=merged,
        )

    def _build_candidates(
        self,
        account: Account,
        quotes: Sequence[OptionQuote],
        books: Mapping[str, OrderBook],
        hist_vol: Mapping[str, Tuple[Decimal, int]],
        revision: str,
        now: datetime,
    ) -> Dict[str, List[Candidate]]:
        out: Dict[str, List[Candidate]] = {k: [] for k in STRATEGY_TABLES}
        valid_quotes = [
            q for q in quotes
            if q.days_to_expiry >= 0
            and q.contract_size > 0 and q.spot > 0 and q.strike > 0
        ]

        # ------------------------------------------------------------------
        # Covered Call + Protective Put (single option + underlying)
        # ------------------------------------------------------------------
        for q in valid_quotes:
            underlying_book = books.get(q.ua_ins_code)
            underlying_ask = (
                underlying_book.best_price("BUY")
                if underlying_book and underlying_book.is_fresh(now, MAX_ENTRY_BOOK_AGE_SECONDS)
                else q.spot
            )

            greek_details = {
                "implied_volatility": q.implied_volatility,
                "delta": q.delta, "gamma": q.gamma, "theta": q.theta,
                "vega": q.vega, "rho": q.rho, "leverage": q.leverage,
                "moneyness_ratio_1py": q.moneyness_ratio,
                "greek_spread_percent_1py": q.greek_spread_percent,
                "open_interest": q.open_interest,
            }

            # Covered Call policy: only ITM call options are eligible.
            # For a CALL, ITM means underlying spot is strictly above strike (S > K).
            # OTM and ATM calls are not even generated as Covered Call candidates.
            if q.option_type == "CALL" and q.strike < q.spot:
                base_legs = [
                    Leg(1, "UNDERLYING", q.ua_ins_code, q.underlying_symbol, "LONG", None, None, q.expiry, q.contract_size, underlying_ask),
                    Leg(2, "OPTION", q.ins_code, q.symbol, "SHORT", "CALL", q.strike, q.expiry, q.contract_size, D0),
                ]
                call_px = self._best_signal_price(base_legs[1], q, books, now, q.spot)
                legs = [base_legs[0], replace(base_legs[1], entry_price=call_px)]
                try:
                    metrics = strategy_metrics("COVERED_CALL", q.spot, legs, [underlying_ask, call_px])
                except ValueError:
                    continue
                ret = dec(metrics.extras.get("return_to_expiry_pct"))
                annual = ret * Decimal("365") / Decimal(max(1, q.days_to_expiry))
                # Positive percentage depth inside the money. Example: S=100, K=93 -> 7% ITM.
                itm_depth_pct = safe_div(q.spot - q.strike, q.spot) * D100
                # Keep the original Covered Call scoring weights, but flip the old
                # 7% OTM preference into a 7% ITM preference to match the new policy.
                age = book_age_for_legs(legs, books, now)
                liq = liquidity_score(legs, books, now)
                score = (
                    clamp(ret * Decimal("8")) * Decimal("0.30")
                    + clamp(D100 - abs(itm_depth_pct - Decimal("7")) * Decimal("8")) * Decimal("0.22")
                    + liq * Decimal("0.25")
                    + dte_score(q.days_to_expiry) * Decimal("0.13")
                    + freshness_score(age) * Decimal("0.10")
                )
                valid = (
                    underlying_ask > 0 and call_px > 0
                    and metrics.unit_capital > 0 and metrics.unit_loss > 0
                    and (metrics.unit_profit or D0) > 0
                    and q.strike < q.spot
                )
                details = dict(greek_details)
                details.update({
                    "annualized_return_pct": pct(annual),
                    "itm_depth_pct": pct(itm_depth_pct),
                    "moneyness_pct": pct(-itm_depth_pct),
                    # The original 1.ipynb Covered Call metrics are retained in
                    # explicit form while execution/risk uses order-book prices.
                    "notebook_reference_underlying_price_rial": q.spot,
                    "notebook_reference_option_price_rial": q.option_price,
                })
                out["COVERED_CALL"].append(
                    self._make_candidate(
                        account, "COVERED_CALL", revision, now, q.ua_ins_code,
                        q.underlying_symbol, q.expiry, q.days_to_expiry, q.spot,
                        legs, metrics, valid, score, books,
                        f"Covered Call | max loss {money(metrics.unit_loss/TOMAN_TO_RIAL):,.0f} toman | "
                        f"return {pct(ret)}% | ITM depth {pct(itm_depth_pct)}%",
                        details,
                    )
                )

            if q.option_type == "PUT":
                base_legs = [
                    Leg(1, "UNDERLYING", q.ua_ins_code, q.underlying_symbol, "LONG", None, None, q.expiry, q.contract_size, underlying_ask),
                    Leg(2, "OPTION", q.ins_code, q.symbol, "LONG", "PUT", q.strike, q.expiry, q.contract_size, D0),
                ]
                put_px = self._best_signal_price(base_legs[1], q, books, now, q.spot)
                legs = [base_legs[0], replace(base_legs[1], entry_price=put_px)]
                try:
                    metrics = strategy_metrics("PROTECTIVE_PUT", q.spot, legs, [underlying_ask, put_px])
                except ValueError:
                    continue
                insurance_pct = dec(metrics.extras.get("insurance_cost_pct"))
                gap = dec(metrics.extras.get("protection_gap_pct"))
                age = book_age_for_legs(legs, books, now)
                liq = liquidity_score(legs, books, now)
                score = (
                    clamp(D100 - insurance_pct * Decimal("10")) * Decimal("0.27")
                    + clamp(D100 - abs(gap - Decimal("5")) * Decimal("9")) * Decimal("0.27")
                    + liq * Decimal("0.24")
                    + dte_score(q.days_to_expiry) * Decimal("0.12")
                    + freshness_score(age) * Decimal("0.10")
                )
                valid = (
                    underlying_ask > 0 and put_px > 0
                    and metrics.unit_capital > 0 and metrics.unit_loss > 0
                    and q.spot * Decimal("0.75") <= q.strike <= q.spot * Decimal("1.10")
                )
                out["PROTECTIVE_PUT"].append(
                    self._make_candidate(
                        account, "PROTECTIVE_PUT", revision, now, q.ua_ins_code,
                        q.underlying_symbol, q.expiry, q.days_to_expiry, q.spot,
                        legs, metrics, valid, score, books,
                        f"Protective Put | max loss {money(metrics.unit_loss/TOMAN_TO_RIAL):,.0f} toman | "
                        f"insurance {pct(insurance_pct)}% | protection gap {pct(gap)}%",
                        greek_details,
                    )
                )

        # ------------------------------------------------------------------
        # Two-option strategies. Limit pairing distance for computational
        # efficiency; signal rows themselves are NOT truncated.
        # ------------------------------------------------------------------
        groups: DefaultDict[Tuple[str, date, int], List[OptionQuote]] = defaultdict(list)
        for q in valid_quotes:
            groups[(q.ua_ins_code, q.expiry, q.contract_size)].append(q)

        for group in groups.values():
            calls = sorted((q for q in group if q.option_type == "CALL"), key=lambda x: x.strike)
            puts = sorted((q for q in group if q.option_type == "PUT"), key=lambda x: x.strike)

            for i, long_q in enumerate(calls):
                for short_q in calls[i + 1:i + 1 + MAX_STRIKE_STEPS]:
                    long_leg = Leg(1, "OPTION", long_q.ins_code, long_q.symbol, "LONG", "CALL", long_q.strike, long_q.expiry, long_q.contract_size, D0)
                    short_leg = Leg(2, "OPTION", short_q.ins_code, short_q.symbol, "SHORT", "CALL", short_q.strike, short_q.expiry, short_q.contract_size, D0)
                    lp = self._best_signal_price(long_leg, long_q, books, now, long_q.spot)
                    sp = self._best_signal_price(short_leg, short_q, books, now, short_q.spot)
                    legs = [replace(long_leg, entry_price=lp), replace(short_leg, entry_price=sp)]
                    try:
                        metrics = strategy_metrics("BULL_CALL_SPREAD", long_q.spot, legs, [lp, sp])
                    except ValueError:
                        continue
                    rr = metrics.reward_risk or D0
                    move = dec(metrics.extras.get("required_spot_move_pct"))
                    age = book_age_for_legs(legs, books, now)
                    liq = liquidity_score(legs, books, now)
                    score = (
                        clamp(rr * Decimal("30")) * Decimal("0.34")
                        + clamp(D100 - max(D0, move) * Decimal("10")) * Decimal("0.20")
                        + liq * Decimal("0.24")
                        + dte_score(long_q.days_to_expiry) * Decimal("0.12")
                        + freshness_score(age) * Decimal("0.10")
                    )
                    valid = lp > 0 and sp > 0 and metrics.unit_loss > 0 and (metrics.unit_profit or D0) > 0
                    details = {
                        "long_delta": long_q.delta, "short_delta": short_q.delta,
                        "long_iv": long_q.implied_volatility, "short_iv": short_q.implied_volatility,
                    }
                    out["BULL_CALL_SPREAD"].append(
                        self._make_candidate(
                            account, "BULL_CALL_SPREAD", revision, now, long_q.ua_ins_code,
                            long_q.underlying_symbol, long_q.expiry, long_q.days_to_expiry,
                            long_q.spot, legs, metrics, valid, score, books,
                            f"Bull Call Spread | reward/risk {pct(rr)} | move {pct(move)}% | "
                            f"max loss {money(metrics.unit_loss/TOMAN_TO_RIAL):,.0f} toman",
                            details,
                        )
                    )

            for i, short_q in enumerate(puts):
                for long_q in puts[i + 1:i + 1 + MAX_STRIKE_STEPS]:
                    long_leg = Leg(1, "OPTION", long_q.ins_code, long_q.symbol, "LONG", "PUT", long_q.strike, long_q.expiry, long_q.contract_size, D0)
                    short_leg = Leg(2, "OPTION", short_q.ins_code, short_q.symbol, "SHORT", "PUT", short_q.strike, short_q.expiry, short_q.contract_size, D0)
                    lp = self._best_signal_price(long_leg, long_q, books, now, long_q.spot)
                    sp = self._best_signal_price(short_leg, short_q, books, now, short_q.spot)
                    legs = [replace(long_leg, entry_price=lp), replace(short_leg, entry_price=sp)]
                    try:
                        metrics = strategy_metrics("BEAR_PUT_SPREAD", long_q.spot, legs, [lp, sp])
                    except ValueError:
                        continue
                    rr = metrics.reward_risk or D0
                    drop = dec(metrics.extras.get("required_spot_drop_pct"))
                    age = book_age_for_legs(legs, books, now)
                    liq = liquidity_score(legs, books, now)
                    score = (
                        clamp(rr * Decimal("30")) * Decimal("0.34")
                        + clamp(D100 - max(D0, drop) * Decimal("10")) * Decimal("0.20")
                        + liq * Decimal("0.24")
                        + dte_score(long_q.days_to_expiry) * Decimal("0.12")
                        + freshness_score(age) * Decimal("0.10")
                    )
                    valid = lp > 0 and sp > 0 and metrics.unit_loss > 0 and (metrics.unit_profit or D0) > 0
                    details = {
                        "long_delta": long_q.delta, "short_delta": short_q.delta,
                        "long_iv": long_q.implied_volatility, "short_iv": short_q.implied_volatility,
                    }
                    out["BEAR_PUT_SPREAD"].append(
                        self._make_candidate(
                            account, "BEAR_PUT_SPREAD", revision, now, long_q.ua_ins_code,
                            long_q.underlying_symbol, long_q.expiry, long_q.days_to_expiry,
                            long_q.spot, legs, metrics, valid, score, books,
                            f"Bear Put Spread | reward/risk {pct(rr)} | drop {pct(drop)}% | "
                            f"max loss {money(metrics.unit_loss/TOMAN_TO_RIAL):,.0f} toman",
                            details,
                        )
                    )

            call_map = {q.strike: q for q in calls}
            put_map = {q.strike: q for q in puts}
            for strike in sorted(set(call_map) & set(put_map)):
                call_q, put_q = call_map[strike], put_map[strike]
                call_leg = Leg(1, "OPTION", call_q.ins_code, call_q.symbol, "LONG", "CALL", strike, call_q.expiry, call_q.contract_size, D0)
                put_leg = Leg(2, "OPTION", put_q.ins_code, put_q.symbol, "LONG", "PUT", strike, put_q.expiry, put_q.contract_size, D0)
                cp = self._best_signal_price(call_leg, call_q, books, now, call_q.spot)
                pp = self._best_signal_price(put_leg, put_q, books, now, put_q.spot)
                legs = [replace(call_leg, entry_price=cp), replace(put_leg, entry_price=pp)]
                try:
                    metrics = strategy_metrics("LONG_STRADDLE", call_q.spot, legs, [cp, pp])
                except ValueError:
                    continue
                required_move = dec(metrics.extras.get("required_move_pct"))
                moneyness_pct = abs(safe_div(strike - call_q.spot, call_q.spot) * D100)
                vol_pct, obs = hist_vol.get(call_q.ua_ins_code, (D0, 0))
                expected_move = (
                    vol_pct * Decimal(str(math.sqrt(max(1, call_q.days_to_expiry) / 365.0)))
                    if vol_pct > D0 else D0
                )
                coverage = safe_div(expected_move, required_move)
                age = book_age_for_legs(legs, books, now)
                liq = liquidity_score(legs, books, now)
                score = (
                    clamp(coverage * Decimal("50")) * Decimal("0.34")
                    + clamp(D100 - moneyness_pct * Decimal("12")) * Decimal("0.20")
                    + liq * Decimal("0.24")
                    + dte_score(call_q.days_to_expiry) * Decimal("0.12")
                    + freshness_score(age) * Decimal("0.10")
                )
                valid = cp > 0 and pp > 0 and metrics.unit_loss > 0 and moneyness_pct <= Decimal("10") and obs >= MIN_VOL_RETURNS
                details = {
                    "annualized_volatility_pct": pct(vol_pct),
                    "expected_move_to_expiry_pct": pct(expected_move),
                    "volatility_observations": obs,
                    "call_iv": call_q.implied_volatility, "put_iv": put_q.implied_volatility,
                    "call_delta": call_q.delta, "put_delta": put_q.delta,
                }
                out["LONG_STRADDLE"].append(
                    self._make_candidate(
                        account, "LONG_STRADDLE", revision, now, call_q.ua_ins_code,
                        call_q.underlying_symbol, call_q.expiry, call_q.days_to_expiry,
                        call_q.spot, legs, metrics, valid, score, books,
                        f"Long Straddle | required move {pct(required_move)}% | expected hist move {pct(expected_move)}% | "
                        f"max loss {money(metrics.unit_loss/TOMAN_TO_RIAL):,.0f} toman",
                        details,
                    )
                )

        # No MAX_SIGNALS_PER_STRATEGY truncation: every generated candidate is
        # persisted. Sorting is only for deterministic output/inspection.
        for strategy, items in out.items():
            items.sort(key=lambda c: (c.priority_tuple, c.key), reverse=True)
        return out

    @staticmethod
    def _common_signal_columns() -> List[str]:
        return [
            "signal_key", "signal_date", "scan_time", "last_seen_at", "account_id", "strategy_code",
            "ua_ins_code", "underlying_symbol", "expiry_date", "days_to_expiry", "spot_price_rial",
            "leg1_kind", "leg1_ins_code", "leg1_symbol", "leg1_side", "leg1_option_type", "leg1_strike_rial",
            "leg1_entry_price_rial", "leg1_contract_size", "leg2_kind", "leg2_ins_code", "leg2_symbol",
            "leg2_side", "leg2_option_type", "leg2_strike_rial", "leg2_entry_price_rial", "leg2_contract_size",
            "unit_capital_rial", "unit_max_loss_rial", "unit_max_profit_rial", "breakeven_low_rial",
            "breakeven_high_rial", "reward_risk_ratio", "executable_units", "risk_budget_rial",
            "recommended_units", "recommended_risk_rial", "recommended_capital_rial", "liquidity_score",
            "strategy_score", "final_signal", "signal_reason",
        ]

    @staticmethod
    def _strategy_extra_columns(strategy: str) -> List[str]:
        return {
            "COVERED_CALL": ["premium_income_rial", "return_to_expiry_pct", "annualized_return_pct", "moneyness_pct"],
            "PROTECTIVE_PUT": ["insurance_cost_rial", "insurance_cost_pct", "protection_gap_pct"],
            "BULL_CALL_SPREAD": ["net_debit_per_unit_rial", "strike_width_rial", "required_spot_move_pct"],
            "BEAR_PUT_SPREAD": ["net_debit_per_unit_rial", "strike_width_rial", "required_spot_drop_pct"],
            "LONG_STRADDLE": ["total_premium_per_unit_rial", "required_move_pct", "annualized_volatility_pct", "expected_move_to_expiry_pct", "volatility_observations"],
        }[strategy]

    def _signal_row(self, account_id: int, c: Candidate, extra_cols: Sequence[str]) -> Tuple[Any, ...]:
        a, b = c.legs
        common: List[Any] = [
            c.key, c.scan_time.date(), c.scan_time, c.scan_time, account_id, c.strategy,
            c.ua_ins_code, c.underlying_symbol, c.expiry, c.days_to_expiry, c.spot,
            a.kind, a.ins_code, a.symbol, a.side, a.option_type, a.strike, a.entry_price, a.contract_size,
            b.kind, b.ins_code, b.symbol, b.side, b.option_type, b.strike, b.entry_price, b.contract_size,
            c.unit_capital, c.unit_loss, c.unit_profit, c.be_low, c.be_high, c.reward_risk,
            c.executable_units, money(FIXED_RISK_PER_TRADE_RIAL), c.recommended_units,
            c.recommended_risk, c.recommended_capital, c.liquidity, c.score,
            c.final_signal, c.reason,
        ]
        extras = [c.details.get(name) for name in extra_cols]
        details_json = json.dumps(c.details, ensure_ascii=False, default=json_default)
        return tuple(common + extras + [details_json, 1])

    async def _upsert_signals(
        self,
        db: DB,
        account: Account,
        candidates: Dict[str, List[Candidate]],
    ) -> int:
        total = 0
        common_cols = self._common_signal_columns()
        for strategy, items in candidates.items():
            table = qname(STRATEGY_TABLES[strategy])
            await db.execute(
                f"UPDATE {table} SET is_current=0 WHERE account_id=%s AND is_current=1",
                (account.account_id,),
            )
            extra_cols = self._strategy_extra_columns(strategy)
            cols = common_cols + list(extra_cols) + ["details_json", "is_current"]
            if items:
                placeholders = ",".join(["%s"] * len(cols))
                update_cols = [
                    x for x in cols
                    if x not in {"signal_key", "signal_date", "account_id", "strategy_code", "is_current"}
                ]
                update_sql = ",".join(f"{x}=VALUES({x})" for x in update_cols)
                sql = f"""
                    INSERT INTO {table} ({','.join(cols)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_sql},is_current=1,updated_at=CURRENT_TIMESTAMP
                """
                rows = [self._signal_row(account.account_id, c, extra_cols) for c in items]
                await db.executemany(sql, rows)
                total += len(items)

            id_rows = await db.fetch(
                f"SELECT signal_id,signal_key,opened_position_id FROM {table} WHERE account_id=%s AND is_current=1",
                (account.account_id,),
            )
            idmap = {
                str(r["signal_key"]): (
                    int(r["signal_id"]),
                    int(r["opened_position_id"]) if r["opened_position_id"] is not None else None,
                )
                for r in id_rows
            }
            for c in items:
                if c.key in idmap:
                    c.signal_id, c.opened_position_id = idmap[c.key]
        return total

    async def _load_open_positions(self, db: DB, account_id: int) -> Dict[int, Dict[str, Any]]:
        rows = await db.fetch(
            f"""
            SELECT p.position_id,p.strategy_code,p.ua_ins_code,p.underlying_symbol,
                   p.expiry_date,p.units,p.entry_max_loss_rial,p.entry_capital_rial,
                   p.entry_net_value_rial,p.max_favorable_pnl_rial,p.max_adverse_pnl_rial,
                   l.leg_id,l.leg_no,l.instrument_kind,l.ins_code,l.symbol,l.side,
                   l.option_type,l.strike_price_rial,l.contract_size,l.quantity_units,
                   l.entry_price_rial,l.latest_price_rial
            FROM {qname('paper_positions')} p
            JOIN {qname('paper_position_legs')} l ON l.position_id=p.position_id
            WHERE p.account_id=%s AND p.status='OPEN'
            ORDER BY p.position_id,l.leg_no
            """,
            (account_id,),
        )
        positions: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            pid = int(r["position_id"])
            if pid not in positions:
                positions[pid] = {
                    "position_id": pid,
                    "strategy": str(r["strategy_code"]),
                    "ua": str(r["ua_ins_code"]),
                    "underlying_symbol": r["underlying_symbol"],
                    "expiry": r["expiry_date"],
                    "units": int(r["units"]),
                    "entry_loss": dec(r["entry_max_loss_rial"]),
                    "entry_capital": dec(r["entry_capital_rial"]),
                    "entry_net": dec(r["entry_net_value_rial"]),
                    "mfe": dec(r["max_favorable_pnl_rial"]),
                    "mae": dec(r["max_adverse_pnl_rial"]),
                    "legs": [],
                }
            positions[pid]["legs"].append({
                "leg_id": int(r["leg_id"]), "no": int(r["leg_no"]),
                "kind": str(r["instrument_kind"]), "ins_code": str(r["ins_code"]),
                "symbol": r["symbol"], "side": str(r["side"]),
                "option_type": r["option_type"],
                "strike": dec(r["strike_price_rial"]) if r["strike_price_rial"] is not None else None,
                "contract_size": int(r["contract_size"]), "quantity": int(r["quantity_units"]),
                "entry": dec(r["entry_price_rial"]), "latest": dec(r["latest_price_rial"]),
            })
        return positions

    async def _settlement_spot(self, db: DB, ua: str, expiry: date, fallback: Decimal) -> Tuple[Decimal, str]:
        value = await db.fetchval(
            f"""
            SELECT COALESCE(NULLIF(close_price,0),NULLIF(last_price,0))
            FROM {qname('daily_market_data')}
            WHERE ins_code=%s AND trade_date<=%s
              AND COALESCE(NULLIF(close_price,0),NULLIF(last_price,0))>0
            ORDER BY trade_date DESC LIMIT 1
            """,
            (ua, expiry),
        )
        if value is not None:
            return dec(value), "daily_market_data"
        return fallback, "live_underlying_fallback"

    @staticmethod
    def _intrinsic(option_type: str, spot: Decimal, strike: Decimal) -> Decimal:
        return max(D0, spot - strike) if option_type == "CALL" else max(D0, strike - spot)

    def _live_leg_value(
        self,
        leg: Dict[str, Any],
        qmap: Mapping[str, OptionQuote],
        umap: Mapping[str, Decimal],
        books: Mapping[str, OrderBook],
        now: datetime,
    ) -> Tuple[Decimal, Decimal, bool, str]:
        qty = int(leg["quantity"])
        book = books.get(leg["ins_code"])
        stale = book is None or not book.is_fresh(now, MAX_MARK_BOOK_AGE_SECONDS)

        if leg["kind"] == "UNDERLYING":
            px = book.best_price("SELL") if book and not stale else D0
            if px <= D0:
                px = umap.get(leg["ins_code"], D0) or leg["latest"] or leg["entry"]
                stale = True
            value = _underlying_sell_value(px, qty)
            return px, value, stale, "underlying_bid_or_fallback"

        q = qmap.get(leg["ins_code"])
        if leg["side"] == "LONG":
            px = book.best_price("SELL") if book and not stale else D0
            if px <= D0 and q:
                px = q.tick_bid
            if px <= D0 and q:
                px = q.last if q.last > D0 else q.closing
                stale = True
            if px <= D0:
                px = leg["latest"] or leg["entry"]
                stale = True
            value = _option_sell_value(px, qty)
        else:
            px = book.best_price("BUY") if book and not stale else D0
            if px <= D0 and q:
                px = q.tick_ask
            if px <= D0 and q:
                px = q.last if q.last > D0 else q.closing
                stale = True
            if px <= D0:
                px = leg["latest"] or leg["entry"]
                stale = True
            gross = px * Decimal(qty)
            value = -(gross + fee(gross, OPTION_BUY_FEE_PCT))
        return px, value, stale, "option_liquidation_book_or_fallback"

    def _expiry_leg_value(self, leg: Dict[str, Any], spot: Decimal) -> Tuple[Decimal, Decimal, str]:
        qty = int(leg["quantity"])
        if leg["kind"] == "UNDERLYING":
            return spot, _underlying_sell_value(spot, qty), "expiry_underlying_spot"
        strike = dec(leg["strike"])
        intrinsic = self._intrinsic(str(leg["option_type"]), spot, strike)
        gross = intrinsic * Decimal(qty)
        expiry_fee = _option_expiry_fee(strike, qty) if intrinsic > D0 else D0
        if leg["side"] == "LONG":
            value = gross - expiry_fee
        else:
            value = -gross - expiry_fee
        return intrinsic, value, "expiry_intrinsic"

    async def _value_positions(
        self,
        db: DB,
        account_id: int,
        qmap: Mapping[str, OptionQuote],
        umap: Mapping[str, Decimal],
        books: Mapping[str, OrderBook],
        now: datetime,
        manual_close: Optional[set[int]] = None,
    ) -> Tuple[int, int]:
        manual_close = manual_close or set()
        positions = await self._load_open_positions(db, account_id)
        valued = 0
        closed = 0

        for p in positions.values():
            expired = (
                now.date() > p["expiry"]
                or (now.date() == p["expiry"] and now.time() >= MARKET_CLOSE)
            )
            spot = umap.get(p["ua"], D0)
            settlement_source = "live"
            if expired:
                spot, settlement_source = await self._settlement_spot(db, p["ua"], p["expiry"], spot)

            current_net = D0
            stale = False
            marks: List[Dict[str, Any]] = []
            updates: List[Tuple[Decimal, int]] = []
            for leg in p["legs"]:
                if expired:
                    px, value, why = self._expiry_leg_value(leg, spot)
                    leg_stale = False
                else:
                    px, value, leg_stale, why = self._live_leg_value(leg, qmap, umap, books, now)
                current_net += value
                stale = stale or leg_stale
                updates.append((money(px), leg["leg_id"]))
                marks.append({
                    "leg_no": leg["no"], "symbol": leg["symbol"], "side": leg["side"],
                    "mark_price_rial": money(px), "net_leg_value_rial": money(value),
                    "quantity_units": leg["quantity"], "stale": leg_stale, "source": why,
                })

            pnl = current_net - p["entry_net"]
            ror = safe_div(pnl, p["entry_loss"]) * D100 if p["entry_loss"] > D0 else D0
            mfe = max(p["mfe"], pnl)
            mae = min(p["mae"], pnl)
            status: Optional[str] = None
            reason: Optional[str] = None
            if p["position_id"] in manual_close:
                status = "CLOSED"
                reason = "Manual paper close override requested."
            elif expired:
                status = "EXPIRED"
                reason = f"Held to expiry; settlement source={settlement_source}; underlying={money(spot)} rial."

            await db.executemany(
                f"UPDATE {qname('paper_position_legs')} SET latest_price_rial=%s,updated_at=CURRENT_TIMESTAMP WHERE leg_id=%s",
                updates,
            )
            await db.execute(
                f"""
                INSERT INTO {qname('paper_position_valuations')} (
                    position_id,valuation_time,position_value_rial,unrealized_pnl_rial,
                    return_on_risk_pct,underlying_price_rial,max_favorable_pnl_rial,
                    max_adverse_pnl_rial,quote_is_stale,valuation_reason,leg_marks_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    position_value_rial=VALUES(position_value_rial),
                    unrealized_pnl_rial=VALUES(unrealized_pnl_rial),
                    return_on_risk_pct=VALUES(return_on_risk_pct),
                    underlying_price_rial=VALUES(underlying_price_rial),
                    max_favorable_pnl_rial=VALUES(max_favorable_pnl_rial),
                    max_adverse_pnl_rial=VALUES(max_adverse_pnl_rial),
                    quote_is_stale=VALUES(quote_is_stale),valuation_reason=VALUES(valuation_reason),
                    leg_marks_json=VALUES(leg_marks_json)
                """,
                (
                    p["position_id"], now, money(current_net), money(pnl), pct(ror),
                    money(spot) if spot > D0 else None, money(mfe), money(mae), stale,
                    reason or ("Conservative fee-aware liquidation marks." if not stale else "Fallback/stale mark used."),
                    json.dumps(marks, ensure_ascii=False, default=json_default),
                ),
            )

            if status:
                await db.execute(
                    f"""
                    UPDATE {qname('paper_positions')}
                    SET status=%s,current_position_value_rial=%s,current_underlying_price_rial=%s,
                        unrealized_pnl_rial=0,realized_pnl_rial=%s,return_on_risk_pct=%s,
                        max_favorable_pnl_rial=%s,max_adverse_pnl_rial=%s,last_valued_at=%s,
                        closed_at=%s,close_reason=%s,updated_at=CURRENT_TIMESTAMP
                    WHERE position_id=%s
                    """,
                    (
                        status, money(current_net), money(spot) if spot > D0 else None,
                        money(pnl), pct(ror), money(mfe), money(mae), now, now, reason,
                        p["position_id"],
                    ),
                )
                await db.executemany(
                    f"UPDATE {qname('paper_position_legs')} SET exit_price_rial=%s,updated_at=CURRENT_TIMESTAMP WHERE leg_id=%s",
                    updates,
                )
                closed += 1
            else:
                await db.execute(
                    f"""
                    UPDATE {qname('paper_positions')}
                    SET current_position_value_rial=%s,current_underlying_price_rial=%s,
                        unrealized_pnl_rial=%s,return_on_risk_pct=%s,
                        max_favorable_pnl_rial=%s,max_adverse_pnl_rial=%s,
                        last_valued_at=%s,updated_at=CURRENT_TIMESTAMP
                    WHERE position_id=%s
                    """,
                    (
                        money(current_net), money(spot) if spot > D0 else None, money(pnl),
                        pct(ror), money(mfe), money(mae), now, p["position_id"],
                    ),
                )
            valued += 1
        return valued, closed

    async def _update_account(self, db: DB, account: Account, now: datetime, history: bool = True) -> Account:
        row = await db.fetchrow(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN status<>'OPEN' THEN realized_pnl_rial ELSE 0 END),0) AS realized,
                COALESCE(SUM(CASE WHEN status='OPEN' THEN unrealized_pnl_rial ELSE 0 END),0) AS unrealized,
                COALESCE(SUM(CASE WHEN status='OPEN' THEN entry_max_loss_rial ELSE 0 END),0) AS reserved,
                COALESCE(SUM(CASE WHEN status='OPEN' THEN entry_capital_rial ELSE 0 END),0) AS allocated,
                COALESCE(SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END),0) AS open_count
            FROM {qname('paper_positions')}
            WHERE account_id=%s
            """,
            (account.account_id,),
        )
        if row is None:
            raise RuntimeError("Could not aggregate paper account.")
        realized = dec(row["realized"])
        unrealized = dec(row["unrealized"])
        reserved = dec(row["reserved"])
        allocated = dec(row["allocated"])
        count = int(row["open_count"] or 0)
        equity = account.initial_equity + realized + unrealized
        high = max(account.high_watermark, equity)
        drawdown = safe_div(high - equity, high) * D100 if high > D0 else D0

        await db.execute(
            f"""
            UPDATE {qname('paper_strategy_account')}
            SET current_equity_rial=%s,realized_pnl_rial=%s,unrealized_pnl_rial=%s,
                reserved_risk_rial=%s,allocated_capital_rial=%s,open_positions_count=%s,
                high_watermark_rial=%s,drawdown_pct=%s,last_scan_at=%s,updated_at=CURRENT_TIMESTAMP
            WHERE account_id=%s
            """,
            (
                money(equity), money(realized), money(unrealized), money(reserved),
                money(allocated), count, money(high), pct(drawdown), now, account.account_id,
            ),
        )
        if history:
            await db.execute(
                f"""
                INSERT INTO {qname('paper_account_equity_history')} (
                    account_id,snapshot_time,current_equity_rial,realized_pnl_rial,
                    unrealized_pnl_rial,reserved_risk_rial,allocated_capital_rial,
                    open_positions_count,drawdown_pct
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    current_equity_rial=VALUES(current_equity_rial),realized_pnl_rial=VALUES(realized_pnl_rial),
                    unrealized_pnl_rial=VALUES(unrealized_pnl_rial),reserved_risk_rial=VALUES(reserved_risk_rial),
                    allocated_capital_rial=VALUES(allocated_capital_rial),open_positions_count=VALUES(open_positions_count),
                    drawdown_pct=VALUES(drawdown_pct)
                """,
                (
                    account.account_id, now, money(equity), money(realized), money(unrealized),
                    money(reserved), money(allocated), count, pct(drawdown),
                ),
            )
        return Account(
            account.account_id, account.account_name, account.initial_equity, money(equity),
            money(realized), money(unrealized), money(reserved), money(allocated), count, money(high),
        )

    async def _open_signatures(self, db: DB, account_id: int) -> set[str]:
        rows = await db.fetch(
            f"SELECT position_signature FROM {qname('paper_positions')} WHERE account_id=%s AND status='OPEN'",
            (account_id,),
        )
        return {str(r["position_signature"]) for r in rows}

    async def _set_signal_execution_status(
        self,
        db: DB,
        c: Candidate,
        status: str,
        now: datetime,
        reason_code: Optional[str] = None,
        reason: Optional[str] = None,
        position_id: Optional[int] = None,
    ) -> None:
        if c.signal_id is None:
            raise RuntimeError("Cannot set paper execution status without signal_id.")
        table = qname(c.table)
        if status == "EXECUTED":
            if position_id is None:
                raise RuntimeError("EXECUTED status requires a paper position id.")
            await db.execute(
                f"""
                UPDATE {table}
                SET details_json=%s,
                    executed_on_paper_account=1,
                    paper_execution_status='EXECUTED',
                    paper_execution_reason_code=%s,
                    paper_execution_reason=%s,
                    paper_execution_checked_at=%s,
                    paper_executed_at=%s,
                    opened_position_id=%s,
                    updated_at=CURRENT_TIMESTAMP
                WHERE signal_id=%s
                """,
                (json.dumps(c.details, ensure_ascii=False, default=json_default),
                 reason_code or "POSITION_OPENED", reason or "Opened on shared paper account.",
                 now, now, position_id, c.signal_id),
            )
            return
        if status != "NOT_EXECUTED":
            raise ValueError(f"Unsupported execution status: {status}")
        # Never downgrade a signal that was already linked to a real paper position.
        await db.execute(
            f"""
            UPDATE {table}
            SET details_json=%s,
                executed_on_paper_account=0,
                paper_execution_status='NOT_EXECUTED',
                paper_execution_reason_code=%s,
                paper_execution_reason=%s,
                paper_execution_checked_at=%s,
                paper_executed_at=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE signal_id=%s
              AND opened_position_id IS NULL
              AND paper_execution_status<>'EXECUTED'
            """,
            (json.dumps(c.details, ensure_ascii=False, default=json_default), reason_code, reason, now, c.signal_id),
        )

    @staticmethod
    def _non_entry_reason(c: Candidate) -> Tuple[str, str]:
        er_state = str(c.details.get("expected_return_filter") or "")
        if er_state == "NO_USABLE_HISTORY":
            return "EXPECTED_RETURN_NO_HISTORY", "No usable retained underlying history for the expected-return model."
        if er_state == "NO_USABLE_IV":
            return "EXPECTED_RETURN_NO_IV", "No usable implied volatility for the expected-return model."
        if er_state == "FAIL":
            return "EXPECTED_RETURN_BELOW_40PCT", "History and IV did not both beat the configured effective-annual return hurdle."
        return "STRATEGY_FILTER_REJECTED", "Strategy validity / expected-return filters rejected this candidate."

    def _execution_books(self, books: Mapping[str, OrderBook]) -> Dict[str, OrderBook]:
        return {k: v.clone() for k, v in books.items()}

    def _plan_execution(
        self,
        c: Candidate,
        books: Mapping[str, OrderBook],
        available_cash: Decimal,
        max_entry_capital: Optional[Decimal] = None,
    ) -> ExecutionDecision:
        if c.unit_loss <= D0:
            return ExecutionDecision(None, "INVALID_MAX_LOSS", "Candidate max-loss calculation is not positive.")
        if c.unit_loss > FIXED_RISK_PER_TRADE_RIAL:
            return ExecutionDecision(None, "RISK_BUDGET_EXCEEDED", "One unit exceeds the fixed 10M toman max-loss budget.")

        for leg in c.legs:
            book = books.get(leg.ins_code)
            if book is None:
                return ExecutionDecision(None, "ORDER_BOOK_MISSING", f"No order book is available for {leg.symbol}.")
            if not book.is_fresh(c.scan_time, MAX_ENTRY_BOOK_AGE_SECONDS):
                return ExecutionDecision(None, "STALE_ORDER_BOOK", f"Order book for {leg.symbol} is too old for entry.")
            if book.capacity_units(leg) <= 0:
                return ExecutionDecision(None, "INSUFFICIENT_ORDER_BOOK_DEPTH", f"No executable depth is available for {leg.symbol}.")

        live_exec = executable_units_for_legs(c.legs, books, c.scan_time)
        if live_exec <= 0:
            return ExecutionDecision(None, "INSUFFICIENT_ORDER_BOOK_DEPTH", "Combined leg depth cannot execute one strategy unit.")

        risk_units = floor_int(FIXED_RISK_PER_TRADE_RIAL / c.unit_loss)
        if risk_units <= 0:
            return ExecutionDecision(None, "RISK_BUDGET_EXCEEDED", "Fixed risk budget cannot fund one strategy unit.")

        allocation_units = live_exec
        if max_entry_capital is not None:
            if max_entry_capital <= D0:
                return ExecutionDecision(
                    None, "STRATEGY_ALLOCATION_CAP_REACHED",
                    f"The {MAX_STRATEGY_ALLOCATION_PCT}% per-strategy allocation cap is already full."
                )
            if c.unit_capital > D0:
                allocation_units = min(allocation_units, floor_int(max_entry_capital / c.unit_capital))
            if allocation_units <= 0:
                return ExecutionDecision(
                    None, "STRATEGY_ALLOCATION_CAP_REACHED",
                    "Remaining per-strategy allocation cannot fund one strategy unit."
                )

        cash_units = live_exec
        if c.unit_capital > D0:
            cash_units = min(cash_units, floor_int(available_cash / c.unit_capital))
        if c.pretrade_cash > D0:
            cash_units = min(cash_units, floor_int(available_cash / c.pretrade_cash))
        if cash_units <= 0:
            return ExecutionDecision(
                None, "INSUFFICIENT_CASH",
                f"Available cash {money(available_cash/TOMAN_TO_RIAL):,.0f} toman cannot fund one strategy unit."
            )

        target = min(live_exec, risk_units, cash_units, allocation_units)
        if target <= 0:
            return ExecutionDecision(None, "NO_EXECUTABLE_UNITS", "Risk, cash, strategy-allocation, and order-book constraints leave zero executable units.")

        plan: Optional[ExecutionPlan] = None
        for _ in range(6):
            prices: List[Decimal] = []
            for leg in c.legs:
                book = books.get(leg.ins_code)
                if book is None or not book.is_fresh(c.scan_time, MAX_ENTRY_BOOK_AGE_SECONDS):
                    return ExecutionDecision(None, "STALE_ORDER_BOOK", f"Fresh depth disappeared for {leg.symbol}.")
                px = book.quote_vwap(leg, target)
                if px is None or px <= D0:
                    return ExecutionDecision(None, "INSUFFICIENT_ORDER_BOOK_DEPTH", f"Five-level depth cannot fill {target} unit(s) for {leg.symbol}.")
                prices.append(px)

            metrics = strategy_metrics(c.strategy, c.spot, c.legs, prices)
            if metrics.unit_loss <= D0 or metrics.unit_capital <= D0:
                return ExecutionDecision(None, "INVALID_EXECUTION_METRICS", "VWAP execution produced invalid capital/max-loss metrics.")

            new_target = live_exec
            new_target = min(new_target, floor_int(FIXED_RISK_PER_TRADE_RIAL / metrics.unit_loss))
            new_target = min(new_target, floor_int(available_cash / metrics.unit_capital))
            if max_entry_capital is not None:
                new_target = min(new_target, floor_int(max_entry_capital / metrics.unit_capital))
            if metrics.pretrade_cash > D0:
                new_target = min(new_target, floor_int(available_cash / metrics.pretrade_cash))
            if new_target <= 0:
                if floor_int(FIXED_RISK_PER_TRADE_RIAL / metrics.unit_loss) <= 0:
                    return ExecutionDecision(None, "RISK_BUDGET_EXCEEDED", "VWAP slippage pushes one unit above the fixed max-loss budget.")
                if max_entry_capital is not None and floor_int(max_entry_capital / metrics.unit_capital) <= 0:
                    return ExecutionDecision(
                        None, "STRATEGY_ALLOCATION_CAP_REACHED",
                        "VWAP execution cost cannot fit one unit inside the remaining per-strategy allocation."
                    )
                return ExecutionDecision(None, "INSUFFICIENT_CASH", "VWAP execution cost exceeds available paper-account cash.")
            plan = ExecutionPlan(new_target, tuple(prices), metrics)
            if new_target == target:
                break
            target = new_target

        if plan is None:
            return ExecutionDecision(None, "EXECUTION_PLAN_FAILED", "No stable execution plan could be produced.")

        final_prices: List[Decimal] = []
        for leg in c.legs:
            book = books.get(leg.ins_code)
            if book is None:
                return ExecutionDecision(None, "ORDER_BOOK_MISSING", f"Order book missing for {leg.symbol} at final quote.")
            px = book.quote_vwap(leg, plan.units)
            if px is None:
                return ExecutionDecision(None, "INSUFFICIENT_ORDER_BOOK_DEPTH", f"Final depth cannot fill {plan.units} unit(s) for {leg.symbol}.")
            final_prices.append(px)

        final_metrics = strategy_metrics(c.strategy, c.spot, c.legs, final_prices)
        final_max_loss = final_metrics.unit_loss * Decimal(plan.units)
        if final_max_loss > FIXED_RISK_PER_TRADE_RIAL + Decimal("0.01"):
            return ExecutionDecision(None, "RISK_BUDGET_EXCEEDED", "Final VWAP max loss exceeds the fixed 10M toman budget.")
        if final_max_loss + Decimal("0.01") < MIN_EXECUTION_RISK_RIAL:
            return ExecutionDecision(
                None,
                "MIN_EXECUTION_RISK_NOT_MET",
                f"Final VWAP max loss {money(final_max_loss/TOMAN_TO_RIAL):,.0f} toman is below the configured {MIN_EXECUTION_RISK_TOMAN:,.0f} toman minimum.",
            )

        history_exec, iv_exec, iv = self._expected_return_pair(c, final_metrics.unit_capital)
        if history_exec is None:
            return ExecutionDecision(None, "VWAP_HISTORY_UNAVAILABLE", "History expected return could not be evaluated at final VWAP.")
        if iv_exec is None or iv is None:
            return ExecutionDecision(None, "VWAP_IV_UNAVAILABLE", "IV expected return could not be evaluated at final VWAP.")
        c.details.update({
            "execution_required_return_to_expiry_pct": history_exec["required_return"] * 100.0,
            "execution_history_expected_return_to_expiry_pct": history_exec["expected_return"] * 100.0,
            "execution_iv_expected_return_to_expiry_pct": iv_exec["expected_return"] * 100.0,
            "execution_effective_iv_pct": iv * 100.0,
        })
        if not history_exec["passes"]:
            return ExecutionDecision(None, "VWAP_HISTORY_BELOW_40PCT", "History expected return falls below the configured hurdle at final five-level VWAP.")
        if EXPECTED_RETURN_REQUIRE_BOTH and not iv_exec["passes"]:
            return ExecutionDecision(None, "VWAP_IV_BELOW_40PCT", "IV expected return falls below the configured hurdle at final five-level VWAP.")
        if not EXPECTED_RETURN_REQUIRE_BOTH and not (history_exec["passes"] or iv_exec["passes"]):
            return ExecutionDecision(None, "VWAP_EXPECTED_RETURN_BELOW_40PCT", "Expected return falls below the configured hurdle at final five-level VWAP.")

        if max_entry_capital is not None and final_metrics.unit_capital * Decimal(plan.units) > max_entry_capital + Decimal("0.01"):
            return ExecutionDecision(
                None, "STRATEGY_ALLOCATION_CAP_REACHED",
                f"Final execution capital exceeds the remaining {MAX_STRATEGY_ALLOCATION_PCT}% strategy allocation."
            )
        if final_metrics.unit_capital * Decimal(plan.units) > available_cash + Decimal("0.01"):
            return ExecutionDecision(None, "INSUFFICIENT_CASH", "Final execution capital exceeds available paper-account cash.")
        if final_metrics.pretrade_cash * Decimal(plan.units) > available_cash + Decimal("0.01"):
            return ExecutionDecision(None, "INSUFFICIENT_CASH", "Covered-call gross stock purchase / pretrade cash exceeds available cash.")

        return ExecutionDecision(
            ExecutionPlan(plan.units, tuple(final_prices), final_metrics),
            "EXECUTABLE",
            f"Executable for {plan.units} unit(s) using five-level order-book VWAP.",
        )

    async def _open_position(
        self,
        db: DB,
        account: Account,
        c: Candidate,
        plan: ExecutionPlan,
        now: datetime,
    ) -> int:
        if c.signal_id is None:
            raise RuntimeError("Signal ID missing.")
        units = plan.units
        max_loss = plan.metrics.unit_loss * Decimal(units)
        max_profit = (
            plan.metrics.unit_profit * Decimal(units)
            if plan.metrics.unit_profit is not None else None
        )
        capital = plan.metrics.unit_capital * Decimal(units)
        entry_net = capital  # all five strategies are debit strategies in this engine
        pid = await db.insert_id(
            f"""
            INSERT INTO {qname('paper_positions')} (
                account_id,signal_table,signal_id,signal_key,position_signature,
                strategy_code,ua_ins_code,underlying_symbol,expiry_date,status,
                units,entry_risk_budget_rial,entry_unit_risk_rial,entry_max_loss_rial,
                entry_max_profit_rial,entry_capital_rial,entry_net_value_rial,
                current_position_value_rial,current_underlying_price_rial,opened_at,last_valued_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                account.account_id, c.table, c.signal_id, c.key, c.signature,
                c.strategy, c.ua_ins_code, c.underlying_symbol, c.expiry, units,
                money(FIXED_RISK_PER_TRADE_RIAL), money(plan.metrics.unit_loss), money(max_loss),
                money(max_profit) if max_profit is not None else None, money(capital), money(entry_net),
                money(entry_net), c.spot, now, now,
            ),
        )
        if pid <= 0:
            raise RuntimeError("Position insert did not return an ID.")

        leg_rows: List[Tuple[Any, ...]] = []
        for leg, px in zip(c.legs, plan.leg_prices):
            leg_rows.append((
                pid, leg.no, leg.kind, leg.ins_code, leg.symbol, leg.side, leg.option_type,
                leg.strike, leg.expiry, leg.contract_size, units,
                units * leg.contract_size, money(px), money(px),
            ))
        await db.executemany(
            f"""
            INSERT INTO {qname('paper_position_legs')} (
                position_id,leg_no,instrument_kind,ins_code,symbol,side,option_type,
                strike_price_rial,expiry_date,contract_size,contract_count,quantity_units,
                entry_price_rial,latest_price_rial
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            leg_rows,
        )
        await self._set_signal_execution_status(
            db, c, "EXECUTED", now,
            reason_code="POSITION_OPENED",
            reason=(
                f"Opened {units} unit(s) on {ACCOUNT_NAME} using order-book VWAP; "
                f"per-strategy allocation cap={MAX_STRATEGY_ALLOCATION_PCT}%."
            ),
            position_id=pid,
        )
        return pid

    async def _strategy_open_allocations(self, db: DB, account_id: int) -> Dict[str, Decimal]:
        rows = await db.fetch(
            f"""
            SELECT strategy_code, COALESCE(SUM(entry_capital_rial),0) AS allocated_rial
            FROM {qname('paper_positions')}
            WHERE account_id=%s AND status='OPEN'
            GROUP BY strategy_code
            """,
            (account_id,),
        )
        return {
            str(row["strategy_code"]): dec(row.get("allocated_rial"))
            for row in rows
        }

    async def _auto_open(
        self,
        db: DB,
        account: Account,
        candidates: Dict[str, List[Candidate]],
        books: Mapping[str, OrderBook],
        now: datetime,
        enabled: bool = True,
    ) -> List[int]:
        all_candidates = [c for items in candidates.values() for c in items]

        if not AUTO_TRADE or not enabled:
            for c in all_candidates:
                if c.opened_position_id is not None:
                    await self._set_signal_execution_status(
                        db, c, "EXECUTED", now, "POSITION_OPENED",
                        "Signal already has a linked paper position.", c.opened_position_id,
                    )
                else:
                    await self._set_signal_execution_status(
                        db, c, "NOT_EXECUTED", now, "AUTO_TRADE_DISABLED",
                        "Automatic paper-account execution is disabled for this scan.",
                    )
            return []

        open_signatures = await self._open_signatures(db, account.account_id)
        eligible: List[Candidate] = []
        for c in all_candidates:
            if c.opened_position_id is not None:
                await self._set_signal_execution_status(
                    db, c, "EXECUTED", now, "POSITION_OPENED",
                    "Signal already has a linked paper position.", c.opened_position_id,
                )
                continue
            if c.final_signal not in {"CANDIDATE", "STRONG_CANDIDATE"}:
                code, reason = self._non_entry_reason(c)
                await self._set_signal_execution_status(db, c, "NOT_EXECUTED", now, code, reason)
                continue
            eligible.append(c)

        eligible.sort(key=lambda c: c.priority_tuple, reverse=True)
        execution_books = self._execution_books(books)
        available_cash = account.available_cash
        strategy_allocated = await self._strategy_open_allocations(db, account.account_id)
        strategy_cap = account.initial_equity * MAX_STRATEGY_ALLOCATION_PCT / D100
        opened: List[int] = []

        for c in eligible:
            # Safety against repeated entry on exactly the same still-open structure.
            # Exact open signatures are de-duplicated and each strategy has a hard
            # allocation cap as a percentage of initial paper-account equity.
            if c.signature in open_signatures:
                await self._set_signal_execution_status(
                    db, c, "NOT_EXECUTED", now, "DUPLICATE_OPEN_SIGNATURE",
                    "The exact same strategy/legs combination already has an OPEN paper position.",
                )
                continue

            current_strategy_allocation = strategy_allocated.get(c.strategy, D0)
            remaining_strategy_allocation = max(D0, strategy_cap - current_strategy_allocation)
            decision = self._plan_execution(
                c,
                execution_books,
                available_cash,
                max_entry_capital=remaining_strategy_allocation,
            )
            if decision.plan is None or decision.plan.units <= 0:
                await self._set_signal_execution_status(
                    db, c, "NOT_EXECUTED", now, decision.reason_code, decision.reason
                )
                continue
            plan = decision.plan

            can_fill = True
            for leg in c.legs:
                book = execution_books.get(leg.ins_code)
                if book is None or book.capacity_units(leg) < plan.units:
                    can_fill = False
                    break
            if not can_fill:
                await self._set_signal_execution_status(
                    db, c, "NOT_EXECUTED", now, "INSUFFICIENT_ORDER_BOOK_DEPTH",
                    "Displayed five-level depth was consumed by higher-priority signals in the same scan.",
                )
                continue

            pid = await self._open_position(db, account, c, plan, now)
            for leg in c.legs:
                ok = execution_books[leg.ins_code].consume(leg, plan.units)
                if not ok:
                    raise RuntimeError("In-memory order-book consumption mismatch.")

            opened_capital = plan.metrics.unit_capital * Decimal(plan.units)
            available_cash -= opened_capital
            strategy_allocated[c.strategy] = strategy_allocated.get(c.strategy, D0) + opened_capital
            open_signatures.add(c.signature)
            c.opened_position_id = pid
            opened.append(pid)
            rr_text = f"{plan.metrics.reward_risk}" if plan.metrics.reward_risk is not None else "n/a"
            print(
                f"[{now:%H:%M:%S}] 🧪 Paper OPEN #{pid} | {c.strategy} | {c.underlying_symbol} "
                f"| units={plan.units} | max_loss={money(plan.metrics.unit_loss*plan.units/TOMAN_TO_RIAL):,.0f} toman "
                f"| capital={money(plan.metrics.unit_capital*plan.units/TOMAN_TO_RIAL):,.0f} toman | RR={rr_text}"
            )

        return opened

    async def run_cycle(self, auto_trade: bool = True, revision: Optional[str] = None) -> Dict[str, int]:
        if not self.pool:
            raise RuntimeError("Engine not open.")
        revision = revision or await self.source_revision()
        if not revision:
            raise RuntimeError("No collector source revision is available yet.")
        run_id = await self._start_run(revision)
        now = tehran_now().replace(microsecond=0)
        quotes_count = signals_count = opened_count = closed_count = 0
        try:
            async with self.pool.acquire() as raw:
                db = DB(raw)
                async with db.transaction():
                    account = await self._load_account(db)
                    quotes, qmap, umap, books = await self._load_market_snapshot(db)
                    quotes_count = len(quotes)

                    valued, closed = await self._value_positions(
                        db, account.account_id, qmap, umap, books, now
                    )
                    closed_count = closed
                    account = await self._update_account(db, account, now, history=False)
                    hist_vol = await self._load_historical_volatility(db)
                    await self._load_expected_return_history(db, now)
                    self._er_iv_multiplier_cache.clear()
                    candidates = self._build_candidates(
                        account, quotes, books, hist_vol, revision, now
                    )
                    self._apply_expected_return_hurdle(candidates)
                    signals_count = await self._upsert_signals(db, account, candidates)
                    opened = await self._auto_open(
                        db, account, candidates, books, now, enabled=auto_trade
                    )
                    opened_count = len(opened)
                    if opened:
                        await self._value_positions(
                            db, account.account_id, qmap, umap, books,
                            now + timedelta(seconds=1),
                        )
                    account = await self._update_account(
                        db, account, now + timedelta(seconds=1 if opened else 0), history=True
                    )

            counts = {
                "quotes": quotes_count,
                "covered_call": len(candidates["COVERED_CALL"]),
                "protective_put": len(candidates["PROTECTIVE_PUT"]),
                "bull_call_spread": len(candidates["BULL_CALL_SPREAD"]),
                "bear_put_spread": len(candidates["BEAR_PUT_SPREAD"]),
                "long_straddle": len(candidates["LONG_STRADDLE"]),
                "signals": signals_count,
                "valued": valued,
                "opened": opened_count,
                "closed": closed_count,
            }
            actionable_count = sum(
                1 for items in candidates.values() for c in items
                if c.final_signal in {"CANDIDATE", "STRONG_CANDIDATE"}
            )
            print(
                f"[{tehran_now():%H:%M:%S}] ✅ Scan | quotes={quotes_count} | structures={signals_count} "
                f"| actionable_40pct_both={actionable_count} "
                f"| CC={counts['covered_call']} | PP={counts['protective_put']} "
                f"| BCS={counts['bull_call_spread']} | BPS={counts['bear_put_spread']} "
                f"| STR={counts['long_straddle']}"
            )
            print(
                f"   Equity={money(account.equity/TOMAN_TO_RIAL):,.0f} toman | "
                f"cash_available={money(account.available_cash/TOMAN_TO_RIAL):,.0f} | "
                f"allocated={money(account.allocated_capital/TOMAN_TO_RIAL):,.0f} | "
                f"reserved_max_loss={money(account.reserved_risk/TOMAN_TO_RIAL):,.0f} | "
                f"open={account.open_count} | new={opened_count} | expired/closed={closed_count}"
            )
            await self._finish_run(
                run_id, "SUCCESS", quotes_count, signals_count, opened_count, closed_count
            )
            return counts
        except Exception as exc:
            await self._finish_run(
                run_id, "FAILED", quotes_count, signals_count, opened_count, closed_count,
                str(exc)[:2000],
            )
            raise

    async def close_positions(self, ids: Sequence[int]) -> None:
        if not self.pool:
            raise RuntimeError("Engine not open.")
        now = tehran_now().replace(microsecond=0)
        async with self.pool.acquire() as raw:
            db = DB(raw)
            async with db.transaction():
                account = await self._load_account(db)
                _, qmap, umap, books = await self._load_market_snapshot(db)
                valued, closed = await self._value_positions(
                    db, account.account_id, qmap, umap, books, now,
                    {int(x) for x in ids},
                )
                account = await self._update_account(db, account, now, history=True)
        print(
            f"✅ Manual override close | valued={valued} | closed={closed} | "
            f"equity={money(account.equity/TOMAN_TO_RIAL):,.0f} toman"
        )

    async def report(self) -> None:
        if not self.pool:
            raise RuntimeError("Engine not open.")
        async with self.pool.acquire() as raw:
            db = DB(raw)
            account = await self._load_account(db)
            rows = await db.fetch(
                f"""
                SELECT position_id,strategy_code,underlying_symbol,status,units,
                       entry_max_loss_rial,entry_capital_rial,unrealized_pnl_rial,
                       realized_pnl_rial,return_on_risk_pct,opened_at,expiry_date
                FROM {qname('paper_positions')}
                WHERE account_id=%s
                ORDER BY CASE WHEN status='OPEN' THEN 0 ELSE 1 END,opened_at DESC
                LIMIT 50
                """,
                (account.account_id,),
            )
        print("=" * 112)
        print("REYT PAPER ACCOUNT REPORT")
        print("=" * 112)
        print(f"Initial equity     : {money(account.initial_equity/TOMAN_TO_RIAL):,.0f} toman")
        print(f"Current equity     : {money(account.equity/TOMAN_TO_RIAL):,.0f} toman")
        print(f"Available cash     : {money(account.available_cash/TOMAN_TO_RIAL):,.0f} toman")
        print(f"Allocated capital  : {money(account.allocated_capital/TOMAN_TO_RIAL):,.0f} toman")
        print(f"Reserved max loss  : {money(account.reserved_risk/TOMAN_TO_RIAL):,.0f} toman (informational; no aggregate cap)")
        print(f"Risk / entry       : min {MIN_EXECUTION_RISK_TOMAN:,.0f} | max {FIXED_RISK_PER_TRADE_TOMAN:,.0f} toman (final VWAP Max Loss)")
        print(
            f"Strategy alloc cap : {MAX_STRATEGY_ALLOCATION_PCT}% of initial equity "
            f"({money(account.initial_equity * MAX_STRATEGY_ALLOCATION_PCT / D100 / TOMAN_TO_RIAL):,.0f} toman each)"
        )
        print(f"Open positions     : {account.open_count}")
        print(f"Realized P/L       : {money(account.realized/TOMAN_TO_RIAL):,.0f} toman")
        print(f"Unrealized P/L     : {money(account.unrealized/TOMAN_TO_RIAL):,.0f} toman")
        print("-" * 112)
        if not rows:
            print("No paper positions yet.")
        for r in rows:
            pnl = dec(r["unrealized_pnl_rial"]) if r["status"] == "OPEN" else dec(r["realized_pnl_rial"])
            print(
                f"#{int(r['position_id']):<6} {str(r['strategy_code']):<22} "
                f"{str(r['underlying_symbol'] or ''):<16} {str(r['status']):<8} "
                f"units={int(r['units']):<5} "
                f"risk={money(dec(r['entry_max_loss_rial'])/TOMAN_TO_RIAL):>13,.0f} "
                f"capital={money(dec(r['entry_capital_rial'])/TOMAN_TO_RIAL):>13,.0f} "
                f"P/L={money(pnl/TOMAN_TO_RIAL):>13,.0f} toman "
                f"ROR={pct(r['return_on_risk_pct']):>8}%"
            )
        print("=" * 112)


# =============================================================================
# CLI / watcher
# =============================================================================


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ReyT unified 1B-toman paper strategy engine")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    mode.add_argument("--report", action="store_true")
    p.add_argument(
        "--interval", type=float, default=1.0,
        help="Poll collector_state.live_source_revision; scan only when revision changes.",
    )
    p.add_argument("--no-auto-trade", action="store_true")
    p.add_argument("--close-position", type=int, action="append", default=[], metavar="ID")
    return p


async def watch(args: argparse.Namespace) -> None:
    engine = PaperEngine()
    poll = max(0.5, float(args.interval))
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not engine._shutdown:
            print(f"\n[{tehran_now():%H:%M:%S}] 🛑 Shutdown requested.")
        engine._shutdown = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    # Always initialize/verify the paper account at service startup, even when
    # the market is closed. This guarantees that Bale 08:30/13:00 reports have
    # a valid account immediately after a trading-state reset. No scan/trade is
    # performed here; the connection is closed again before the market-idle loop.
    await engine.open()
    await engine.close()
    print(f"[{tehran_now():%H:%M:%S}] ✅ Paper account startup initialization complete.")

    while not engine._shutdown:
        if not is_market_open():
            nxt = next_market_open()
            print(
                f"[{tehran_now():%Y-%m-%d %H:%M:%S}] ⏸ Market closed; "
                f"fully idle until {nxt:%Y-%m-%d %H:%M} Tehran."
            )
            while not engine._shutdown and not is_market_open():
                await asyncio.sleep(30)
            if engine._shutdown:
                break

        last_revision: Optional[str] = None
        await engine.open()
        try:
            print(
                f"[{tehran_now():%H:%M:%S}] 👀 Watching atomic collector revisions "
                f"(poll={poll:g}s)."
            )
            while not engine._shutdown and is_market_open():
                try:
                    revision = await engine.source_revision()
                    if revision and revision != last_revision:
                        print(f"[{tehran_now():%H:%M:%S}] 🔄 Atomic collector revision: {revision}")
                        await engine.run_cycle(
                            auto_trade=not args.no_auto_trade,
                            revision=revision,
                        )
                        last_revision = revision
                except Exception as exc:
                    print(f"[{tehran_now():%H:%M:%S}] ❌ Strategy cycle error: {exc}")
                await asyncio.sleep(poll)
        finally:
            await engine.close()
            print(f"[{tehran_now():%H:%M:%S}] ⏸ Strategy DB connection closed.")


async def async_main(args: argparse.Namespace) -> None:
    validate_configuration()
    engine = PaperEngine()

    if args.watch:
        # Signal handlers mutate this specific engine instance only in non-watch
        # modes, so watch installs its own instance internally.
        await watch(args)
        return

    await engine.open()
    try:
        if args.close_position:
            await engine.close_positions(args.close_position)
            await engine.report()
            return
        if args.report:
            await engine.report()
            return
        if not is_market_open():
            print(f"⏸ Market is closed. Next opening: {next_market_open():%Y-%m-%d %H:%M} Tehran.")
            return
        revision = await engine.source_revision()
        if not revision:
            print("⏳ No collector revision yet; nothing to scan.")
            return
        await engine.run_cycle(auto_trade=not args.no_auto_trade, revision=revision)
        await engine.report()
    finally:
        await engine.close()


def main() -> None:
    args = build_parser().parse_args()
    if not any((args.once, args.watch, args.report, args.close_position)):
        args.once = True
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass
    except (RuntimeError, ValueError, pymysql.MySQLError, OSError) as exc:
        print(f"❌ Error: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
