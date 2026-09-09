# -*- coding: utf-8 -*-
"""ReyT unified TSETMC options collector for MySQL/MariaDB on VPS.

Design basis
------------
This collector is a clean rewrite of the data pipeline in ``1.ipynb`` and is
matched to ``00_create_all_ReyT_mysql_tables_REBUILT.sql``.

Collector-owned tables populated by this program:
    underlying_assets
    option_contracts
    market_data_ticks
    order_book_depth
    daily_market_data
    option_greeks
    collector_state
    history_sync_state

The five strategy signal tables and the paper-trading tables are intentionally
NOT written here. They belong to the strategy/paper engine and consume the
atomic live snapshot produced by this collector.

Important properties
--------------------
* No CREATE/ALTER/DROP statements. The SQL schema must already exist.
* Fully idle outside configured Tehran market hours.
* One shared aiohttp session and one MySQL pool per market session.
* API-1 normalization + vectorized Greeks are computed in memory.
* Underlyings, contracts, ticks and Greeks commit atomically in one transaction.
* Latest-only tables are UPSERTed; daily history is retained separately.
* Historical downloads are resumable using history_sync_state.
* Order-book collection is round-robin and concurrent to avoid hammering TSETMC.

Typical usage
-------------
    python ReyT_collector_unified_optimized.py --once
    python ReyT_collector_unified_optimized.py --watch
    python ReyT_collector_unified_optimized.py --watch --skip-history

Database credentials are read from settings.ini (next to this file by default)
or environment variables. Example keys: MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE,
MYSQL_USER, MYSQL_PASSWORD. Set OPTIONS_CONFIG_FILE to use another INI file.
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
import math
import os
import re
import signal
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp
import aiomysql
import numpy as np
from scipy.special import ndtr

try:
    import jdatetime  # optional: only used for begin/end Jalali labels
except ImportError:  # pragma: no cover - optional dependency
    jdatetime = None


# ============================================================================
# Configuration
# ============================================================================

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
    for section in (
        "collector",
        "greeks",
        "mysql",
        "mariadb",
        "database",
        "sql",
        "paper",
    ):
        if _CONFIG.has_option(section, key):
            return _CONFIG.get(section, key).strip()
    return default


def _bool_setting(name: str, default: bool) -> bool:
    return _setting(name, "yes" if default else "no").lower() in {
        "1", "true", "yes", "y", "on"
    }


def _int_setting(name: str, default: int, minimum: Optional[int] = None) -> int:
    value = int(_setting(name, str(default)))
    return max(minimum, value) if minimum is not None else value


def _float_setting(name: str, default: float, minimum: Optional[float] = None) -> float:
    value = float(_setting(name, str(default)))
    return max(minimum, value) if minimum is not None else value


def _parse_clock(raw: str, default: time) -> time:
    text = (raw or "").strip()
    if not text:
        return default
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            pass
    raise ValueError(f"Invalid clock value {raw!r}; expected HH:MM or HH:MM:SS")


MYSQL_HOST = _setting("MYSQL_HOST", _setting("SQL_SERVER", "127.0.0.1"))
MYSQL_PORT = _int_setting("MYSQL_PORT", 3306)
MYSQL_DATABASE = _setting("MYSQL_DATABASE", _setting("SQL_DATABASE", "ghazali1_ReyTOption"))
MYSQL_USER = _setting("MYSQL_USER", _setting("SQL_USERNAME", "reyt_app"))
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", _setting("SQL_PASSWORD"))
MYSQL_CHARSET = _setting("MYSQL_CHARSET", "utf8mb4")
MYSQL_CONNECT_TIMEOUT = _int_setting("MYSQL_CONNECT_TIMEOUT", 20, 1)
MYSQL_POOL_MIN = _int_setting("COLLECTOR_DB_POOL_MIN", 1, 1)
MYSQL_POOL_MAX = _int_setting("COLLECTOR_DB_POOL_MAX", 8, MYSQL_POOL_MIN)
MYSQL_TIME_ZONE = _setting("MYSQL_TIME_ZONE", "+03:30")

BASE_URL = _setting("TSETMC_API_BASE_URL", "https://cdn.tsetmc.com/api").rstrip("/")
HTTP_TIMEOUT = _float_setting("COLLECTOR_HTTP_TIMEOUT", 20.0, 1.0)
HTTP_RETRIES = _int_setting("COLLECTOR_HTTP_RETRIES", 3, 1)
HTTP_CONNECTOR_LIMIT = _int_setting("COLLECTOR_HTTP_CONNECTOR_LIMIT", 60, 1)

REALTIME_INTERVAL = _float_setting("COLLECTOR_REALTIME_INTERVAL", 5.0, 1.0)
SNAPSHOT_INTERVAL = _float_setting("COLLECTOR_UNDERLYING_INTERVAL", 60.0, 5.0)
ORDER_BOOK_INTERVAL = _float_setting("COLLECTOR_ORDER_BOOK_INTERVAL", 15.0, 1.0)
ORDER_BOOK_BATCH_SIZE = _int_setting("COLLECTOR_ORDER_BOOK_BATCH_SIZE", 300, 1)
ORDER_BOOK_CONCURRENCY = _int_setting("COLLECTOR_ORDER_BOOK_CONCURRENCY", 30, 1)
ORDER_BOOK_INCLUDE_UNDERLYINGS = _bool_setting("COLLECTOR_ORDER_BOOK_INCLUDE_UNDERLYINGS", True)
SNAPSHOT_CONCURRENCY = _int_setting("COLLECTOR_UNDERLYING_CONCURRENCY", 8, 1)

INITIAL_HISTORY_DAYS = _int_setting("COLLECTOR_INITIAL_HISTORY_DAYS", 450, 1)
EOD_HISTORY_LOOKBACK_DAYS = _int_setting("COLLECTOR_EOD_HISTORY_LOOKBACK_DAYS", 7, 1)
EOD_HISTORY_TIME = _parse_clock(_setting("COLLECTOR_EOD_HISTORY_TIME", "13:00"), time(13, 0))
EOD_HISTORY_RETRY_SECONDS = _int_setting("COLLECTOR_EOD_HISTORY_RETRY_SECONDS", 600, 30)
EOD_HISTORY_MAX_RETRIES = _int_setting("COLLECTOR_EOD_HISTORY_MAX_RETRIES", 36, 1)
HISTORY_CONCURRENCY = _int_setting("COLLECTOR_HISTORY_CONCURRENCY", 4, 1)
HISTORY_REQUEST_DELAY = _float_setting("COLLECTOR_HISTORY_REQUEST_DELAY", 0.20, 0.0)
DB_DEADLOCK_RETRIES = _int_setting("COLLECTOR_DB_DEADLOCK_RETRIES", 5, 1)
DB_DEADLOCK_BASE_DELAY = _float_setting("COLLECTOR_DB_DEADLOCK_BASE_DELAY", 0.50, 0.05)

CALCULATE_GREEKS = _bool_setting("COLLECTOR_CALCULATE_GREEKS", True)
RISK_FREE_RATE = _float_setting("GREEKS_RISK_FREE_RATE", 0.40)
DIVIDEND_YIELD = _float_setting("GREEKS_DIVIDEND_YIELD", 0.0)
MAX_IV = _float_setting("GREEKS_MAX_IV", 5.0, 0.01)
MIN_IV = _float_setting("GREEKS_MIN_IV", 1e-6, 1e-12)
MAX_NEWTON_ITER = _int_setting("GREEKS_MAX_NEWTON_ITER", 30, 1)
NEWTON_TOL = _float_setting("GREEKS_NEWTON_TOL", 1e-7, 1e-12)
BISECTION_ITER = _int_setting("GREEKS_BISECTION_ITER", 60, 1)
SPREAD_THRESHOLD = _float_setting("GREEKS_SPREAD_THRESHOLD", 0.10, 0.0)

MARKET_OPEN = _parse_clock(_setting("MARKET_OPEN", "09:00"), time(9, 0))
MARKET_CLOSE = _parse_clock(_setting("MARKET_CLOSE", "12:30"), time(12, 30))
# Python weekday: Monday=0 ... Sunday=6. Iran market: Sat-Wed.
MARKET_WEEKDAYS = {0, 1, 2, 5, 6}
TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tsetmc.com/",
}

COLLECTOR_REQUIRED_TABLES = (
    "underlying_assets",
    "option_contracts",
    "market_data_ticks",
    "order_book_depth",
    "daily_market_data",
    "option_greeks",
    "collector_state",
    "history_sync_state",
)


def tehran_now() -> datetime:
    """Naive Tehran datetime, suitable for MySQL DATETIME columns."""
    return datetime.now(TEHRAN_TZ).replace(tzinfo=None)


def _parse_holidays(raw: str) -> set[date]:
    result: set[date] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            result.add(datetime.strptime(item, "%Y-%m-%d").date())
    return result


MARKET_HOLIDAYS = _parse_holidays(_setting("MARKET_HOLIDAYS", ""))


def validate_configuration() -> None:
    if not MYSQL_HOST:
        raise RuntimeError("MYSQL_HOST is empty.")
    if not MYSQL_DATABASE:
        raise RuntimeError("MYSQL_DATABASE is empty.")
    if not MYSQL_USER:
        raise RuntimeError("MYSQL_USER is empty.")
    if not MYSQL_PASSWORD:
        raise RuntimeError(
            "MYSQL_PASSWORD is empty. Put it in settings.ini or set MYSQL_PASSWORD."
        )
    if not (1 <= MYSQL_PORT <= 65535):
        raise RuntimeError("MYSQL_PORT must be between 1 and 65535.")
    if not re.fullmatch(r"[A-Za-z0-9_]+", MYSQL_DATABASE):
        raise RuntimeError("MYSQL_DATABASE may contain only letters, digits and _." )
    if MARKET_OPEN >= MARKET_CLOSE:
        raise RuntimeError("MARKET_OPEN must be earlier than MARKET_CLOSE.")
    if MIN_IV >= MAX_IV:
        raise RuntimeError("GREEKS_MIN_IV must be smaller than GREEKS_MAX_IV.")


# ============================================================================
# Data helpers
# ============================================================================


def to_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    number = to_decimal(value)
    return int(number) if number is not None else None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_api_date(value: Any) -> Optional[date]:
    if value in (None, "", 0, "0"):
        return None
    text = str(value).strip()
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text[:10])
    except (TypeError, ValueError):
        return None


def to_jalali_label(value: Optional[date]) -> Optional[str]:
    if value is None or jdatetime is None:
        return None
    try:
        return jdatetime.date.fromgregorian(date=value).strftime("%Y-%m-%d")
    except Exception:
        return None


def event_datetime(d_even: Any, h_even: Any, fallback: Optional[datetime] = None) -> datetime:
    fallback = fallback or tehran_now()
    parsed_date = parse_api_date(d_even)
    if parsed_date is None:
        return fallback
    try:
        text = str(to_int(h_even) or 0).zfill(6)
        return datetime.combine(parsed_date, datetime.strptime(text, "%H%M%S").time())
    except (TypeError, ValueError):
        return datetime.combine(parsed_date, time.min)


def finite_or_none(value: Any) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def np_float_array(values: Iterable[Any]) -> np.ndarray:
    out: List[float] = []
    for value in values:
        x = finite_or_none(value)
        out.append(np.nan if x is None else x)
    return np.asarray(out, dtype=np.float64)


@dataclass(frozen=True)
class NormalizedOption:
    ins_code: str
    option_type: str
    symbol: Optional[str]
    full_symbol: Optional[str]
    ua_ins_code: str
    ua_symbol: Optional[str]
    ua_close: Optional[Decimal]
    ua_last: Optional[Decimal]
    ua_prev_close: Optional[Decimal]
    contract_size: Optional[int]
    strike: Optional[Decimal]
    begin_date: Optional[date]
    end_date: Optional[date]
    days_to_expiry: Optional[int]
    last: Optional[Decimal]
    close: Optional[Decimal]
    prev_close: Optional[Decimal]
    open_interest: Optional[int]
    prev_open_interest: Optional[int]
    trades_count: Optional[int]
    volume: Optional[int]
    trade_value: Optional[Decimal]
    notional_value: Optional[Decimal]
    bid: Optional[Decimal]
    bid_volume: Optional[int]
    ask: Optional[Decimal]
    ask_volume: Optional[int]


# ============================================================================
# Vectorized Black-Scholes / IV
# ============================================================================


def _bs_price(
    s: np.ndarray,
    k: np.ndarray,
    t: np.ndarray,
    sigma: np.ndarray,
    is_call: np.ndarray,
) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        sqrt_t = np.sqrt(t)
        d1 = (
            np.log(s / k)
            + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * sigma * sigma) * t
        ) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t
        call = (
            s * np.exp(-DIVIDEND_YIELD * t) * ndtr(d1)
            - k * np.exp(-RISK_FREE_RATE * t) * ndtr(d2)
        )
        put = (
            k * np.exp(-RISK_FREE_RATE * t) * ndtr(-d2)
            - s * np.exp(-DIVIDEND_YIELD * t) * ndtr(-d1)
        )
    return np.where(is_call, call, put)


def _bs_vega(
    s: np.ndarray,
    k: np.ndarray,
    t: np.ndarray,
    sigma: np.ndarray,
) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        sqrt_t = np.sqrt(t)
        d1 = (
            np.log(s / k)
            + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * sigma * sigma) * t
        ) / (sigma * sqrt_t)
        pdf = np.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
        return s * np.exp(-DIVIDEND_YIELD * t) * pdf * sqrt_t


def implied_vol_hybrid(
    s: np.ndarray,
    k: np.ndarray,
    t: np.ndarray,
    price: np.ndarray,
    is_call: np.ndarray,
    intrinsic: np.ndarray,
    structurally_valid: np.ndarray,
) -> np.ndarray:
    """Vectorized Newton solver with vectorized bisection fallback."""
    n = len(s)
    iv = np.full(n, np.nan, dtype=np.float64)

    with np.errstate(over="ignore", invalid="ignore"):
        call_upper = s * np.exp(-DIVIDEND_YIELD * t)
        put_upper = k * np.exp(-RISK_FREE_RATE * t)
        upper = np.where(is_call, call_upper, put_upper)

    valid_iv = (
        structurally_valid
        & np.isfinite(price)
        & (price >= intrinsic * 0.98)
        & (price <= upper * 1.02)
    )
    if not np.any(valid_iv):
        return iv

    sigma = np.full(n, np.nan, dtype=np.float64)
    sigma[valid_iv] = 0.30

    for _ in range(MAX_NEWTON_ITER):
        model = _bs_price(s, k, t, sigma, is_call)
        vega = np.maximum(_bs_vega(s, k, t, sigma), 1e-12)
        diff = model - price
        proposal = np.clip(sigma - diff / vega, MIN_IV, MAX_IV)
        delta = np.abs(proposal - sigma)
        sigma = np.where(valid_iv, proposal, np.nan)
        if np.all((delta[valid_iv] < NEWTON_TOL) | ~np.isfinite(delta[valid_iv])):
            break

    final_price = _bs_price(s, k, t, sigma, is_call)
    price_tolerance = np.maximum(0.5, np.abs(price) * 1e-6)
    newton_ok = valid_iv & np.isfinite(sigma) & (np.abs(final_price - price) <= price_tolerance)
    iv[newton_ok] = sigma[newton_ok]

    failed = valid_iv & ~newton_ok
    idx = np.flatnonzero(failed)
    if idx.size == 0:
        return np.where((iv >= MIN_IV) & (iv <= MAX_IV), iv, np.nan)

    sf, kf, tf = s[idx], k[idx], t[idx]
    pf, cf = price[idx], is_call[idx]
    low = np.full(idx.size, MIN_IV, dtype=np.float64)
    high = np.full(idx.size, MAX_IV, dtype=np.float64)
    p_low = _bs_price(sf, kf, tf, low, cf)
    p_high = _bs_price(sf, kf, tf, high, cf)
    bracket = np.isfinite(p_low) & np.isfinite(p_high) & (pf >= p_low) & (pf <= p_high)

    for _ in range(BISECTION_ITER):
        mid = 0.5 * (low + high)
        p_mid = _bs_price(sf, kf, tf, mid, cf)
        too_high = p_mid > pf
        high = np.where(bracket & too_high, mid, high)
        low = np.where(bracket & ~too_high, mid, low)

    solved = 0.5 * (low + high)
    solved = np.where(bracket, solved, np.nan)
    iv[idx] = solved
    return np.where((iv >= MIN_IV) & (iv <= MAX_IV), iv, np.nan)


def build_greeks_rows(
    options: Sequence[NormalizedOption],
    fetch_time: datetime,
) -> List[Tuple[Any, ...]]:
    eligible = [
        x for x in options
        if x.ins_code
        and x.ua_ins_code
        and x.end_date is not None
        and x.strike is not None
        and x.strike > 0
        and x.contract_size is not None
        and x.contract_size > 0
    ]
    if not eligible:
        return []

    today = fetch_time.date()
    s = np_float_array(
        (x.ua_last if x.ua_last is not None and x.ua_last > 0 else x.ua_close)
        for x in eligible
    )
    k = np_float_array(x.strike for x in eligible)
    dte = np.asarray(
        [max(0, (x.end_date - today).days) for x in eligible], dtype=np.int32
    )
    t = dte.astype(np.float64) / 365.0
    is_call = np.asarray([x.option_type == "CALL" for x in eligible], dtype=bool)
    bid = np_float_array(x.bid for x in eligible)
    ask = np_float_array(x.ask for x in eligible)
    last = np_float_array(x.last for x in eligible)
    close = np_float_array(x.close for x in eligible)

    with np.errstate(divide="ignore", invalid="ignore"):
        spread = ask - bid
        spread_ratio = np.where(bid > 0, spread / bid, np.nan)
        midpoint = (bid + ask) / 2.0
        use_mid = (bid > 0) & (ask > 0) & (spread >= 0) & (spread_ratio < SPREAD_THRESHOLD)
        option_price = np.where(use_mid, midpoint, last)
        option_price = np.where(
            np.isfinite(option_price) & (option_price > 0), option_price, close
        )
        # Last fallback: if last/close are missing but a two-sided market exists,
        # using mid is still more useful than dropping the row entirely.
        option_price = np.where(
            np.isfinite(option_price) & (option_price > 0), option_price,
            np.where((bid > 0) & (ask > 0), midpoint, np.nan),
        )

        intrinsic = np.where(is_call, np.maximum(s - k, 0.0), np.maximum(k - s, 0.0))
        time_value = option_price - intrinsic
        moneyness = s / k
        distance_to_strike = (s - k) / k

    valid = (
        np.isfinite(s) & (s > 0)
        & np.isfinite(k) & (k > 0)
        & (t > 0)
        & np.isfinite(option_price) & (option_price > 0)
    )

    iv = implied_vol_hybrid(s, k, t, option_price, is_call, intrinsic, valid)

    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        sqrt_t = np.sqrt(t)
        d1 = (
            np.log(s / k)
            + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * iv * iv) * t
        ) / (iv * sqrt_t)
        d2 = d1 - iv * sqrt_t
        pdf_d1 = np.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)

        delta = np.where(
            is_call,
            np.exp(-DIVIDEND_YIELD * t) * ndtr(d1),
            -np.exp(-DIVIDEND_YIELD * t) * ndtr(-d1),
        )
        gamma = np.exp(-DIVIDEND_YIELD * t) * pdf_d1 / (s * iv * sqrt_t)
        vega = s * np.exp(-DIVIDEND_YIELD * t) * pdf_d1 * sqrt_t / 100.0

        theta_call = (
            -s * np.exp(-DIVIDEND_YIELD * t) * pdf_d1 * iv / (2.0 * sqrt_t)
            - RISK_FREE_RATE * k * np.exp(-RISK_FREE_RATE * t) * ndtr(d2)
            + DIVIDEND_YIELD * s * np.exp(-DIVIDEND_YIELD * t) * ndtr(d1)
        ) / 365.0
        theta_put = (
            -s * np.exp(-DIVIDEND_YIELD * t) * pdf_d1 * iv / (2.0 * sqrt_t)
            + RISK_FREE_RATE * k * np.exp(-RISK_FREE_RATE * t) * ndtr(-d2)
            - DIVIDEND_YIELD * s * np.exp(-DIVIDEND_YIELD * t) * ndtr(-d1)
        ) / 365.0
        theta = np.where(is_call, theta_call, theta_put)
        rho = np.where(
            is_call,
            k * t * np.exp(-RISK_FREE_RATE * t) * ndtr(d2) / 100.0,
            -k * t * np.exp(-RISK_FREE_RATE * t) * ndtr(-d2) / 100.0,
        )
        mask = valid & np.isfinite(iv)
        delta = np.where(mask, delta, np.nan)
        gamma = np.where(mask, gamma, np.nan)
        theta = np.where(mask, theta, np.nan)
        vega = np.where(mask, vega, np.nan)
        rho = np.where(mask, rho, np.nan)
        leverage = np.where((option_price > 0) & mask, delta * s / option_price, np.nan)
        elasticity = leverage.copy()
        break_even = np.where(is_call, k + option_price, k - option_price)

    rows: List[Tuple[Any, ...]] = []
    for i, x in enumerate(eligible):
        underlying = finite_or_none(s[i])
        strike = finite_or_none(k[i])
        if underlying is None or underlying <= 0 or strike is None or strike <= 0:
            # Mandatory option_greeks fields cannot be populated safely.
            continue
        rows.append((
            x.ins_code,
            fetch_time,
            x.ua_ins_code,
            x.option_type,
            x.symbol,
            x.ua_symbol,
            x.end_date,
            strike,
            int(dte[i]),
            underlying,
            finite_or_none(option_price[i]),
            finite_or_none(bid[i]),
            finite_or_none(ask[i]),
            finite_or_none(spread[i]),
            finite_or_none(spread_ratio[i]),
            finite_or_none(intrinsic[i]),
            finite_or_none(time_value[i]),
            finite_or_none(moneyness[i]),
            finite_or_none(distance_to_strike[i]),
            finite_or_none(iv[i]),
            finite_or_none(delta[i]),
            finite_or_none(gamma[i]),
            finite_or_none(theta[i]),
            finite_or_none(vega[i]),
            finite_or_none(rho[i]),
            finite_or_none(leverage[i]),
            finite_or_none(elasticity[i]),
            finite_or_none(break_even[i]),
            x.open_interest,
        ))
    return rows


# ============================================================================
# Collector
# ============================================================================


class ReyTCollector:
    def __init__(self) -> None:
        self.pool: Optional[aiomysql.Pool] = None
        self._shutdown = False
        self._shutdown_event = asyncio.Event()
        self._order_book_cursor = 0
        self._order_book_codes: List[str] = []
        self._underlying_codes: List[str] = []
        # HTTP can be concurrent; history DB writes stay serialized because old
        # MariaDB/shared-host setups are more prone to deadlocks under upserts.
        self._history_write_lock = asyncio.Lock()

    async def _sleep_or_shutdown(self, seconds: float) -> None:
        """Sleep for up to ``seconds`` but wake immediately on shutdown."""
        if self._shutdown or seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=float(seconds))
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------
    # Market calendar
    # ------------------------------------------------------------------

    def is_market_day(self, current: Optional[datetime] = None) -> bool:
        current = current or tehran_now()
        return current.weekday() in MARKET_WEEKDAYS and current.date() not in MARKET_HOLIDAYS

    def is_market_open(self, current: Optional[datetime] = None) -> bool:
        current = current or tehran_now()
        return self.is_market_day(current) and MARKET_OPEN <= current.time() <= MARKET_CLOSE

    def next_market_open(self) -> datetime:
        now = datetime.now(TEHRAN_TZ)
        for offset in range(370):
            day = now.date() + timedelta(days=offset)
            candidate = datetime.combine(day, MARKET_OPEN)
            if (
                day.weekday() in MARKET_WEEKDAYS
                and day not in MARKET_HOLIDAYS
                and candidate.replace(tzinfo=TEHRAN_TZ) > now
            ):
                return candidate
        return tehran_now() + timedelta(hours=1)

    async def wait_until_market_open(self) -> bool:
        if self.is_market_open():
            return True
        nxt = self.next_market_open()
        print(
            f"[{tehran_now():%Y-%m-%d %H:%M:%S}] ⏸ Market closed; "
            f"collector idle until {nxt:%Y-%m-%d %H:%M} Tehran."
        )
        while not self._shutdown and not self.is_market_open():
            await self._sleep_or_shutdown(30)
        return not self._shutdown

    # ------------------------------------------------------------------
    # MySQL
    # ------------------------------------------------------------------

    async def open_db(self) -> None:
        validate_configuration()
        if self.pool is not None:
            return
        self.pool = await aiomysql.create_pool(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE,
            charset=MYSQL_CHARSET,
            autocommit=False,
            minsize=MYSQL_POOL_MIN,
            maxsize=MYSQL_POOL_MAX,
            connect_timeout=MYSQL_CONNECT_TIMEOUT,
            init_command=f"SET time_zone = '{MYSQL_TIME_ZONE}'",
        )
        await self._verify_required_tables()
        print(
            f"[{tehran_now():%H:%M:%S}] ✅ MySQL ready | "
            f"{MYSQL_HOST}:{MYSQL_PORT} | {MYSQL_DATABASE}"
        )

    async def close_db(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _verify_required_tables(self) -> None:
        if not self.pool:
            raise RuntimeError("MySQL pool is not initialized.")
        missing: List[str] = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for table_name in COLLECTOR_REQUIRED_TABLES:
                    try:
                        await cur.execute(f"SELECT 1 FROM `{table_name}` LIMIT 0")
                    except Exception as exc:
                        text = str(exc).lower()
                        if "1146" in text or "doesn't exist" in text or "does not exist" in text:
                            missing.append(table_name)
                        else:
                            raise
        if missing:
            raise RuntimeError(
                "Missing collector tables: " + ", ".join(missing)
                + ". Run 00_create_all_ReyT_mysql_tables_REBUILT.sql first."
            )

    async def get_state(self, key: str) -> Optional[str]:
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT state_value FROM collector_state WHERE state_key=%s",
                    (key,),
                )
                row = await cur.fetchone()
                return str(row[0]) if row and row[0] is not None else None

    async def set_state(self, key: str, value: str) -> None:
        if not self.pool:
            raise RuntimeError("MySQL pool is not initialized.")
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO collector_state (state_key, state_value, updated_at)
                        VALUES (%s,%s,CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            state_value=VALUES(state_value),
                            updated_at=CURRENT_TIMESTAMP
                        """,
                        (key, value),
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

    async def get_all_ins_codes(self) -> List[str]:
        if not self.pool:
            return []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT ins_code
                    FROM option_contracts
                    WHERE end_date >= CURDATE()
                    UNION
                    SELECT ua_ins_code FROM underlying_assets
                    ORDER BY 1
                    """
                )
                rows = await cur.fetchall()
        return [str(x[0]) for x in rows if x and x[0]]

    async def get_symbols_needing_initial_history(self) -> List[str]:
        if not self.pool:
            return []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT codes.ins_code
                    FROM (
                        SELECT ins_code
                        FROM option_contracts
                        WHERE end_date >= CURDATE()
                        UNION
                        SELECT ua_ins_code AS ins_code FROM underlying_assets
                    ) AS codes
                    LEFT JOIN history_sync_state h ON h.ins_code=codes.ins_code
                    WHERE COALESCE(h.initial_90_day_complete,0)=0
                    ORDER BY codes.ins_code
                    """
                )
                rows = await cur.fetchall()
        return [str(x[0]) for x in rows if x and x[0]]

    @staticmethod
    def _is_deadlock_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(token in text for token in ("1213", "1205", "deadlock", "lock wait timeout", "40001"))

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def make_http_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=HTTP_CONNECTOR_LIMIT,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        return aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector)

    async def _fetch_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        *,
        allow_closed_hours: bool = False,
    ) -> Optional[Any]:
        for attempt in range(1, HTTP_RETRIES + 1):
            if self._shutdown or (not allow_closed_hours and not self.is_market_open()):
                return None
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                if attempt >= HTTP_RETRIES:
                    print(
                        f"[{tehran_now():%H:%M:%S}] ❌ Fetch failed ({attempt}/{HTTP_RETRIES}) "
                        f"| {url} | {exc}"
                    )
                    return None
                await asyncio.sleep(min(8.0, 2.0 ** (attempt - 1)))
        return None

    async def fetch_option_market_watch(self, session: aiohttp.ClientSession) -> Optional[List[Dict[str, Any]]]:
        data = await self._fetch_json(
            session,
            f"{BASE_URL}/Instrument/GetInstrumentOptionMarketWatch/1",
        )
        if isinstance(data, dict):
            rows = data.get("instrumentOptMarketWatch")
            return rows if isinstance(rows, list) else None
        return data if isinstance(data, list) else None

    async def fetch_daily_history(
        self,
        session: aiohttp.ClientSession,
        ins_code: str,
        days: int,
        *,
        allow_closed_hours: bool = False,
    ) -> Optional[Dict[str, Any]]:
        data = await self._fetch_json(
            session,
            f"{BASE_URL}/ClosingPrice/GetClosingPriceDailyList/{ins_code}/{days}",
            allow_closed_hours=allow_closed_hours,
        )
        return data if isinstance(data, dict) else None

    async def fetch_underlying_snapshot(self, session: aiohttp.ClientSession, ins_code: str) -> Optional[Dict[str, Any]]:
        data = await self._fetch_json(
            session,
            f"{BASE_URL}/ClosingPrice/GetClosingPriceInfo/{ins_code}",
        )
        return data if isinstance(data, dict) else None

    async def fetch_order_book(self, session: aiohttp.ClientSession, ins_code: str) -> Optional[Dict[str, Any]]:
        data = await self._fetch_json(session, f"{BASE_URL}/BestLimits/{ins_code}")
        return data if isinstance(data, dict) else None

    # ------------------------------------------------------------------
    # API-1 normalize + atomic live save
    # ------------------------------------------------------------------

    def normalize_market_watch(
        self,
        raw: Sequence[Dict[str, Any]],
        fetch_time: datetime,
    ) -> Tuple[Dict[str, Tuple[Any, ...]], List[NormalizedOption]]:
        underlyings: Dict[str, Tuple[Any, ...]] = {}
        options_by_code: Dict[str, NormalizedOption] = {}

        for item in raw:
            ua_raw = item.get("uaInsCode")
            ua = str(ua_raw).strip() if ua_raw not in (None, "") else ""
            if not ua:
                continue

            ua_symbol = str(item.get("lval30_UA") or "").strip() or None
            ua_close = to_decimal(item.get("pClosing_UA"))
            ua_last = to_decimal(item.get("pDrCotVal_UA"))
            ua_prev = to_decimal(item.get("priceYesterday_UA"))
            underlyings[ua] = (
                ua,
                ua_symbol,
                ua_close,
                ua_last,
                ua_prev,
                fetch_time,
            )

            begin = parse_api_date(item.get("beginDate"))
            end = parse_api_date(item.get("endDate"))
            api_dte = to_int(item.get("remainedDay"))
            dte = max(0, (end - fetch_time.date()).days) if end else api_dte

            for suffix, kind in (("_P", "PUT"), ("_C", "CALL")):
                code_raw = item.get(f"insCode{suffix}")
                code = str(code_raw).strip() if code_raw not in (None, "") else ""
                if not code:
                    continue
                option = NormalizedOption(
                    ins_code=code,
                    option_type=kind,
                    symbol=str(item.get(f"lVal18AFC{suffix}") or "").strip() or None,
                    full_symbol=str(item.get(f"lVal30{suffix}") or "").strip() or None,
                    ua_ins_code=ua,
                    ua_symbol=ua_symbol,
                    ua_close=ua_close,
                    ua_last=ua_last,
                    ua_prev_close=ua_prev,
                    contract_size=to_int(item.get("contractSize")),
                    strike=to_decimal(item.get("strikePrice")),
                    begin_date=begin,
                    end_date=end,
                    days_to_expiry=dte,
                    last=to_decimal(item.get(f"pDrCotVal{suffix}")),
                    close=to_decimal(item.get(f"pClosing{suffix}")),
                    prev_close=to_decimal(item.get(f"priceYesterday{suffix}")),
                    open_interest=to_int(item.get(f"oP{suffix}")),
                    prev_open_interest=to_int(item.get(f"yesterdayOP{suffix}")),
                    trades_count=to_int(item.get(f"zTotTran{suffix}")),
                    volume=to_int(item.get(f"qTotTran5J{suffix}")),
                    trade_value=to_decimal(item.get(f"qTotCap{suffix}")),
                    notional_value=to_decimal(item.get(f"notionalValue{suffix}")),
                    bid=to_decimal(item.get(f"pMeDem{suffix}")),
                    bid_volume=to_int(item.get(f"qTitMeDem{suffix}")),
                    ask=to_decimal(item.get(f"pMeOf{suffix}")),
                    ask_volume=to_int(item.get(f"qTitMeOf{suffix}")),
                )
                if (
                    option.end_date is None
                    or option.strike is None
                    or option.strike <= 0
                    or option.contract_size is None
                    or option.contract_size <= 0
                ):
                    continue
                options_by_code[code] = option

        return underlyings, list(options_by_code.values())

    async def save_live_bundle(
        self,
        underlyings: Dict[str, Tuple[Any, ...]],
        options: Sequence[NormalizedOption],
        fetch_time: datetime,
        greeks_rows: Sequence[Tuple[Any, ...]],
    ) -> None:
        if not self.pool:
            raise RuntimeError("MySQL pool is not initialized.")

        underlying_sql = """
            INSERT INTO underlying_assets (
                ua_ins_code, symbol, closing_price, last_trade_price,
                previous_closing_price, last_update, created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                symbol=COALESCE(VALUES(symbol),symbol),
                closing_price=VALUES(closing_price),
                last_trade_price=VALUES(last_trade_price),
                previous_closing_price=VALUES(previous_closing_price),
                last_update=VALUES(last_update),
                updated_at=CURRENT_TIMESTAMP
        """
        contract_sql = """
            INSERT INTO option_contracts (
                ins_code, ua_ins_code, contract_type, short_symbol, full_symbol,
                strike_price, contract_size, begin_date, end_date,
                begin_date_shamsi, end_date_shamsi, remained_days,
                created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                ua_ins_code=VALUES(ua_ins_code),
                contract_type=VALUES(contract_type),
                short_symbol=VALUES(short_symbol),
                full_symbol=VALUES(full_symbol),
                strike_price=VALUES(strike_price),
                contract_size=VALUES(contract_size),
                begin_date=VALUES(begin_date),
                end_date=VALUES(end_date),
                begin_date_shamsi=COALESCE(VALUES(begin_date_shamsi),begin_date_shamsi),
                end_date_shamsi=COALESCE(VALUES(end_date_shamsi),end_date_shamsi),
                remained_days=VALUES(remained_days),
                updated_at=CURRENT_TIMESTAMP
        """
        tick_sql = """
            INSERT INTO market_data_ticks (
                ins_code, tick_time, last_price, closing_price, previous_close,
                bid_price, bid_volume, ask_price, ask_volume,
                open_interest, previous_open_interest, trade_count_today,
                volume_today, volume_5d, value_today, notional_value,
                created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                tick_time=VALUES(tick_time),
                last_price=VALUES(last_price),
                closing_price=VALUES(closing_price),
                previous_close=VALUES(previous_close),
                bid_price=VALUES(bid_price),
                bid_volume=VALUES(bid_volume),
                ask_price=VALUES(ask_price),
                ask_volume=VALUES(ask_volume),
                open_interest=VALUES(open_interest),
                previous_open_interest=VALUES(previous_open_interest),
                trade_count_today=VALUES(trade_count_today),
                volume_today=VALUES(volume_today),
                volume_5d=VALUES(volume_5d),
                value_today=VALUES(value_today),
                notional_value=VALUES(notional_value),
                updated_at=CURRENT_TIMESTAMP
        """
        greeks_sql = """
            INSERT INTO option_greeks (
                ins_code, fetch_datetime, ua_ins_code, option_type, symbol, ua_symbol,
                expiry_date, strike, days_to_expiry, underlying_price, option_price,
                bid, ask, spread, spread_percent, intrinsic_value, time_value,
                moneyness, distance_to_strike, implied_volatility,
                delta, gamma, theta, vega, rho, leverage, elasticity, break_even,
                open_interest, created_at, updated_at
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
            )
            ON DUPLICATE KEY UPDATE
                fetch_datetime=VALUES(fetch_datetime),
                ua_ins_code=VALUES(ua_ins_code),
                option_type=VALUES(option_type),
                symbol=VALUES(symbol),
                ua_symbol=VALUES(ua_symbol),
                expiry_date=VALUES(expiry_date),
                strike=VALUES(strike),
                days_to_expiry=VALUES(days_to_expiry),
                underlying_price=VALUES(underlying_price),
                option_price=VALUES(option_price),
                bid=VALUES(bid), ask=VALUES(ask), spread=VALUES(spread),
                spread_percent=VALUES(spread_percent),
                intrinsic_value=VALUES(intrinsic_value),
                time_value=VALUES(time_value),
                moneyness=VALUES(moneyness),
                distance_to_strike=VALUES(distance_to_strike),
                implied_volatility=VALUES(implied_volatility),
                delta=VALUES(delta), gamma=VALUES(gamma), theta=VALUES(theta),
                vega=VALUES(vega), rho=VALUES(rho), leverage=VALUES(leverage),
                elasticity=VALUES(elasticity), break_even=VALUES(break_even),
                open_interest=VALUES(open_interest),
                updated_at=CURRENT_TIMESTAMP
        """

        contract_rows = [
            (
                x.ins_code, x.ua_ins_code, x.option_type, x.symbol, x.full_symbol,
                x.strike, x.contract_size, x.begin_date, x.end_date,
                to_jalali_label(x.begin_date), to_jalali_label(x.end_date),
                x.days_to_expiry,
            )
            for x in options
        ]
        tick_rows = [
            (
                x.ins_code, fetch_time, x.last, x.close, x.prev_close,
                x.bid, x.bid_volume, x.ask, x.ask_volume,
                x.open_interest, x.prev_open_interest, x.trades_count,
                x.volume, x.volume, x.trade_value, x.notional_value,
            )
            for x in options
        ]
        revision = f"{fetch_time.isoformat()}|{len(options)}"

        async with self.pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    if underlyings:
                        await cur.executemany(underlying_sql, list(underlyings.values()))
                    if contract_rows:
                        await cur.executemany(contract_sql, contract_rows)
                    if tick_rows:
                        await cur.executemany(tick_sql, tick_rows)
                    if CALCULATE_GREEKS and greeks_rows:
                        await cur.executemany(greeks_sql, list(greeks_rows))
                    await cur.execute(
                        """
                        INSERT INTO collector_state (state_key,state_value,updated_at)
                        VALUES ('live_source_revision',%s,CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            state_value=VALUES(state_value), updated_at=CURRENT_TIMESTAMP
                        """,
                        (revision,),
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

        self._underlying_codes = sorted(underlyings)
        option_codes = sorted(x.ins_code for x in options)
        self._order_book_codes = (
            sorted(set(option_codes) | set(self._underlying_codes))
            if ORDER_BOOK_INCLUDE_UNDERLYINGS else option_codes
        )

    async def collect_live_once(self, session: aiohttp.ClientSession) -> int:
        raw = await self.fetch_option_market_watch(session)
        if raw is None:
            return 0
        fetch_time = tehran_now().replace(microsecond=0)
        underlyings, options = self.normalize_market_watch(raw, fetch_time)
        if not options:
            print(f"[{fetch_time:%H:%M:%S}] ⚠️ API 1 returned no valid option contracts.")
            return 0

        greeks_rows: List[Tuple[Any, ...]] = []
        if CALCULATE_GREEKS:
            greeks_rows = build_greeks_rows(options, fetch_time)

        await self.save_live_bundle(underlyings, options, fetch_time, greeks_rows)
        valid_iv = sum(1 for row in greeks_rows if row[19] is not None)
        print(
            f"[{fetch_time:%H:%M:%S}] ✅ LIVE atomic commit | "
            f"series={len(raw)} | underlyings={len(underlyings)} | "
            f"contracts={len(options)} | greeks={len(greeks_rows)} | IV={valid_iv}"
        )
        return len(options)

    # ------------------------------------------------------------------
    # API-3 underlying snapshots
    # ------------------------------------------------------------------

    async def save_underlying_snapshot(self, payload: Dict[str, Any], ins_code: str) -> bool:
        if not self.pool:
            raise RuntimeError("MySQL pool is not initialized.")
        info = payload.get("closingPriceInfo")
        if not isinstance(info, dict):
            return False
        state = info.get("instrumentState")
        state = state if isinstance(state, dict) else {}
        last_update = event_datetime(
            info.get("dEven"), info.get("hEven") or info.get("lastHEven")
        )
        params = (
            ins_code,
            str(state.get("lVal18AFC") or "").strip() or None,
            str(state.get("lVal30") or "").strip() or None,
            str(state.get("cEtaval") or "").strip() or None,
            state.get("cEtavalTitle"),
            to_bool(state.get("underSupervision")),
            to_decimal(info.get("pClosing")),
            to_decimal(info.get("pDrCotVal")),
            to_decimal(info.get("priceYesterday")),
            last_update,
        )
        sql = """
            INSERT INTO underlying_assets (
                ua_ins_code,symbol,full_name,status_code,status_title,under_supervision,
                closing_price,last_trade_price,previous_closing_price,last_update,
                created_at,updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                symbol=COALESCE(VALUES(symbol),symbol),
                full_name=COALESCE(VALUES(full_name),full_name),
                status_code=COALESCE(VALUES(status_code),status_code),
                status_title=COALESCE(VALUES(status_title),status_title),
                under_supervision=VALUES(under_supervision),
                closing_price=VALUES(closing_price),
                last_trade_price=VALUES(last_trade_price),
                previous_closing_price=VALUES(previous_closing_price),
                last_update=VALUES(last_update),
                updated_at=CURRENT_TIMESTAMP
        """
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return True

    async def collect_underlying_snapshots_once(self, session: aiohttp.ClientSession) -> Tuple[int, int]:
        codes = list(self._underlying_codes)
        if not codes and self.pool:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT ua_ins_code FROM underlying_assets ORDER BY ua_ins_code")
                    codes = [str(x[0]) for x in await cur.fetchall() if x and x[0]]
        if not codes:
            return 0, 0

        sem = asyncio.Semaphore(SNAPSHOT_CONCURRENCY)

        async def one(code: str) -> bool:
            async with sem:
                if self._shutdown or not self.is_market_open():
                    return False
                payload = await self.fetch_underlying_snapshot(session, code)
                return bool(payload and await self.save_underlying_snapshot(payload, code))

        results = await asyncio.gather(*(one(code) for code in codes), return_exceptions=True)
        success = sum(x is True for x in results)
        errors = [x for x in results if isinstance(x, Exception)]
        if errors:
            print(f"[{tehran_now():%H:%M:%S}] ⚠️ API 3 errors: {len(errors)}")
        print(f"[{tehran_now():%H:%M:%S}] ✅ API 3 refreshed | {success}/{len(codes)} underlyings")
        return success, len(codes)

    # ------------------------------------------------------------------
    # API-4 order book
    # ------------------------------------------------------------------

    async def next_order_book_batch(self) -> List[str]:
        codes = self._order_book_codes
        if not codes:
            all_codes = await self.get_all_ins_codes()
            if not ORDER_BOOK_INCLUDE_UNDERLYINGS and self.pool:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT ins_code FROM option_contracts ORDER BY ins_code")
                        all_codes = [str(x[0]) for x in await cur.fetchall() if x and x[0]]
            self._order_book_codes = all_codes
            codes = self._order_book_codes
        if not codes:
            return []
        size = min(ORDER_BOOK_BATCH_SIZE, len(codes))
        start = self._order_book_cursor % len(codes)
        batch = [codes[(start + i) % len(codes)] for i in range(size)]
        self._order_book_cursor = (start + size) % len(codes)
        return batch

    def parse_order_book_rows(
        self,
        payload: Dict[str, Any],
        ins_code: str,
        snapshot_time: datetime,
    ) -> List[Tuple[Any, ...]]:
        levels = payload.get("bestLimits")
        if not isinstance(levels, list):
            return []
        rows: List[Tuple[Any, ...]] = []
        for item in levels:
            level = to_int(item.get("number"))
            if level is None or not 1 <= level <= 5:
                continue
            rows.append((
                ins_code,
                level,
                snapshot_time,
                to_decimal(item.get("pMeDem")),
                to_int(item.get("qTitMeDem")),
                to_int(item.get("zOrdMeDem")),
                to_decimal(item.get("pMeOf")),
                to_int(item.get("qTitMeOf")),
                to_int(item.get("zOrdMeOf")),
            ))
        return rows

    async def save_order_book_batch(
        self,
        rows_by_code: Dict[str, List[Tuple[Any, ...]]],
    ) -> int:
        if not self.pool or not rows_by_code:
            return 0
        successful_codes = list(rows_by_code)
        rows = [row for code in successful_codes for row in rows_by_code[code]]
        placeholders = ",".join(["%s"] * len(successful_codes))
        insert_sql = """
            INSERT INTO order_book_depth (
                ins_code,`level`,snapshot_time,bid_price,bid_volume,bid_order_count,
                ask_price,ask_volume,ask_order_count,created_at,updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    # Delete+insert is deliberate: if API returns fewer than 5 levels,
                    # old deeper levels must not survive as fake current liquidity.
                    await cur.execute(
                        f"DELETE FROM order_book_depth WHERE ins_code IN ({placeholders})",
                        tuple(successful_codes),
                    )
                    if rows:
                        await cur.executemany(insert_sql, rows)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return len(rows)

    async def collect_order_books_once(self, session: aiohttp.ClientSession) -> Tuple[int, int, int]:
        codes = await self.next_order_book_batch()
        if not codes:
            return 0, 0, 0
        snapshot_time = tehran_now().replace(microsecond=0)
        sem = asyncio.Semaphore(ORDER_BOOK_CONCURRENCY)

        async def one(code: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            async with sem:
                if self._shutdown or not self.is_market_open():
                    return code, None
                return code, await self.fetch_order_book(session, code)

        fetched = await asyncio.gather(*(one(code) for code in codes), return_exceptions=True)
        rows_by_code: Dict[str, List[Tuple[Any, ...]]] = {}
        errors = 0
        for item in fetched:
            if isinstance(item, Exception):
                errors += 1
                continue
            code, payload = item
            if payload is not None and isinstance(payload.get("bestLimits"), list):
                rows_by_code[code] = self.parse_order_book_rows(
                    payload, code, snapshot_time
                )
        saved = await self.save_order_book_batch(rows_by_code)
        success = len(rows_by_code)
        print(
            f"[{tehran_now():%H:%M:%S}] ✅ API 4 refreshed | "
            f"{success}/{len(codes)} instruments | levels={saved} | errors={errors}"
        )
        return success, len(codes), saved

    # ------------------------------------------------------------------
    # API-2 history
    # ------------------------------------------------------------------

    async def save_daily_history(
        self,
        payload: Dict[str, Any],
        ins_code: str,
        mark_initial_complete: bool,
        *,
        latest_only: bool = False,
        after_date: Optional[date] = None,
    ) -> int:
        if not self.pool:
            raise RuntimeError("MySQL pool is not initialized.")
        raw = payload.get("closingPriceDaily")
        if not isinstance(raw, list):
            return 0
        rows: List[Tuple[Any, ...]] = []
        for item in raw:
            trade_date = parse_api_date(item.get("dEven"))
            if not trade_date:
                continue
            if after_date is not None and trade_date <= after_date:
                continue
            rows.append((
                ins_code,
                trade_date,
                to_decimal(item.get("priceFirst")),
                to_decimal(item.get("priceMax")),
                to_decimal(item.get("priceMin")),
                to_decimal(item.get("pClosing")),
                to_decimal(item.get("pDrCotVal")),
                to_decimal(item.get("priceYesterday")),
                to_decimal(item.get("priceChange")),
                to_int(item.get("qTotTran5J")),
                to_decimal(item.get("qTotCap")),
                to_int(item.get("zTotTran")),
            ))
        if not rows:
            return 0
        if latest_only:
            rows = [max(rows, key=lambda x: x[1])]
        last_trade_date = max(x[1] for x in rows)
        daily_sql = """
            INSERT INTO daily_market_data (
                ins_code,trade_date,open_price,high_price,low_price,close_price,
                last_price,previous_close,price_change,volume,value,trade_count,
                created_at,updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                open_price=VALUES(open_price), high_price=VALUES(high_price),
                low_price=VALUES(low_price), close_price=VALUES(close_price),
                last_price=VALUES(last_price), previous_close=VALUES(previous_close),
                price_change=VALUES(price_change), volume=VALUES(volume),
                value=VALUES(value), trade_count=VALUES(trade_count),
                updated_at=CURRENT_TIMESTAMP
        """
        state_sql = """
            INSERT INTO history_sync_state (
                ins_code,initial_90_day_complete,last_trade_date,last_success_at,last_error,updated_at
            )
            VALUES (%s,%s,%s,CURRENT_TIMESTAMP,NULL,CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
                initial_90_day_complete=GREATEST(initial_90_day_complete,VALUES(initial_90_day_complete)),
                last_trade_date=VALUES(last_trade_date),
                last_success_at=CURRENT_TIMESTAMP,
                last_error=NULL,
                updated_at=CURRENT_TIMESTAMP
        """

        last_exc: Optional[Exception] = None
        for attempt in range(1, DB_DEADLOCK_RETRIES + 1):
            try:
                async with self._history_write_lock:
                    async with self.pool.acquire() as conn:
                        try:
                            await conn.begin()
                            async with conn.cursor() as cur:
                                await cur.executemany(daily_sql, rows)
                                await cur.execute(
                                    state_sql,
                                    (ins_code, 1 if mark_initial_complete else 0, last_trade_date),
                                )
                            await conn.commit()
                        except Exception:
                            await conn.rollback()
                            raise
                return len(rows)
            except Exception as exc:
                last_exc = exc
                if self._is_deadlock_error(exc) and attempt < DB_DEADLOCK_RETRIES:
                    delay = DB_DEADLOCK_BASE_DELAY * (2 ** (attempt - 1))
                    print(
                        f"[{tehran_now():%H:%M:%S}] ⚠️ History deadlock {ins_code}; "
                        f"retry in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
        if last_exc:
            raise last_exc
        return 0

    async def mark_history_error(self, ins_code: str, exc: Exception) -> None:
        if not self.pool:
            return
        text = str(exc)[:1000]
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO history_sync_state (
                            ins_code,initial_90_day_complete,last_error,updated_at
                        ) VALUES (%s,0,%s,CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            last_error=VALUES(last_error), updated_at=CURRENT_TIMESTAMP
                        """,
                        (ins_code, text),
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()

    async def sync_history_for_codes(
        self,
        session: aiohttp.ClientSession,
        codes: Sequence[str],
        day_count: int,
        label: str,
        mark_initial_complete: bool,
    ) -> Tuple[int, int, bool]:
        if not codes:
            return 0, 0, True
        sem = asyncio.Semaphore(HISTORY_CONCURRENCY)
        success = 0
        saved_rows = 0
        failed = 0
        lock = asyncio.Lock()

        async def one(code: str) -> None:
            nonlocal success, saved_rows, failed
            async with sem:
                if self._shutdown or not self.is_market_open():
                    return
                try:
                    payload = await self.fetch_daily_history(session, code, day_count)
                    count = await self.save_daily_history(payload, code, mark_initial_complete) if payload else 0
                    async with lock:
                        if count:
                            success += 1
                            saved_rows += count
                        else:
                            failed += 1
                except Exception as exc:
                    await self.mark_history_error(code, exc)
                    async with lock:
                        failed += 1
                if HISTORY_REQUEST_DELAY and self.is_market_open() and not self._shutdown:
                    await asyncio.sleep(HISTORY_REQUEST_DELAY)

        print(
            f"[{tehran_now():%H:%M:%S}] 📅 {label} | symbols={len(codes)} | days={day_count}"
        )
        chunk_size = 50
        completed = 0
        for start in range(0, len(codes), chunk_size):
            if self._shutdown or not self.is_market_open():
                break
            chunk = codes[start:start + chunk_size]
            await asyncio.gather(*(one(code) for code in chunk))
            completed = start + len(chunk)
            print(
                f"   History progress {min(completed,len(codes))}/{len(codes)} "
                f"| saved={saved_rows} | failed={failed}"
            )
        all_done = completed >= len(codes) and not self._shutdown and self.is_market_open()
        return success, saved_rows, all_done

    async def prune_history(self) -> int:
        # Deliberately disabled: the initial one-year archive is permanent and
        # every completed market day is appended thereafter. No historical rows
        # are deleted by the collector.
        return 0

    async def market_history_once(self, session: aiohttp.ClientSession, skip_history: bool) -> None:
        """Bootstrap roughly one trading year (252 daily rows) once per instrument. No rolling in-market refresh."""
        if skip_history or not self.is_market_open():
            return
        all_codes = await self.get_all_ins_codes()
        if not all_codes:
            print(f"[{tehran_now():%H:%M:%S}] ⚠️ No instruments available for initial history sync.")
            return
        initial_set = set(await self.get_symbols_needing_initial_history())
        initial_codes = [x for x in all_codes if x in initial_set]
        if not initial_codes:
            return
        _, _, initial_done = await self.sync_history_for_codes(
            session, initial_codes, INITIAL_HISTORY_DAYS,
            "Initial one-year history", True,
        )
        if initial_done:
            print(
                f"[{tehran_now():%H:%M:%S}] ✅ Initial history bootstrap complete "
                f"| days_requested={INITIAL_HISTORY_DAYS} | pruning=OFF"
            )

    async def get_history_last_dates(self, codes: Sequence[str]) -> Dict[str, Optional[date]]:
        if not self.pool or not codes:
            return {}
        result: Dict[str, Optional[date]] = {str(c): None for c in codes}
        # Avoid a huge IN list by reading the small state table once.
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT ins_code,last_trade_date FROM history_sync_state")
                for code, last_date in await cur.fetchall():
                    key = str(code)
                    if key in result:
                        result[key] = last_date
        return result

    async def eod_history_append_once(self) -> bool:
        """Try one EOD append and return True only when today's history is confirmed."""
        now = tehran_now()
        if not self.is_market_day(now):
            return True
        today = now.date().isoformat()
        await self.open_db()
        try:
            if await self.get_state("eod_history_last_sync") == today:
                return True
            codes = await self.get_all_ins_codes()
            if not codes:
                print(f"[{now:%H:%M:%S}] ⚠️ EOD history append skipped: no instruments.")
                return False
            last_dates = await self.get_history_last_dates(codes)
            sem = asyncio.Semaphore(HISTORY_CONCURRENCY)
            saved = 0
            failed = 0
            unchanged = 0
            lock = asyncio.Lock()

            async with self.make_http_session() as session:
                async def one(code: str) -> None:
                    nonlocal saved, failed, unchanged
                    async with sem:
                        if self._shutdown:
                            return
                        try:
                            payload = await self.fetch_daily_history(
                                session, code, EOD_HISTORY_LOOKBACK_DAYS,
                                allow_closed_hours=True,
                            )
                            count = await self.save_daily_history(
                                payload or {}, code, False, latest_only=True,
                                after_date=last_dates.get(code),
                            )
                            async with lock:
                                if count:
                                    saved += count
                                else:
                                    unchanged += 1
                        except Exception as exc:
                            await self.mark_history_error(code, exc)
                            async with lock:
                                failed += 1
                        if HISTORY_REQUEST_DELAY and not self._shutdown:
                            await asyncio.sleep(HISTORY_REQUEST_DELAY)

                print(
                    f"[{now:%H:%M:%S}] 📅 EOD single-row history append "
                    f"| symbols={len(codes)} | lookback={EOD_HISTORY_LOOKBACK_DAYS}"
                )
                chunk_size = 50
                for start_i in range(0, len(codes), chunk_size):
                    if self._shutdown:
                        break
                    chunk = codes[start_i:start_i + chunk_size]
                    await asyncio.gather(*(one(code) for code in chunk))

            if not self._shutdown:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT
                                COUNT(DISTINCT CASE
                                    WHEN d.trade_date=%s THEN u.ua_ins_code
                                END) AS current_rows,
                                COUNT(DISTINCT u.ua_ins_code) AS required_rows
                            FROM underlying_assets u
                            LEFT JOIN daily_market_data d
                              ON d.ins_code=u.ua_ins_code
                             AND d.trade_date=%s
                            """,
                            (now.date(), now.date()),
                        )
                        row = await cur.fetchone()
                        current_underlying_rows = int(row[0] or 0) if row else 0
                        required_underlying_rows = int(row[1] or 0) if row else 0

                if (
                    required_underlying_rows > 0
                    and current_underlying_rows >= required_underlying_rows
                ):
                    await self.set_state("eod_history_last_sync", today)
                    print(
                        f"[{tehran_now():%H:%M:%S}] ✅ EOD history append complete "
                        f"| appended={saved} | unchanged={unchanged} | failed={failed} "
                        f"| today_underlyings={current_underlying_rows}/{required_underlying_rows} | pruning=OFF"
                    )
                    return True

                print(
                    f"[{tehran_now():%H:%M:%S}] ⚠️ EOD history not finalized "
                    f"| appended={saved} | unchanged={unchanged} | failed={failed} "
                    f"| today_underlyings={current_underlying_rows}/{required_underlying_rows} | state NOT advanced"
                )
                return False

            return False
        finally:
            await self.close_db()

    async def eod_history_append_with_retry(self) -> None:
        for attempt in range(1, EOD_HISTORY_MAX_RETRIES + 1):
            if self._shutdown:
                return

            complete = await self.eod_history_append_once()
            if complete:
                return

            if attempt >= EOD_HISTORY_MAX_RETRIES:
                print(
                    f"[{tehran_now():%H:%M:%S}] ⚠️ EOD history still incomplete "
                    f"after {attempt} attempts; will catch up on the next service/market cycle."
                )
                return

            print(
                f"[{tehran_now():%H:%M:%S}] ⏳ EOD history retry "
                f"{attempt + 1}/{EOD_HISTORY_MAX_RETRIES} "
                f"in {EOD_HISTORY_RETRY_SECONDS}s"
            )
            await self._sleep_or_shutdown(EOD_HISTORY_RETRY_SECONDS)

    async def wait_until_eod(self) -> bool:
        while not self._shutdown:
            now = tehran_now()
            if self.is_market_day(now) and now.time() >= EOD_HISTORY_TIME:
                return True
            if self.is_market_day(now) and MARKET_CLOSE < now.time() < EOD_HISTORY_TIME:
                target = datetime.combine(now.date(), EOD_HISTORY_TIME)
                await self._sleep_or_shutdown(min(max((target - now).total_seconds(), 1.0), 60.0))
            else:
                return False
        return False

    # ------------------------------------------------------------------
    # Schedulers
    # ------------------------------------------------------------------

    async def realtime_loop(self, session: aiohttp.ClientSession, skip_initial: bool = False) -> None:
        if skip_initial:
            await asyncio.sleep(REALTIME_INTERVAL)
        while not self._shutdown and self.is_market_open():
            started = asyncio.get_running_loop().time()
            try:
                await self.collect_live_once(session)
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ LIVE loop error: {exc}")
            elapsed = asyncio.get_running_loop().time() - started
            await self._sleep_or_shutdown(max(0.25, REALTIME_INTERVAL - elapsed))

    async def snapshot_loop(self, session: aiohttp.ClientSession) -> None:
        while not self._shutdown and self.is_market_open():
            started = asyncio.get_running_loop().time()
            try:
                await self.collect_underlying_snapshots_once(session)
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ API 3 loop error: {exc}")
            elapsed = asyncio.get_running_loop().time() - started
            await self._sleep_or_shutdown(max(0.5, SNAPSHOT_INTERVAL - elapsed))

    async def order_book_loop(self, session: aiohttp.ClientSession) -> None:
        while not self._shutdown and self.is_market_open():
            started = asyncio.get_running_loop().time()
            try:
                await self.collect_order_books_once(session)
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ API 4 loop error: {exc}")
            elapsed = asyncio.get_running_loop().time() - started
            await self._sleep_or_shutdown(max(0.5, ORDER_BOOK_INTERVAL - elapsed))

    async def history_loop(self, session: aiohttp.ClientSession, skip_history: bool) -> None:
        try:
            await self.market_history_once(session, skip_history)
        except Exception as exc:
            print(f"[{tehran_now():%H:%M:%S}] ❌ History loop error: {exc}")

    async def run_once(self, skip_history: bool, skip_greeks: bool = False) -> None:
        global CALCULATE_GREEKS
        if not self.is_market_open():
            print("⏸ Market is closed; --once was not executed.")
            return
        original_greeks = CALCULATE_GREEKS
        if skip_greeks:
            CALCULATE_GREEKS = False
        try:
            async with self.make_http_session() as session:
                await self.collect_live_once(session)
                if self.is_market_open():
                    await asyncio.gather(
                        self.collect_underlying_snapshots_once(session),
                        self.collect_order_books_once(session),
                    )
                if not skip_history and self.is_market_open():
                    await self.market_history_once(session, False)
        finally:
            CALCULATE_GREEKS = original_greeks

    async def run_market_session(self, skip_history: bool, skip_greeks: bool = False) -> None:
        global CALCULATE_GREEKS
        original_greeks = CALCULATE_GREEKS
        if skip_greeks:
            CALCULATE_GREEKS = False
        await self.open_db()
        try:
            print("=" * 82)
            print("ReyT Unified TSETMC Options Collector - MySQL/MariaDB")
            print("=" * 82)
            print(f"Database          : {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
            print(f"Python            : {sys.version.split()[0]}")
            print(f"Market hours      : {MARKET_OPEN:%H:%M}-{MARKET_CLOSE:%H:%M} Tehran")
            print(f"Live API-1        : every {REALTIME_INTERVAL:g}s, atomic commit")
            print(f"Greeks            : {'ON' if CALCULATE_GREEKS else 'OFF'}")
            print(f"Underlying API-3  : every {SNAPSHOT_INTERVAL:g}s")
            print(
                f"Order book API-4  : {ORDER_BOOK_BATCH_SIZE} instruments / "
                f"{ORDER_BOOK_INTERVAL:g}s, concurrency={ORDER_BOOK_CONCURRENCY}"
            )
            print(f"Initial history    : {INITIAL_HISTORY_DAYS} trading-session rows requested once")
            print(f"EOD history append : {EOD_HISTORY_TIME:%H:%M} Tehran | max 1 new row/instrument/day")
            print("History retention : unlimited / pruning OFF")
            print(f"Closed hours      : idle except EOD history append at {EOD_HISTORY_TIME:%H:%M}")
            print("=" * 82)

            async with self.make_http_session() as session:
                # Seed API-1 before any dependent job. This ensures fresh instrument
                # universe and creates a complete atomic live snapshot immediately.
                await self.collect_live_once(session)
                if self._shutdown or not self.is_market_open():
                    return
                await asyncio.gather(
                    self.realtime_loop(session, skip_initial=True),
                    self.snapshot_loop(session),
                    self.order_book_loop(session),
                    self.history_loop(session, skip_history),
                )
        finally:
            CALCULATE_GREEKS = original_greeks
            await self.close_db()
            print(
                f"[{tehran_now():%H:%M:%S}] ⏸ Market session ended; MySQL closed."
            )


# ============================================================================
# CLI
# ============================================================================


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="ReyT optimized TSETMC options collector for MySQL/MariaDB"
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one market-hours collection cycle.")
    mode.add_argument("--watch", action="store_true", help="Run continuously during market hours.")
    p.add_argument("--skip-history", action="store_true", help="Skip initial/daily history synchronization.")
    p.add_argument("--skip-greeks", action="store_true", help="Do not calculate/update option_greeks.")
    return p


async def async_main(args: argparse.Namespace) -> None:
    validate_configuration()
    collector = ReyTCollector()

    def request_shutdown() -> None:
        if not collector._shutdown:
            print("\n🛑 Shutdown requested...")
            collector._shutdown = True
            collector._shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    if args.once:
        if not collector.is_market_open():
            nxt = collector.next_market_open()
            print(
                f"⏸ Market is closed. --once not run. "
                f"Next configured opening: {nxt:%Y-%m-%d %H:%M} Tehran."
            )
            return
        await collector.open_db()
        try:
            await collector.run_once(args.skip_history, args.skip_greeks)
        finally:
            await collector.close_db()
        return

    # Default mode is watch when no mode flag is supplied.
    while not collector._shutdown:
        now = tehran_now()

        # Same-day catch-up: if the service starts after 13:00, perform the
        # single EOD append once, then wait for the next market open.
        if collector.is_market_day(now) and now.time() >= EOD_HISTORY_TIME:
            try:
                await collector.eod_history_append_with_retry()
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ EOD history error: {exc}")

        if collector.is_market_day(tehran_now()) and MARKET_CLOSE < tehran_now().time() < EOD_HISTORY_TIME:
            if await collector.wait_until_eod():
                try:
                    await collector.eod_history_append_with_retry()
                except Exception as exc:
                    print(f"[{tehran_now():%H:%M:%S}] ❌ EOD history error: {exc}")
            continue

        if not await collector.wait_until_market_open():
            break
        try:
            await collector.run_market_session(args.skip_history, args.skip_greeks)
        except Exception as exc:
            await collector.close_db()
            print(f"[{tehran_now():%H:%M:%S}] ❌ Market session error: {exc}")
            if collector.is_market_open() and not collector._shutdown:
                await collector._sleep_or_shutdown(30)

        # Market session ends at 12:30. Stay alive and perform exactly one
        # post-market daily-history append at 13:00 Tehran.
        if not collector._shutdown and await collector.wait_until_eod():
            try:
                await collector.eod_history_append_with_retry()
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ EOD history error: {exc}")

    await collector.close_db()
    print("👋 Collector stopped.")


def main() -> None:
    args = build_parser().parse_args()
    if not args.once and not args.watch:
        args.watch = True
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"❌ Collector error: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
