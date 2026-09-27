#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ReyT isolated Short Straddle / Short Strangle paper-trading engine.

This module is intentionally isolated from the legacy ReyT paper-trading stack.
It READS the existing collector-owned market tables and WRITES only the additive
short_premium_* / short_straddle_signals / short_strangle_signals tables.

Core locked behavior:
- Separate 100M-toman accounts for SHORT_STRADDLE and SHORT_STRANGLE.
- 70% Entry / 30% Adjustment targets based on realized account equity.
- 5..30 calendar-day DTE.
- Entry/Adjustment sells only at Best Bid (Level 1); no deeper sweep.
- Normal/forced close buys only at Best Ask (Level 1).
- Scheduled exit (previous trading day from 12:00) may consume Ask L1 then L2.
- Residual scheduled exit remains EXITING and retries on expiry day from 09:15.
- Full-position break-evens are recomputed from net cashflows after every fill.
"""

from __future__ import annotations

import argparse
import asyncio
import configparser
import json
import math
import os
import signal
import ssl
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import aiomysql
import pymysql


TEHRAN_TZ = ZoneInfo("Asia/Tehran")
BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = Path(
    os.getenv("SHORT_PREMIUM_CONFIG_FILE", "/etc/reyt/short-premium/settings.ini")
)

CFG = configparser.ConfigParser(interpolation=None, strict=False)
if CONFIG_FILE.exists():
    CFG.read(CONFIG_FILE, encoding="utf-8")


def _setting(name: str, default: str = "") -> str:
    env = os.getenv(name)
    if env is not None:
        return env.strip()
    key = name.lower()
    for section in ("short_premium", "database", "mysql", "mariadb", "sql"):
        if CFG.has_option(section, key):
            return CFG.get(section, key).strip()
    return default


def _bool(name: str, default: bool) -> bool:
    raw = _setting(name, "yes" if default else "no").lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _int(name: str, default: int, minimum: Optional[int] = None) -> int:
    value = int(_setting(name, str(default)))
    if minimum is not None:
        value = max(minimum, value)
    return value


def _dec(name: str, default: str) -> Decimal:
    return Decimal(_setting(name, default))


def _clock(name: str, default: str) -> time:
    return datetime.strptime(_setting(name, default), "%H:%M").time()


def _holiday_set(raw: str) -> set[date]:
    out: set[date] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.add(datetime.strptime(item, "%Y-%m-%d").date())
    return out


MYSQL_HOST = _setting("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = _int("MYSQL_PORT", 3306, 1)
MYSQL_DATABASE = _setting("MYSQL_DATABASE", "ghazali1_ReyTOption")
MYSQL_USER = _setting("MYSQL_USER", "reyt_app")
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", "")
MYSQL_CHARSET = _setting("MYSQL_CHARSET", "utf8mb4")
MYSQL_TIME_ZONE = _setting("MYSQL_TIME_ZONE", "+03:30")
MYSQL_CONNECT_TIMEOUT = _int("MYSQL_CONNECT_TIMEOUT", 20, 1)
MYSQL_SSL = _bool("MYSQL_SSL", False)

DB_POOL_MIN = _int("DB_POOL_MIN", 1, 1)
DB_POOL_MAX = _int("DB_POOL_MAX", 4, DB_POOL_MIN)

STRADDLE_ACCOUNT_NAME = _setting("STRADDLE_ACCOUNT_NAME", "paper_100m_short_straddle")
STRANGLE_ACCOUNT_NAME = _setting("STRANGLE_ACCOUNT_NAME", "paper_100m_short_strangle")
INITIAL_CAPITAL_TOMAN = _dec("INITIAL_CAPITAL_TOMAN", "100000000")
TOMAN_TO_RIAL = Decimal("10")
INITIAL_CAPITAL_RIAL = INITIAL_CAPITAL_TOMAN * TOMAN_TO_RIAL

ALLOWED_UNDERLYINGS = tuple(
    x.strip()
    for x in _setting("ALLOWED_UNDERLYINGS", "شستا,شپنا,وبملت,اهرم,فملی").split(",")
    if x.strip()
)

MIN_DTE = _int("MIN_DTE", 5, 0)
MAX_DTE = _int("MAX_DTE", 30, MIN_DTE)
MARKET_START = _clock("MARKET_START", "09:15")
NEW_ENTRY_CUTOFF = _clock("NEW_ENTRY_CUTOFF", "12:15")
MARKET_END = _clock("MARKET_END", "12:30")
SCHEDULED_EXIT_TIME = _clock("SCHEDULED_EXIT_TIME", "12:00")
SCAN_INTERVAL_SECONDS = _int("SCAN_INTERVAL_SECONDS", 300, 60)

ENTRY_BUCKET_PCT = _dec("ENTRY_BUCKET_PCT", "70")
ADJUSTMENT_BUCKET_PCT = _dec("ADJUSTMENT_BUCKET_PCT", "30")

MIN_POSITION_VALUE_RIAL = _dec("MIN_POSITION_VALUE_TOMAN", "1000000") * TOMAN_TO_RIAL
MAX_POSITION_MARGIN_RIAL = _dec("MAX_POSITION_MARGIN_TOMAN", "5000000") * TOMAN_TO_RIAL
MIN_NET_PREMIUM_TO_MARGIN_PCT = _dec("MIN_NET_PREMIUM_TO_MARGIN_PCT", "10")

STRADDLE_STRESS_PCT = _dec("STRADDLE_STRESS_PCT", "20")
STRANGLE_STRESS_PCT = _dec("STRANGLE_STRESS_PCT", "10")
MAX_LOSS_ZONE_DISTANCE_PCT = _dec("MAX_LOSS_ZONE_DISTANCE_PCT", "5")

MARGIN_A_PCT = _dec("MARGIN_A_PCT", "20")
MARGIN_B_PCT = _dec("MARGIN_B_PCT", "10")
MARGIN_ROUNDING_RIAL = _dec("MARGIN_ROUNDING_RIAL", "10000")

OPTION_SELL_FEE_PCT = _dec("OPTION_SELL_FEE_PCT", "0.103")
OPTION_BUY_FEE_PCT = _dec("OPTION_BUY_FEE_PCT", "0.103")

QUOTE_MAX_AGE_SECONDS = _int("QUOTE_MAX_AGE_SECONDS", 120, 1)
OPTION_VOLUME_MODE = _setting("OPTION_VOLUME_MODE", "auto").lower()
OPTION_VOLUME_AUTO_FALLBACK = _setting("OPTION_VOLUME_AUTO_FALLBACK", "contracts").lower()
SCHEDULED_EXIT_USE_LEVEL2 = _bool("SCHEDULED_EXIT_USE_LEVEL2", True)
MARKET_HOLIDAYS = _holiday_set(_setting("MARKET_HOLIDAYS", ""))

D0 = Decimal("0")
D1 = Decimal("1")
D100 = Decimal("100")
MONEY_Q = Decimal("0.01")
PRICE_Q = Decimal("0.0001")
PCT_Q = Decimal("0.000001")

ACCOUNT_BY_STRATEGY = {
    "SHORT_STRADDLE": STRADDLE_ACCOUNT_NAME,
    "SHORT_STRANGLE": STRANGLE_ACCOUNT_NAME,
}
SIGNAL_TABLE_BY_STRATEGY = {
    "SHORT_STRADDLE": "short_straddle_signals",
    "SHORT_STRANGLE": "short_strangle_signals",
}


def dec(value: Any, default: Decimal = D0) -> Decimal:
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def money(value: Any) -> Decimal:
    return dec(value).quantize(MONEY_Q, rounding=ROUND_HALF_UP)


def pct(value: Any) -> Decimal:
    return dec(value).quantize(PCT_Q, rounding=ROUND_HALF_UP)


def fee(gross: Decimal, rate_pct: Decimal) -> Decimal:
    return money(max(D0, gross) * max(D0, rate_pct) / D100)


def ceil_to(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return money(value)
    units = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return money(units * step)


def tehran_now() -> datetime:
    return datetime.now(TEHRAN_TZ).replace(tzinfo=None)


def is_market_day(d: date) -> bool:
    # Python weekday: Mon=0 ... Sat=5 Sun=6
    return d.weekday() in {0, 1, 2, 5, 6} and d not in MARKET_HOLIDAYS


def previous_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    while not is_market_day(x):
        x -= timedelta(days=1)
    return x


def in_market_window(now: datetime) -> bool:
    return is_market_day(now.date()) and MARKET_START <= now.time() <= MARKET_END


def can_open_new_entries(now: datetime) -> bool:
    return in_market_window(now) and now.time() < NEW_ENTRY_CUTOFF


def normalize_option_volume(raw_volume: int, contract_size: int) -> int:
    if raw_volume <= 0 or contract_size <= 0:
        return 0
    value = int(raw_volume)
    size = int(contract_size)
    mode = OPTION_VOLUME_MODE
    if mode == "auto":
        if value < size or value % size != 0:
            mode = "contracts"
        else:
            mode = OPTION_VOLUME_AUTO_FALLBACK
    if mode == "contracts":
        return value
    if mode == "units":
        return value // size
    raise RuntimeError("OPTION_VOLUME_MODE must be auto/contracts/units")


def validate_configuration() -> None:
    if not MYSQL_HOST or not MYSQL_DATABASE or not MYSQL_USER or not MYSQL_PASSWORD:
        raise RuntimeError("MySQL configuration is incomplete.")
    if MIN_DTE < 0 or MAX_DTE < MIN_DTE:
        raise RuntimeError("Invalid DTE range.")
    if ENTRY_BUCKET_PCT + ADJUSTMENT_BUCKET_PCT != D100:
        raise RuntimeError("Entry + Adjustment bucket percentages must equal 100.")
    if MIN_POSITION_VALUE_RIAL <= 0 or MAX_POSITION_MARGIN_RIAL <= 0:
        raise RuntimeError("Position value/margin limits must be positive.")
    if MARGIN_A_PCT <= 0 or MARGIN_B_PCT <= 0:
        raise RuntimeError("Margin parameters must be positive.")
    if OPTION_VOLUME_MODE not in {"auto", "contracts", "units"}:
        raise RuntimeError("Invalid OPTION_VOLUME_MODE.")
    if OPTION_VOLUME_AUTO_FALLBACK not in {"contracts", "units"}:
        raise RuntimeError("Invalid OPTION_VOLUME_AUTO_FALLBACK.")


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


class DB:
    def __init__(self, conn: aiomysql.Connection):
        self.conn = conn

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> int:
        async with self.conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return int(cur.rowcount)

    async def insert(self, sql: str, args: Sequence[Any] = ()) -> int:
        async with self.conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return int(cur.lastrowid)

    async def fetchone(self, sql: str, args: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
        async with self.conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def fetchall(self, sql: str, args: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        async with self.conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            return [dict(x) for x in await cur.fetchall()]


@dataclass(frozen=True)
class OptionQuote:
    ins_code: str
    ua_ins_code: str
    underlying_symbol: str
    option_type: str
    symbol: str
    strike: Decimal
    contract_size: int
    expiry: date
    dte: int
    bid: Decimal
    bid_volume_raw: int
    ask: Decimal
    ask_volume_raw: int
    tick_time: datetime

    @property
    def bid_capacity(self) -> int:
        return normalize_option_volume(self.bid_volume_raw, self.contract_size)

    @property
    def ask_capacity(self) -> int:
        return normalize_option_volume(self.ask_volume_raw, self.contract_size)


@dataclass
class EntryCandidate:
    strategy: str
    account_id: int
    ua_ins_code: str
    underlying_symbol: str
    expiry: date
    dte: int
    spot: Decimal
    put: OptionQuote
    call: OptionQuote
    desired_qty: int
    min_qty: int
    pair_margin_per_contract: Decimal
    net_premium_margin_pct: Decimal
    lower_be: Decimal
    upper_be: Decimal
    stress_down_spot: Decimal
    stress_up_spot: Decimal
    stress_down_pnl_per_pair: Decimal
    stress_up_pnl_per_pair: Decimal

    @property
    def yield_per_day(self) -> Decimal:
        return self.net_premium_margin_pct / Decimal(max(1, self.dte))

    @property
    def be_width_pct(self) -> Decimal:
        if self.spot <= 0:
            return D0
        return (self.upper_be - self.lower_be) / self.spot * D100


def quote_is_fresh(q: OptionQuote, now: datetime) -> bool:
    age = (now - q.tick_time).total_seconds()
    return 0 <= age <= QUOTE_MAX_AGE_SECONDS


def valid_quote(q: OptionQuote) -> bool:
    return q.bid > 0 and q.ask > 0 and q.ask >= q.bid


def short_margin_per_contract(q: OptionQuote, spot: Decimal) -> Decimal:
    """Configured official-formula paper approximation using current Best Ask as P."""
    n = Decimal(q.contract_size)
    if q.option_type == "CALL":
        otm_per_unit = max(q.strike - spot, D0)
    else:
        otm_per_unit = max(spot - q.strike, D0)
    risk_a = (MARGIN_A_PCT / D100) * spot * n - otm_per_unit * n
    risk_b = (MARGIN_B_PCT / D100) * q.strike * n
    risk_part = max(risk_a, risk_b, D0)
    raw = q.ask * n + risk_part
    return ceil_to(raw, MARGIN_ROUNDING_RIAL)


def sell_cash(q: OptionQuote, qty: int) -> Tuple[Decimal, Decimal, Decimal]:
    gross = money(q.bid * Decimal(q.contract_size) * Decimal(qty))
    f = fee(gross, OPTION_SELL_FEE_PCT)
    return gross, f, money(gross - f)


def buy_cash(price: Decimal, contract_size: int, qty: int) -> Tuple[Decimal, Decimal, Decimal]:
    gross = money(price * Decimal(contract_size) * Decimal(qty))
    f = fee(gross, OPTION_BUY_FEE_PCT)
    return gross, f, money(-(gross + f))


def terminal_pnl(
    net_cashflow: Decimal,
    open_legs: Sequence[Mapping[str, Any]],
    terminal_spot: Decimal,
) -> Decimal:
    result = net_cashflow
    for leg in open_legs:
        qty = int(leg.get("remaining_contracts") or leg.get("qty") or 0)
        if qty <= 0:
            continue
        cs = Decimal(int(leg.get("contract_size") or 0))
        strike = dec(leg.get("strike_price_rial") or leg.get("strike"))
        exposure = cs * Decimal(qty)
        if str(leg.get("option_type")) == "CALL":
            result -= max(terminal_spot - strike, D0) * exposure
        else:
            result -= max(strike - terminal_spot, D0) * exposure
    return money(result)


def full_position_breakevens(
    net_cashflow: Decimal,
    open_legs: Sequence[Mapping[str, Any]],
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    active = [x for x in open_legs if int(x.get("remaining_contracts") or x.get("qty") or 0) > 0]
    if not active:
        return None, None
    strikes = sorted({dec(x.get("strike_price_rial") or x.get("strike")) for x in active})
    points = [D0] + strikes
    roots: List[Decimal] = []

    for a, b in zip(points, points[1:]):
        fa = terminal_pnl(net_cashflow, active, a)
        fb = terminal_pnl(net_cashflow, active, b)
        if fa == 0:
            roots.append(a)
        if fb == 0:
            roots.append(b)
        if fa * fb < 0 and b > a:
            root = a + (D0 - fa) * (b - a) / (fb - fa)
            roots.append(root)

    last = strikes[-1]
    f_last = terminal_pnl(net_cashflow, active, last)
    call_exposure = sum(
        Decimal(int(x.get("contract_size") or 0))
        * Decimal(int(x.get("remaining_contracts") or x.get("qty") or 0))
        for x in active
        if str(x.get("option_type")) == "CALL"
    )
    if f_last > 0 and call_exposure > 0:
        roots.append(last + f_last / call_exposure)
    elif f_last == 0:
        roots.append(last)

    unique = sorted({r.quantize(PRICE_Q, rounding=ROUND_HALF_UP) for r in roots if r >= 0})
    if not unique:
        return None, None
    if len(unique) == 1:
        return unique[0], unique[0]
    return unique[0], unique[-1]


def loss_distance(
    spot: Decimal,
    lower_be: Optional[Decimal],
    upper_be: Optional[Decimal],
) -> Tuple[Optional[str], Decimal]:
    if upper_be and upper_be > 0 and spot > upper_be:
        return "CALL", pct((spot - upper_be) / upper_be * D100)
    if lower_be and lower_be > 0 and spot < lower_be:
        return "PUT", pct((lower_be - spot) / lower_be * D100)
    return None, D0


class ShortPremiumEngine:
    REQUIRED_TABLES = (
        "underlying_assets",
        "option_contracts",
        "market_data_ticks",
        "order_book_depth",
        "short_premium_accounts",
        "short_premium_positions",
        "short_premium_legs",
        "short_premium_fills",
        "short_premium_valuations",
        "short_premium_events",
        "short_premium_engine_runs",
        "short_straddle_signals",
        "short_strangle_signals",
    )

    def __init__(self) -> None:
        self.pool: Optional[aiomysql.Pool] = None
        self.shutdown = asyncio.Event()

    async def open(self) -> None:
        validate_configuration()
        self.pool = await aiomysql.create_pool(
            minsize=DB_POOL_MIN,
            maxsize=DB_POOL_MAX,
            **connection_kwargs(),
        )
        await self._verify_schema()
        await self._ensure_accounts()

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _with_db(self):
        if self.pool is None:
            raise RuntimeError("Engine not open")
        return self.pool.acquire()

    async def _verify_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            db = DB(conn)
            for table in self.REQUIRED_TABLES:
                row = await db.fetchone(
                    "SELECT COUNT(*) AS n FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                    (MYSQL_DATABASE, table),
                )
                if not row or int(row["n"]) != 1:
                    raise RuntimeError(
                        f"Missing required table {table}. Run database/02_create_short_premium_isolated.sql"
                    )

    async def _ensure_accounts(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            db = DB(conn)
            try:
                for strategy, name in ACCOUNT_BY_STRATEGY.items():
                    await db.execute(
                        """
                        INSERT INTO short_premium_accounts (
                            account_name,strategy_code,initial_equity_rial,current_equity_rial,
                            entry_bucket_target_rial,adjustment_bucket_target_rial,high_watermark_rial
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE account_name=VALUES(account_name)
                        """,
                        (
                            name,
                            strategy,
                            money(INITIAL_CAPITAL_RIAL),
                            money(INITIAL_CAPITAL_RIAL),
                            money(INITIAL_CAPITAL_RIAL * ENTRY_BUCKET_PCT / D100),
                            money(INITIAL_CAPITAL_RIAL * ADJUSTMENT_BUCKET_PCT / D100),
                            money(INITIAL_CAPITAL_RIAL),
                        ),
                    )
                    row = await db.fetchone(
                        "SELECT initial_equity_rial,strategy_code FROM short_premium_accounts "
                        "WHERE account_name=%s",
                        (name,),
                    )
                    if not row:
                        raise RuntimeError(f"Could not initialize account {name}")
                    if money(row["initial_equity_rial"]) != money(INITIAL_CAPITAL_RIAL):
                        raise RuntimeError(
                            f"Existing {name} has a different initial capital; use a new account name."
                        )
                    if str(row["strategy_code"]) != strategy:
                        raise RuntimeError(f"Account {name} strategy mismatch")
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

    async def _accounts(self, db: DB) -> Dict[str, Dict[str, Any]]:
        rows = await db.fetchall(
            "SELECT * FROM short_premium_accounts WHERE strategy_code IN "
            "('SHORT_STRADDLE','SHORT_STRANGLE')"
        )
        return {str(x["strategy_code"]): x for x in rows}

    async def _load_chain(
        self,
        db: DB,
        now: datetime,
        ua_ins_code: str,
        underlying_symbol: str,
        expiry: date,
    ) -> List[OptionQuote]:
        rows = await db.fetchall(
            """
            SELECT c.ins_code,c.ua_ins_code,c.contract_type,c.short_symbol,c.strike_price,
                   c.contract_size,c.end_date,c.remained_days,
                   m.bid_price,m.bid_volume,m.ask_price,m.ask_volume,m.tick_time
            FROM option_contracts c
            JOIN market_data_ticks m ON m.ins_code=c.ins_code
            WHERE c.ua_ins_code=%s AND c.end_date=%s
            ORDER BY c.strike_price,c.contract_type
            """,
            (ua_ins_code, expiry),
        )
        out: List[OptionQuote] = []
        dte = max(0, (expiry - now.date()).days)
        for r in rows:
            tt = r.get("tick_time")
            if not isinstance(tt, datetime):
                continue
            out.append(
                OptionQuote(
                    ins_code=str(r["ins_code"]),
                    ua_ins_code=str(r["ua_ins_code"]),
                    underlying_symbol=underlying_symbol,
                    option_type=str(r["contract_type"]),
                    symbol=str(r.get("short_symbol") or ""),
                    strike=dec(r["strike_price"]),
                    contract_size=int(r["contract_size"] or 0),
                    expiry=expiry,
                    dte=dte,
                    bid=dec(r.get("bid_price")),
                    bid_volume_raw=int(r.get("bid_volume") or 0),
                    ask=dec(r.get("ask_price")),
                    ask_volume_raw=int(r.get("ask_volume") or 0),
                    tick_time=tt,
                )
            )
        return out

    async def _universe(self, db: DB, now: datetime) -> List[Dict[str, Any]]:
        if not ALLOWED_UNDERLYINGS:
            return []
        placeholders = ",".join(["%s"] * len(ALLOWED_UNDERLYINGS))
        rows = await db.fetchall(
            f"""
            SELECT ua.ua_ins_code,ua.symbol,
                   COALESCE(NULLIF(ua.last_trade_price,0),ua.closing_price) AS spot
            FROM underlying_assets ua
            WHERE ua.symbol IN ({placeholders})
            """,
            ALLOWED_UNDERLYINGS,
        )
        out: List[Dict[str, Any]] = []
        for u in rows:
            spot = dec(u.get("spot"))
            if spot <= 0:
                continue
            expiries = await db.fetchall(
                """
                SELECT DISTINCT end_date
                FROM option_contracts
                WHERE ua_ins_code=%s
                  AND DATEDIFF(end_date,%s) BETWEEN %s AND %s
                ORDER BY end_date
                """,
                (u["ua_ins_code"], now.date(), MIN_DTE, MAX_DTE),
            )
            for e in expiries:
                exp = e.get("end_date")
                if isinstance(exp, date):
                    out.append(
                        {
                            "ua_ins_code": str(u["ua_ins_code"]),
                            "underlying_symbol": str(u.get("symbol") or ""),
                            "spot": spot,
                            "expiry": exp,
                            "dte": (exp - now.date()).days,
                        }
                    )
        return out

    async def _has_open_position(
        self, db: DB, account_id: int, ua_ins_code: str, expiry: date
    ) -> bool:
        row = await db.fetchone(
            """
            SELECT position_id FROM short_premium_positions
            WHERE account_id=%s AND ua_ins_code=%s AND expiry_date=%s
              AND status IN ('OPEN','EXITING')
            LIMIT 1
            """,
            (account_id, ua_ins_code, expiry),
        )
        return bool(row)

    async def _forced_exit_today(
        self, db: DB, account_id: int, ua_ins_code: str, expiry: date, today: date
    ) -> bool:
        row = await db.fetchone(
            """
            SELECT position_id FROM short_premium_positions
            WHERE account_id=%s AND ua_ins_code=%s AND expiry_date=%s
              AND forced_exit_date=%s
            LIMIT 1
            """,
            (account_id, ua_ins_code, expiry, today),
        )
        return bool(row)

    def _select_initial_legs(
        self,
        strategy: str,
        chain: Sequence[OptionQuote],
        spot: Decimal,
    ) -> Tuple[Optional[OptionQuote], Optional[OptionQuote], str]:
        calls = [q for q in chain if q.option_type == "CALL"]
        puts = [q for q in chain if q.option_type == "PUT"]
        if strategy == "SHORT_STRADDLE":
            all_strikes = sorted({q.strike for q in chain})
            if not all_strikes:
                return None, None, "NO_STRIKES"
            target = min(all_strikes, key=lambda k: (abs(k - spot), k))
            put = next((q for q in puts if q.strike == target), None)
            call = next((q for q in calls if q.strike == target), None)
            if not put or not call:
                return None, None, "ATM_PAIR_MISSING_NO_FALLBACK"
            return put, call, "OK"

        put_candidates = [q for q in puts if q.strike < spot]
        call_candidates = [q for q in calls if q.strike > spot]
        if not put_candidates or not call_candidates:
            return None, None, "STRICT_OTM_PAIR_MISSING_NO_FALLBACK"
        put = max(put_candidates, key=lambda q: q.strike)
        call = min(call_candidates, key=lambda q: q.strike)
        return put, call, "OK"

    def _entry_candidate(
        self,
        strategy: str,
        account_id: int,
        u: Mapping[str, Any],
        put: OptionQuote,
        call: OptionQuote,
        now: datetime,
    ) -> Tuple[Optional[EntryCandidate], str]:
        spot = dec(u["spot"])
        if not valid_quote(put) or not valid_quote(call):
            return None, "INVALID_QUOTE"
        if not quote_is_fresh(put, now) or not quote_is_fresh(call, now):
            return None, "STALE_QUOTE"
        if put.bid_capacity <= 0 or call.bid_capacity <= 0:
            return None, "NO_BEST_BID_VOLUME"

        pair_capacity = min(put.bid_capacity, call.bid_capacity)
        put_margin = short_margin_per_contract(put, spot)
        call_margin = short_margin_per_contract(call, spot)
        pair_margin = put_margin + call_margin
        if pair_margin <= 0:
            return None, "INVALID_MARGIN"

        max_qty_margin = int(MAX_POSITION_MARGIN_RIAL // pair_margin)
        desired_qty = min(pair_capacity, max_qty_margin)
        if desired_qty <= 0:
            return None, "ONE_PAIR_EXCEEDS_MARGIN_CAP"

        gross_pair, fee_pair, net_pair = D0, D0, D0
        for q in (put, call):
            gross, f, net = sell_cash(q, 1)
            gross_pair += gross
            fee_pair += f
            net_pair += net

        if gross_pair <= 0:
            return None, "INVALID_PREMIUM"
        min_qty = int(
            (MIN_POSITION_VALUE_RIAL / gross_pair).to_integral_value(rounding=ROUND_CEILING)
        )
        min_qty = max(1, min_qty)
        if desired_qty < min_qty:
            return None, "MIN_POSITION_VALUE_NOT_EXECUTABLE"

        ratio = pct(net_pair / pair_margin * D100)
        if ratio < MIN_NET_PREMIUM_TO_MARGIN_PCT:
            return None, "NET_PREMIUM_MARGIN_BELOW_MINIMUM"

        synthetic_legs = [
            {
                "option_type": "PUT",
                "strike": put.strike,
                "contract_size": put.contract_size,
                "qty": 1,
            },
            {
                "option_type": "CALL",
                "strike": call.strike,
                "contract_size": call.contract_size,
                "qty": 1,
            },
        ]
        lower, upper = full_position_breakevens(net_pair, synthetic_legs)
        if lower is None or upper is None:
            return None, "BREAKEVEN_UNAVAILABLE"

        stress_pct = STRADDLE_STRESS_PCT if strategy == "SHORT_STRADDLE" else STRANGLE_STRESS_PCT
        stress_down = spot * (D1 - stress_pct / D100)
        stress_up = spot * (D1 + stress_pct / D100)
        pnl_down = terminal_pnl(net_pair, synthetic_legs, stress_down)
        pnl_up = terminal_pnl(net_pair, synthetic_legs, stress_up)
        if pnl_down < 0 or pnl_up < 0:
            return None, "STRESS_FILTER_FAILED"

        return (
            EntryCandidate(
                strategy=strategy,
                account_id=account_id,
                ua_ins_code=str(u["ua_ins_code"]),
                underlying_symbol=str(u["underlying_symbol"]),
                expiry=u["expiry"],
                dte=int(u["dte"]),
                spot=spot,
                put=put,
                call=call,
                desired_qty=desired_qty,
                min_qty=min_qty,
                pair_margin_per_contract=pair_margin,
                net_premium_margin_pct=ratio,
                lower_be=lower,
                upper_be=upper,
                stress_down_spot=money(stress_down),
                stress_up_spot=money(stress_up),
                stress_down_pnl_per_pair=pnl_down,
                stress_up_pnl_per_pair=pnl_up,
            ),
            "OK",
        )

    async def _insert_signal(
        self,
        db: DB,
        strategy: str,
        now: datetime,
        account_id: int,
        u: Mapping[str, Any],
        put: Optional[OptionQuote],
        call: Optional[OptionQuote],
        decision: str,
        reason: str,
        requested_qty: int = 0,
        executable_qty: int = 0,
        opened_position_id: Optional[int] = None,
        metrics: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if put is None or call is None:
            return
        table = SIGNAL_TABLE_BY_STRATEGY[strategy]
        m = dict(metrics or {})
        await db.execute(
            f"""
            INSERT INTO {table} (
                scan_time,account_id,ua_ins_code,underlying_symbol,expiry_date,days_to_expiry,spot_rial,
                put_ins_code,put_symbol,put_strike_rial,put_bid_rial,put_bid_volume,
                call_ins_code,call_symbol,call_strike_rial,call_bid_rial,call_bid_volume,
                requested_qty,executable_qty,gross_premium_rial,sell_fees_rial,net_premium_rial,
                required_margin_rial,net_premium_margin_pct,position_value_rial,
                stress_down_spot_rial,stress_up_spot_rial,stress_down_pnl_rial,stress_up_pnl_rial,
                lower_breakeven_rial,upper_breakeven_rial,decision,reason_code,opened_position_id,details_json
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s
            )
            ON DUPLICATE KEY UPDATE
                requested_qty=VALUES(requested_qty),
                executable_qty=VALUES(executable_qty),
                gross_premium_rial=VALUES(gross_premium_rial),
                sell_fees_rial=VALUES(sell_fees_rial),
                net_premium_rial=VALUES(net_premium_rial),
                required_margin_rial=VALUES(required_margin_rial),
                net_premium_margin_pct=VALUES(net_premium_margin_pct),
                position_value_rial=VALUES(position_value_rial),
                decision=VALUES(decision),
                reason_code=VALUES(reason_code),
                opened_position_id=VALUES(opened_position_id),
                details_json=VALUES(details_json)
            """,
            (
                now, account_id, u["ua_ins_code"], u["underlying_symbol"], u["expiry"], u["dte"], u["spot"],
                put.ins_code, put.symbol, put.strike, put.bid, put.bid_volume_raw,
                call.ins_code, call.symbol, call.strike, call.bid, call.bid_volume_raw,
                requested_qty, executable_qty,
                money(m.get("gross_premium_rial")), money(m.get("sell_fees_rial")),
                money(m.get("net_premium_rial")), money(m.get("required_margin_rial")),
                pct(m.get("net_premium_margin_pct")), money(m.get("position_value_rial")),
                m.get("stress_down_spot_rial"), m.get("stress_up_spot_rial"),
                m.get("stress_down_pnl_rial"), m.get("stress_up_pnl_rial"),
                m.get("lower_be_rial"), m.get("upper_be_rial"),
                decision, reason, opened_position_id,
                json.dumps(m.get("details", {}), ensure_ascii=False),
            ),
        )

    async def _open_position(
        self,
        db: DB,
        c: EntryCandidate,
        qty: int,
        now: datetime,
    ) -> int:
        put_gross, put_fee, put_net = sell_cash(c.put, qty)
        call_gross, call_fee, call_net = sell_cash(c.call, qty)
        gross = put_gross + call_gross
        fees = put_fee + call_fee
        net = put_net + call_net
        margin = c.pair_margin_per_contract * Decimal(qty)

        position_id = await db.insert(
            """
            INSERT INTO short_premium_positions (
                account_id,strategy_code,ua_ins_code,underlying_symbol,expiry_date,status,
                initial_spot_rial,initial_qty,initial_gross_premium_rial,initial_net_premium_rial,
                initial_fees_rial,entry_margin_allocated_rial,current_margin_rial,
                cumulative_sell_gross_rial,total_fees_rial,cumulative_net_cashflow_rial,
                lower_breakeven_rial,upper_breakeven_rial,current_underlying_price_rial,opened_at
            ) VALUES (
                %s,%s,%s,%s,%s,'OPEN',
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                c.account_id, c.strategy, c.ua_ins_code, c.underlying_symbol, c.expiry,
                c.spot, qty, gross, net, fees, margin, margin, gross, fees, net,
                c.lower_be, c.upper_be, c.spot, now,
            ),
        )

        legs: List[Tuple[int, OptionQuote, Decimal, Decimal, Decimal]] = []
        for opt, g, f, n in (
            (c.put, put_gross, put_fee, put_net),
            (c.call, call_gross, call_fee, call_net),
        ):
            leg_margin = short_margin_per_contract(opt, c.spot) * Decimal(qty)
            leg_id = await db.insert(
                """
                INSERT INTO short_premium_legs (
                    position_id,role,option_type,ins_code,symbol,strike_price_rial,expiry_date,
                    contract_size,opened_contracts,remaining_contracts,entry_price_rial,
                    entry_gross_rial,entry_fee_rial,entry_net_credit_rial,margin_allocated_rial,opened_at
                ) VALUES (%s,'INITIAL',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    position_id, opt.option_type, opt.ins_code, opt.symbol, opt.strike, opt.expiry,
                    opt.contract_size, qty, qty, opt.bid, g, f, n, leg_margin, now,
                ),
            )
            await db.insert(
                """
                INSERT INTO short_premium_fills (
                    position_id,leg_id,action,reason,book_level,contract_count,price_rial,
                    gross_value_rial,fee_rial,net_cashflow_rial,fill_time
                ) VALUES (%s,%s,'SELL','ENTRY',1,%s,%s,%s,%s,%s,%s)
                """,
                (position_id, leg_id, qty, opt.bid, g, f, n, now),
            )
            legs.append((leg_id, opt, g, f, n))

        open_legs = [
            {
                "option_type": c.put.option_type,
                "strike_price_rial": c.put.strike,
                "contract_size": c.put.contract_size,
                "remaining_contracts": qty,
            },
            {
                "option_type": c.call.option_type,
                "strike_price_rial": c.call.strike,
                "contract_size": c.call.contract_size,
                "remaining_contracts": qty,
            },
        ]
        lower, upper = full_position_breakevens(net, open_legs)
        await db.execute(
            "UPDATE short_premium_positions SET lower_breakeven_rial=%s,upper_breakeven_rial=%s "
            "WHERE position_id=%s",
            (lower, upper, position_id),
        )

        await self._event(
            db,
            c.account_id,
            position_id,
            c.strategy,
            "ENTRY",
            now,
            {
                "underlying": c.underlying_symbol,
                "expiry": c.expiry.isoformat(),
                "spot_rial": str(c.spot),
                "qty": qty,
                "put_symbol": c.put.symbol,
                "put_strike_rial": str(c.put.strike),
                "put_bid_rial": str(c.put.bid),
                "call_symbol": c.call.symbol,
                "call_strike_rial": str(c.call.strike),
                "call_bid_rial": str(c.call.bid),
                "gross_premium_rial": str(gross),
                "net_premium_rial": str(net),
                "fees_rial": str(fees),
                "margin_rial": str(margin),
                "net_premium_margin_pct": str(c.net_premium_margin_pct),
                "lower_be_rial": str(lower or D0),
                "upper_be_rial": str(upper or D0),
                "stress_pct": str(
                    STRADDLE_STRESS_PCT if c.strategy == "SHORT_STRADDLE" else STRANGLE_STRESS_PCT
                ),
            },
        )
        return position_id

    async def _event(
        self,
        db: DB,
        account_id: int,
        position_id: Optional[int],
        strategy: str,
        event_type: str,
        when: datetime,
        details: Mapping[str, Any],
    ) -> None:
        await db.insert(
            """
            INSERT INTO short_premium_events (
                account_id,position_id,strategy_code,event_type,event_time,details_json
            ) VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                account_id,
                position_id,
                strategy,
                event_type,
                when,
                json.dumps(dict(details), ensure_ascii=False, default=str),
            ),
        )

    async def _position_legs(self, db: DB, position_id: int) -> List[Dict[str, Any]]:
        return await db.fetchall(
            """
            SELECT * FROM short_premium_legs
            WHERE position_id=%s
            ORDER BY leg_id
            """,
            (position_id,),
        )

    async def _quote_for_ins(
        self, db: DB, ins_code: str, underlying_symbol: str, expiry: date, now: datetime
    ) -> Optional[OptionQuote]:
        row = await db.fetchone(
            """
            SELECT c.ins_code,c.ua_ins_code,c.contract_type,c.short_symbol,c.strike_price,
                   c.contract_size,c.end_date,m.bid_price,m.bid_volume,m.ask_price,m.ask_volume,m.tick_time
            FROM option_contracts c
            JOIN market_data_ticks m ON m.ins_code=c.ins_code
            WHERE c.ins_code=%s
            """,
            (ins_code,),
        )
        if not row or not isinstance(row.get("tick_time"), datetime):
            return None
        return OptionQuote(
            ins_code=str(row["ins_code"]),
            ua_ins_code=str(row["ua_ins_code"]),
            underlying_symbol=underlying_symbol,
            option_type=str(row["contract_type"]),
            symbol=str(row.get("short_symbol") or ""),
            strike=dec(row["strike_price"]),
            contract_size=int(row["contract_size"] or 0),
            expiry=expiry,
            dte=max(0, (expiry - now.date()).days),
            bid=dec(row.get("bid_price")),
            bid_volume_raw=int(row.get("bid_volume") or 0),
            ask=dec(row.get("ask_price")),
            ask_volume_raw=int(row.get("ask_volume") or 0),
            tick_time=row["tick_time"],
        )

    async def _spot(self, db: DB, ua_ins_code: str) -> Decimal:
        row = await db.fetchone(
            """
            SELECT COALESCE(NULLIF(last_trade_price,0),closing_price) AS spot
            FROM underlying_assets WHERE ua_ins_code=%s
            """,
            (ua_ins_code,),
        )
        return dec(row.get("spot") if row else None)

    async def _revalue_position(
        self, db: DB, p: Mapping[str, Any], now: datetime, persist: bool = True
    ) -> Dict[str, Any]:
        spot = await self._spot(db, str(p["ua_ins_code"]))
        legs = await self._position_legs(db, int(p["position_id"]))
        net_cashflow = dec(p.get("cumulative_net_cashflow_rial"))
        gross_sell = dec(p.get("cumulative_sell_gross_rial"))
        gross_buy = dec(p.get("cumulative_buy_gross_rial"))
        close_cost = D0
        mark_buy_fees = D0
        current_margin = D0
        all_marks_valid = True

        for leg in legs:
            remaining = int(leg.get("remaining_contracts") or 0)
            if remaining <= 0:
                continue
            q = await self._quote_for_ins(
                db, str(leg["ins_code"]), str(p["underlying_symbol"]), p["expiry_date"], now
            )
            if not q or not valid_quote(q) or not quote_is_fresh(q, now):
                all_marks_valid = False
                continue
            g, f, _ = buy_cash(q.ask, q.contract_size, remaining)
            close_cost += g + f
            mark_buy_fees += f
            current_margin += short_margin_per_contract(q, spot) * Decimal(remaining)

        lower, upper = full_position_breakevens(net_cashflow, legs)
        side, ld = loss_distance(spot, lower, upper)
        gross_pnl = gross_sell - gross_buy - max(D0, close_cost - mark_buy_fees)
        net_pnl = net_cashflow - close_cost

        state = {
            "spot": spot,
            "legs": legs,
            "lower_be": lower,
            "upper_be": upper,
            "loss_side": side,
            "loss_distance_pct": ld,
            "current_margin": money(current_margin),
            "close_cost": money(close_cost),
            "gross_pnl": money(gross_pnl),
            "net_pnl": money(net_pnl),
            "marks_valid": all_marks_valid,
        }
        if persist:
            await db.execute(
                """
                UPDATE short_premium_positions
                SET current_margin_rial=%s,lower_breakeven_rial=%s,upper_breakeven_rial=%s,
                    current_underlying_price_rial=%s,current_close_cost_rial=%s,
                    gross_pnl_rial=%s,net_pnl_rial=%s,last_valued_at=%s
                WHERE position_id=%s
                """,
                (
                    state["current_margin"], lower, upper, spot, state["close_cost"],
                    state["gross_pnl"], state["net_pnl"], now, p["position_id"],
                ),
            )
            await db.execute(
                """
                INSERT INTO short_premium_valuations (
                    position_id,valuation_time,spot_rial,lower_breakeven_rial,upper_breakeven_rial,
                    loss_distance_pct,current_margin_rial,gross_pnl_rial,net_pnl_rial,close_cost_rial,
                    entry_capital_used_rial,adjustment_capital_used_rial,state_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    spot_rial=VALUES(spot_rial),
                    lower_breakeven_rial=VALUES(lower_breakeven_rial),
                    upper_breakeven_rial=VALUES(upper_breakeven_rial),
                    loss_distance_pct=VALUES(loss_distance_pct),
                    current_margin_rial=VALUES(current_margin_rial),
                    gross_pnl_rial=VALUES(gross_pnl_rial),
                    net_pnl_rial=VALUES(net_pnl_rial),
                    close_cost_rial=VALUES(close_cost_rial),
                    state_json=VALUES(state_json)
                """,
                (
                    p["position_id"], now, spot, lower, upper, ld, state["current_margin"],
                    state["gross_pnl"], state["net_pnl"], state["close_cost"],
                    p["entry_margin_allocated_rial"], p["adjustment_margin_allocated_rial"],
                    json.dumps(
                        {"loss_side": side, "marks_valid": all_marks_valid},
                        ensure_ascii=False,
                    ),
                ),
            )
        return state

    async def _refresh_account(self, db: DB, strategy: str, now: datetime) -> Dict[str, Any]:
        name = ACCOUNT_BY_STRATEGY[strategy]
        account = await db.fetchone(
            "SELECT * FROM short_premium_accounts WHERE account_name=%s FOR UPDATE",
            (name,),
        )
        if not account:
            raise RuntimeError(f"Missing account {name}")
        account_id = int(account["account_id"])
        sums = await db.fetchone(
            """
            SELECT
              COALESCE(SUM(CASE WHEN status='CLOSED' THEN net_pnl_rial ELSE 0 END),0) AS realized,
              COALESCE(SUM(CASE WHEN status<>'CLOSED' THEN net_pnl_rial ELSE 0 END),0) AS unrealized,
              COALESCE(SUM(CASE WHEN status<>'CLOSED' THEN entry_margin_allocated_rial ELSE 0 END),0) AS entry_used,
              COALESCE(SUM(CASE WHEN status<>'CLOSED' THEN adjustment_margin_allocated_rial ELSE 0 END),0) AS adjustment_used,
              SUM(CASE WHEN status<>'CLOSED' THEN 1 ELSE 0 END) AS open_count
            FROM short_premium_positions
            WHERE account_id=%s
            """,
            (account_id,),
        )
        realized = dec(sums.get("realized") if sums else 0)
        unrealized = dec(sums.get("unrealized") if sums else 0)
        realized_equity = INITIAL_CAPITAL_RIAL + realized
        current_equity = realized_equity + unrealized
        entry_target = max(D0, realized_equity * ENTRY_BUCKET_PCT / D100)
        adjustment_target = max(D0, realized_equity * ADJUSTMENT_BUCKET_PCT / D100)
        old_hwm = dec(account.get("high_watermark_rial"), INITIAL_CAPITAL_RIAL)
        hwm = max(old_hwm, current_equity)
        dd = D0 if hwm <= 0 else max(D0, (hwm - current_equity) / hwm * D100)
        await db.execute(
            """
            UPDATE short_premium_accounts
            SET realized_pnl_rial=%s,unrealized_pnl_rial=%s,current_equity_rial=%s,
                entry_bucket_target_rial=%s,adjustment_bucket_target_rial=%s,
                entry_capital_used_rial=%s,adjustment_capital_used_rial=%s,
                open_positions_count=%s,high_watermark_rial=%s,drawdown_pct=%s,last_scan_at=%s
            WHERE account_id=%s
            """,
            (
                money(realized), money(unrealized), money(current_equity),
                money(entry_target), money(adjustment_target),
                money(sums.get("entry_used") if sums else 0),
                money(sums.get("adjustment_used") if sums else 0),
                int(sums.get("open_count") or 0) if sums else 0,
                money(hwm), pct(dd), now, account_id,
            ),
        )
        return (await db.fetchone(
            "SELECT * FROM short_premium_accounts WHERE account_id=%s",
            (account_id,),
        )) or account

    async def _start_exit(
        self,
        db: DB,
        p: Mapping[str, Any],
        mode: str,
        now: datetime,
        reason: str,
    ) -> None:
        if str(p["status"]) == "EXITING":
            return
        forced_date = now.date() if mode == "FORCED" else None
        await db.execute(
            """
            UPDATE short_premium_positions
            SET status='EXITING',exit_mode=%s,exit_started_at=%s,
                forced_exit_date=%s,close_reason=%s
            WHERE position_id=%s
            """,
            (mode, now, forced_date, reason, p["position_id"]),
        )
        await self._event(
            db,
            int(p["account_id"]),
            int(p["position_id"]),
            str(p["strategy_code"]),
            "FORCED_EXIT_START" if mode == "FORCED" else "SCHEDULED_EXIT_START",
            now,
            {
                "underlying": p["underlying_symbol"],
                "expiry": str(p["expiry_date"]),
                "reason": reason,
                "mode": mode,
            },
        )

    async def _level2_ask(
        self, db: DB, ins_code: str, now: datetime, contract_size: int
    ) -> Tuple[Decimal, int]:
        row = await db.fetchone(
            """
            SELECT ask_price,ask_volume,snapshot_time
            FROM order_book_depth
            WHERE ins_code=%s AND level=2
            """,
            (ins_code,),
        )
        if not row or not isinstance(row.get("snapshot_time"), datetime):
            return D0, 0
        age = (now - row["snapshot_time"]).total_seconds()
        if age < 0 or age > QUOTE_MAX_AGE_SECONDS:
            return D0, 0
        price = dec(row.get("ask_price"))
        raw = int(row.get("ask_volume") or 0)
        if price <= 0 or raw <= 0:
            return D0, 0
        return price, normalize_option_volume(raw, contract_size)

    async def _execute_exit_cycle(
        self, db: DB, p: Mapping[str, Any], now: datetime
    ) -> bool:
        scheduled = str(p.get("exit_mode") or "") == "SCHEDULED"
        legs = await self._position_legs(db, int(p["position_id"]))
        l1_remaining_by_ins: Dict[str, int] = {}
        l2_remaining_by_ins: Dict[str, int] = {}
        any_fill = False

        for leg in legs:
            remaining = int(leg.get("remaining_contracts") or 0)
            if remaining <= 0:
                continue
            q = await self._quote_for_ins(
                db, str(leg["ins_code"]), str(p["underlying_symbol"]), p["expiry_date"], now
            )
            if not q or not valid_quote(q) or not quote_is_fresh(q, now) or q.ask_capacity <= 0:
                continue

            if q.ins_code not in l1_remaining_by_ins:
                l1_remaining_by_ins[q.ins_code] = q.ask_capacity
            l1_cap = l1_remaining_by_ins[q.ins_code]
            qty1 = min(remaining, l1_cap)
            if qty1 > 0:
                await self._buy_to_close(db, p, leg, qty1, q.ask, 1, now)
                l1_remaining_by_ins[q.ins_code] -= qty1
                remaining -= qty1
                any_fill = True

            if (
                remaining > 0
                and scheduled
                and SCHEDULED_EXIT_USE_LEVEL2
            ):
                if q.ins_code not in l2_remaining_by_ins:
                    px2, cap2 = await self._level2_ask(db, q.ins_code, now, q.contract_size)
                    l2_remaining_by_ins[q.ins_code] = cap2
                else:
                    px2, _ = await self._level2_ask(db, q.ins_code, now, q.contract_size)
                cap2 = l2_remaining_by_ins.get(q.ins_code, 0)
                qty2 = min(remaining, cap2)
                if qty2 > 0 and px2 > 0:
                    await self._buy_to_close(db, p, leg, qty2, px2, 2, now)
                    l2_remaining_by_ins[q.ins_code] -= qty2
                    remaining -= qty2
                    any_fill = True

        remaining_row = await db.fetchone(
            "SELECT COALESCE(SUM(remaining_contracts),0) AS n "
            "FROM short_premium_legs WHERE position_id=%s",
            (p["position_id"],),
        )
        remaining_total = int(remaining_row.get("n") or 0) if remaining_row else 0
        if remaining_total == 0:
            await self._close_position(db, p, now)
            return True

        if any_fill:
            p2 = await db.fetchone(
                "SELECT * FROM short_premium_positions WHERE position_id=%s",
                (p["position_id"],),
            )
            if p2:
                state = await self._revalue_position(db, p2, now, persist=True)
                await self._event(
                    db,
                    int(p["account_id"]),
                    int(p["position_id"]),
                    str(p["strategy_code"]),
                    "PARTIAL_EXIT",
                    now,
                    {
                        "remaining_contracts": remaining_total,
                        "lower_be_rial": str(state.get("lower_be") or D0),
                        "upper_be_rial": str(state.get("upper_be") or D0),
                        "net_pnl_rial": str(state.get("net_pnl") or D0),
                    },
                )
        return False

    async def _buy_to_close(
        self,
        db: DB,
        p: Mapping[str, Any],
        leg: Mapping[str, Any],
        qty: int,
        price: Decimal,
        level: int,
        now: datetime,
    ) -> None:
        gross, f, net = buy_cash(price, int(leg["contract_size"]), qty)
        reason = "SCHEDULED_EXIT" if str(p.get("exit_mode")) == "SCHEDULED" else "FORCED_EXIT"
        await db.insert(
            """
            INSERT INTO short_premium_fills (
                position_id,leg_id,action,reason,book_level,contract_count,price_rial,
                gross_value_rial,fee_rial,net_cashflow_rial,fill_time
            ) VALUES (%s,%s,'BUY_TO_CLOSE',%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (p["position_id"], leg["leg_id"], reason, level, qty, price, gross, f, net, now),
        )
        new_remaining = max(0, int(leg["remaining_contracts"]) - qty)
        await db.execute(
            """
            UPDATE short_premium_legs
            SET remaining_contracts=%s,closed_at=CASE WHEN %s=0 THEN %s ELSE closed_at END
            WHERE leg_id=%s
            """,
            (new_remaining, new_remaining, now, leg["leg_id"]),
        )
        await db.execute(
            """
            UPDATE short_premium_positions
            SET cumulative_buy_gross_rial=cumulative_buy_gross_rial+%s,
                total_fees_rial=total_fees_rial+%s,
                cumulative_net_cashflow_rial=cumulative_net_cashflow_rial+%s
            WHERE position_id=%s
            """,
            (gross, f, net, p["position_id"]),
        )

    async def _close_position(self, db: DB, p: Mapping[str, Any], now: datetime) -> None:
        row = await db.fetchone(
            "SELECT * FROM short_premium_positions WHERE position_id=%s FOR UPDATE",
            (p["position_id"],),
        )
        if not row or str(row["status"]) == "CLOSED":
            return
        gross_pnl = dec(row["cumulative_sell_gross_rial"]) - dec(row["cumulative_buy_gross_rial"])
        net_pnl = dec(row["cumulative_net_cashflow_rial"])
        await db.execute(
            """
            UPDATE short_premium_positions
            SET status='CLOSED',closed_at=%s,current_margin_rial=0,current_close_cost_rial=0,
                gross_pnl_rial=%s,net_pnl_rial=%s,last_valued_at=%s
            WHERE position_id=%s
            """,
            (now, money(gross_pnl), money(net_pnl), now, row["position_id"]),
        )
        await self._event(
            db,
            int(row["account_id"]),
            int(row["position_id"]),
            str(row["strategy_code"]),
            "POSITION_CLOSED",
            now,
            {
                "underlying": row["underlying_symbol"],
                "expiry": str(row["expiry_date"]),
                "exit_mode": row.get("exit_mode"),
                "gross_pnl_rial": str(money(gross_pnl)),
                "net_pnl_rial": str(money(net_pnl)),
                "total_fees_rial": str(row["total_fees_rial"]),
            },
        )

    async def _adjust_position(
        self,
        db: DB,
        p: Mapping[str, Any],
        state: Mapping[str, Any],
        account: Mapping[str, Any],
        now: datetime,
    ) -> str:
        side = str(state.get("loss_side") or "")
        if side not in {"CALL", "PUT"}:
            return "NO_TRIGGER"
        if dec(state.get("loss_distance_pct")) < MAX_LOSS_ZONE_DISTANCE_PCT:
            return "NO_TRIGGER"

        chain = await self._load_chain(
            db,
            now,
            str(p["ua_ins_code"]),
            str(p["underlying_symbol"]),
            p["expiry_date"],
        )
        typed = [q for q in chain if q.option_type == side]
        fresh_valid = [
            q for q in typed
            if valid_quote(q) and quote_is_fresh(q, now) and q.bid_capacity > 0
        ]
        if not fresh_valid:
            return "WAIT_TRANSIENT_QUOTE"

        current_legs = list(state["legs"])
        cash = dec(p["cumulative_net_cashflow_rial"])
        spot = dec(state["spot"])
        target_qty = int(p["initial_qty"])
        adjustment_free = max(
            D0,
            dec(account["adjustment_bucket_target_rial"])
            - dec(account["adjustment_capital_used_rial"]),
        )

        restoration_candidates: List[Dict[str, Any]] = []
        executable_candidates: List[Dict[str, Any]] = []

        for q in fresh_valid:
            actual_qty = min(target_qty, q.bid_capacity)
            if actual_qty <= 0:
                continue
            gross, f, net = sell_cash(q, actual_qty)
            synthetic = {
                "option_type": q.option_type,
                "strike_price_rial": q.strike,
                "contract_size": q.contract_size,
                "remaining_contracts": actual_qty,
            }
            lower, upper = full_position_breakevens(cash + net, current_legs + [synthetic])
            _, new_ld = loss_distance(spot, lower, upper)
            if new_ld > MAX_LOSS_ZONE_DISTANCE_PCT:
                continue

            added_margin = short_margin_per_contract(q, spot) * Decimal(actual_qty)
            new_margin = dec(state["current_margin"]) + added_margin
            item = {
                "q": q,
                "qty": actual_qty,
                "gross": gross,
                "fee": f,
                "net": net,
                "lower": lower,
                "upper": upper,
                "new_ld": new_ld,
                "added_margin": added_margin,
                "new_margin": new_margin,
            }
            restoration_candidates.append(item)
            if new_margin <= MAX_POSITION_MARGIN_RIAL and added_margin <= adjustment_free:
                executable_candidates.append(item)

        if not restoration_candidates:
            await self._start_exit(
                db, p, "FORCED", now, "No adjustment strike/volume can restore loss distance to <=5%."
            )
            return "FORCED_NO_RESTORATION"

        if not executable_candidates:
            await self._start_exit(
                db, p, "FORCED", now, "Valid adjustment exists but margin cap/reserve blocks execution."
            )
            return "FORCED_MARGIN_OR_RESERVE"

        executable_candidates.sort(
            key=lambda x: (
                abs(dec(x["q"].strike) - spot),
                dec(x["q"].strike),
            )
        )
        x = executable_candidates[0]
        q: OptionQuote = x["q"]
        qty = int(x["qty"])

        leg_id = await db.insert(
            """
            INSERT INTO short_premium_legs (
                position_id,role,option_type,ins_code,symbol,strike_price_rial,expiry_date,
                contract_size,opened_contracts,remaining_contracts,entry_price_rial,
                entry_gross_rial,entry_fee_rial,entry_net_credit_rial,margin_allocated_rial,opened_at
            ) VALUES (%s,'ADJUSTMENT',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                p["position_id"], q.option_type, q.ins_code, q.symbol, q.strike, q.expiry,
                q.contract_size, qty, qty, q.bid, x["gross"], x["fee"], x["net"],
                x["added_margin"], now,
            ),
        )
        await db.insert(
            """
            INSERT INTO short_premium_fills (
                position_id,leg_id,action,reason,book_level,contract_count,price_rial,
                gross_value_rial,fee_rial,net_cashflow_rial,fill_time
            ) VALUES (%s,%s,'SELL','ADJUSTMENT',1,%s,%s,%s,%s,%s,%s)
            """,
            (
                p["position_id"], leg_id, qty, q.bid, x["gross"], x["fee"], x["net"], now,
            ),
        )
        await db.execute(
            """
            UPDATE short_premium_positions
            SET adjustment_margin_allocated_rial=adjustment_margin_allocated_rial+%s,
                cumulative_sell_gross_rial=cumulative_sell_gross_rial+%s,
                total_fees_rial=total_fees_rial+%s,
                cumulative_net_cashflow_rial=cumulative_net_cashflow_rial+%s,
                current_margin_rial=%s,lower_breakeven_rial=%s,upper_breakeven_rial=%s
            WHERE position_id=%s
            """,
            (
                x["added_margin"], x["gross"], x["fee"], x["net"], x["new_margin"],
                x["lower"], x["upper"], p["position_id"],
            ),
        )
        await self._event(
            db,
            int(p["account_id"]),
            int(p["position_id"]),
            str(p["strategy_code"]),
            "ADJUSTMENT",
            now,
            {
                "underlying": p["underlying_symbol"],
                "expiry": str(p["expiry_date"]),
                "side": side,
                "spot_rial": str(spot),
                "loss_distance_before_pct": str(state["loss_distance_pct"]),
                "symbol": q.symbol,
                "strike_rial": str(q.strike),
                "best_bid_rial": str(q.bid),
                "best_bid_volume": q.bid_volume_raw,
                "target_qty": target_qty,
                "executed_qty": qty,
                "gross_premium_rial": str(x["gross"]),
                "fees_rial": str(x["fee"]),
                "added_margin_rial": str(x["added_margin"]),
                "margin_after_rial": str(x["new_margin"]),
                "lower_be_after_rial": str(x["lower"] or D0),
                "upper_be_after_rial": str(x["upper"] or D0),
                "loss_distance_after_pct": str(x["new_ld"]),
            },
        )
        return "ADJUSTED"

    async def _manage_positions(
        self, db: DB, accounts: Dict[str, Dict[str, Any]], now: datetime
    ) -> Tuple[int, int]:
        adjusted = 0
        closed = 0

        positions = await db.fetchall(
            """
            SELECT * FROM short_premium_positions
            WHERE status IN ('OPEN','EXITING')
            ORDER BY position_id
            """
        )

        # 1) Scheduled exit state transition.
        for p in positions:
            if str(p["status"]) != "OPEN":
                continue
            if now.date() == previous_trading_day(p["expiry_date"]) and now.time() >= SCHEDULED_EXIT_TIME:
                await self._start_exit(
                    db, p, "SCHEDULED", now, "Previous trading day scheduled exit at/after 12:00."
                )

        # Reload after transitions.
        positions = await db.fetchall(
            "SELECT * FROM short_premium_positions WHERE status IN ('OPEN','EXITING') ORDER BY position_id"
        )

        # 2) Exit cycles first.
        for p in positions:
            if str(p["status"]) != "EXITING":
                continue
            # EXITING persists across days. We only attempt inside market window.
            if not in_market_window(now):
                continue
            if await self._execute_exit_cycle(db, p, now):
                closed += 1

        # 3) Revalue remaining OPEN positions and collect triggered adjustments.
        open_positions = await db.fetchall(
            "SELECT * FROM short_premium_positions WHERE status='OPEN' ORDER BY position_id"
        )
        triggered: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for p in open_positions:
            state = await self._revalue_position(db, p, now, persist=True)
            if (
                state["loss_side"]
                and state["loss_distance_pct"] >= MAX_LOSS_ZONE_DISTANCE_PCT
                and state["marks_valid"]
            ):
                triggered.append((p, state))

        # Highest current required margin first; tie -> greater loss distance.
        triggered.sort(
            key=lambda x: (dec(x[1]["current_margin"]), dec(x[1]["loss_distance_pct"])),
            reverse=True,
        )

        for p, state in triggered:
            strategy = str(p["strategy_code"])
            account = await self._refresh_account(db, strategy, now)
            result = await self._adjust_position(db, p, state, account, now)
            if result == "ADJUSTED":
                adjusted += 1
                p2 = await db.fetchone(
                    "SELECT * FROM short_premium_positions WHERE position_id=%s",
                    (p["position_id"],),
                )
                if p2:
                    await self._revalue_position(db, p2, now, persist=True)

        return adjusted, closed

    async def _entry_candidates(
        self,
        db: DB,
        strategy: str,
        account: Mapping[str, Any],
        now: datetime,
    ) -> List[EntryCandidate]:
        out: List[EntryCandidate] = []
        universe = await self._universe(db, now)
        for u in universe:
            if await self._has_open_position(
                db, int(account["account_id"]), str(u["ua_ins_code"]), u["expiry"]
            ):
                continue
            if await self._forced_exit_today(
                db,
                int(account["account_id"]),
                str(u["ua_ins_code"]),
                u["expiry"],
                now.date(),
            ):
                continue

            chain = await self._load_chain(
                db, now, str(u["ua_ins_code"]), str(u["underlying_symbol"]), u["expiry"]
            )
            put, call, select_reason = self._select_initial_legs(strategy, chain, dec(u["spot"]))
            if not put or not call:
                continue
            candidate, reason = self._entry_candidate(
                strategy, int(account["account_id"]), u, put, call, now
            )
            if not candidate:
                await self._insert_signal(
                    db, strategy, now, int(account["account_id"]), u,
                    put, call, "REJECTED", reason or select_reason,
                )
                continue
            out.append(candidate)

        out.sort(
            key=lambda c: (
                c.yield_per_day,
                c.be_width_pct,
                -c.pair_margin_per_contract,
            ),
            reverse=True,
        )
        return out

    async def _execute_entries(
        self, db: DB, strategy: str, account: Dict[str, Any], now: datetime
    ) -> int:
        if not can_open_new_entries(now):
            return 0
        opened = 0
        candidates = await self._entry_candidates(db, strategy, account, now)
        for c in candidates:
            account = await self._refresh_account(db, strategy, now)
            entry_free = max(
                D0,
                dec(account["entry_bucket_target_rial"])
                - dec(account["entry_capital_used_rial"]),
            )
            qty_by_bucket = int(entry_free // c.pair_margin_per_contract)
            qty = min(c.desired_qty, qty_by_bucket)
            if qty < c.min_qty:
                u = {
                    "ua_ins_code": c.ua_ins_code,
                    "underlying_symbol": c.underlying_symbol,
                    "expiry": c.expiry,
                    "dte": c.dte,
                    "spot": c.spot,
                }
                await self._insert_signal(
                    db, strategy, now, c.account_id, u, c.put, c.call,
                    "REJECTED", "ENTRY_BUCKET_INSUFFICIENT",
                    requested_qty=c.desired_qty,
                    executable_qty=max(0, qty),
                )
                continue

            put_gross, put_fee, put_net = sell_cash(c.put, qty)
            call_gross, call_fee, call_net = sell_cash(c.call, qty)
            gross = put_gross + call_gross
            fees = put_fee + call_fee
            net = put_net + call_net
            required_margin = c.pair_margin_per_contract * Decimal(qty)
            ratio = pct(net / required_margin * D100) if required_margin > 0 else D0
            open_legs = [
                {
                    "option_type": "PUT",
                    "strike": c.put.strike,
                    "contract_size": c.put.contract_size,
                    "qty": qty,
                },
                {
                    "option_type": "CALL",
                    "strike": c.call.strike,
                    "contract_size": c.call.contract_size,
                    "qty": qty,
                },
            ]
            lower, upper = full_position_breakevens(net, open_legs)
            position_id = await self._open_position(db, c, qty, now)
            opened += 1

            u = {
                "ua_ins_code": c.ua_ins_code,
                "underlying_symbol": c.underlying_symbol,
                "expiry": c.expiry,
                "dte": c.dte,
                "spot": c.spot,
            }
            await self._insert_signal(
                db,
                strategy,
                now,
                c.account_id,
                u,
                c.put,
                c.call,
                "EXECUTED",
                "POSITION_OPENED",
                requested_qty=c.desired_qty,
                executable_qty=qty,
                opened_position_id=position_id,
                metrics={
                    "gross_premium_rial": gross,
                    "sell_fees_rial": fees,
                    "net_premium_rial": net,
                    "required_margin_rial": required_margin,
                    "net_premium_margin_pct": ratio,
                    "position_value_rial": gross,
                    "stress_down_spot_rial": c.stress_down_spot,
                    "stress_up_spot_rial": c.stress_up_spot,
                    "stress_down_pnl_rial": c.stress_down_pnl_per_pair * Decimal(qty),
                    "stress_up_pnl_rial": c.stress_up_pnl_per_pair * Decimal(qty),
                    "lower_be_rial": lower,
                    "upper_be_rial": upper,
                    "details": {
                        "yield_per_day": str(c.yield_per_day),
                        "breakeven_width_pct": str(c.be_width_pct),
                    },
                },
            )
        return opened

    async def run_once(self, now: Optional[datetime] = None) -> Dict[str, int]:
        now = (now or tehran_now()).replace(second=0, microsecond=0)
        if not is_market_day(now.date()):
            return {"opened": 0, "adjusted": 0, "closed": 0, "skipped": 1}

        assert self.pool is not None
        async with self.pool.acquire() as conn:
            db = DB(conn)
            run_id = await db.insert(
                "INSERT INTO short_premium_engine_runs (started_at,status) VALUES (%s,'RUNNING')",
                (now,),
            )
            await conn.commit()

        if not in_market_window(now):
            async with self.pool.acquire() as conn:
                db = DB(conn)
                await db.execute(
                    "UPDATE short_premium_engine_runs SET finished_at=%s,status='SKIPPED' WHERE run_id=%s",
                    (tehran_now(), run_id),
                )
                await conn.commit()
            return {"opened": 0, "adjusted": 0, "closed": 0, "skipped": 1}

        opened = adjusted = closed = 0
        try:
            async with self.pool.acquire() as conn:
                db = DB(conn)
                try:
                    accounts = await self._accounts(db)
                    adjusted, closed = await self._manage_positions(db, accounts, now)
                    for strategy in ("SHORT_STRADDLE", "SHORT_STRANGLE"):
                        account = await self._refresh_account(db, strategy, now)
                        opened += await self._execute_entries(db, strategy, account, now)
                        await self._refresh_account(db, strategy, now)
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

            async with self.pool.acquire() as conn:
                db = DB(conn)
                await db.execute(
                    """
                    UPDATE short_premium_engine_runs
                    SET finished_at=%s,status='SUCCESS',positions_opened=%s,
                        adjustments_executed=%s,positions_closed=%s
                    WHERE run_id=%s
                    """,
                    (tehran_now(), opened, adjusted, closed, run_id),
                )
                await conn.commit()
            return {"opened": opened, "adjusted": adjusted, "closed": closed, "skipped": 0}
        except Exception as exc:
            async with self.pool.acquire() as conn:
                db = DB(conn)
                await db.execute(
                    "UPDATE short_premium_engine_runs SET finished_at=%s,status='FAILED',error_message=%s "
                    "WHERE run_id=%s",
                    (tehran_now(), str(exc)[:2000], run_id),
                )
                await conn.commit()
            raise

    async def watch(self) -> None:
        while not self.shutdown.is_set():
            now = tehran_now()
            if in_market_window(now):
                if now.minute % 5 == 0 and now.second < 5:
                    try:
                        result = await self.run_once(now)
                        print(
                            f"[{tehran_now():%H:%M:%S}] short-premium scan "
                            f"opened={result['opened']} adjusted={result['adjusted']} closed={result['closed']}"
                        )
                    except Exception as exc:
                        print(f"[{tehran_now():%H:%M:%S}] ERROR short-premium scan: {exc}")
                    try:
                        await asyncio.wait_for(self.shutdown.wait(), timeout=55)
                    except asyncio.TimeoutError:
                        pass
                    continue

            # Sleep toward the next five-minute boundary without busy-waiting.
            seconds = max(1, min(30, SCAN_INTERVAL_SECONDS))
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=seconds)
            except asyncio.TimeoutError:
                pass


async def main_async(args: argparse.Namespace) -> None:
    engine = ShortPremiumEngine()
    loop = asyncio.get_running_loop()

    def stop() -> None:
        engine.shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    await engine.open()
    try:
        if args.watch:
            print(
                f"[{tehran_now():%H:%M:%S}] short-premium engine ready | "
                f"accounts={STRADDLE_ACCOUNT_NAME},{STRANGLE_ACCOUNT_NAME}"
            )
            await engine.watch()
        else:
            result = await engine.run_once()
            print(json.dumps(result, ensure_ascii=False))
    finally:
        await engine.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true", help="Run continuously on 5-minute snapshots.")
    p.add_argument("--once", action="store_true", help="Run one snapshot.")
    args = p.parse_args()
    if not args.watch:
        args.once = True
    return args


if __name__ == "__main__":
    asyncio.run(main_async(parse_args()))
