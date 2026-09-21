# -*- coding: utf-8 -*-
"""ReyT Bale notifier for the unified MySQL paper-trading stack.

Responsibilities (notification only):
  * 08:30 Tehran, market days: send a start-of-day snapshot of the shared
    paper account.
  * During market hours: notify each NEW actionable strategy signal after the
    strategy engine has resolved paper execution status, and explicitly say
    whether the signal was executed on the 100M-toman paper account.
  * 13:00 Tehran, market days: send an end-of-day account snapshot and a CSV
    containing ALL strategy-signal rows from the beginning of the database.

This program NEVER creates/alters MySQL tables and NEVER makes trading/risk
choices. It only reads the database produced by:
  - ReyT_strategy_engine_unified_1b_execution_status_v2.py
  - 00_create_all_ReyT_mysql_tables_REBUILT_v2_execution_status.sql

Persistent notification state is stored in a small local SQLite database so
restarting the service does not resend old messages. The SQLite state is not
part of the trading database and can be rebuilt safely if needed.

Default behavior intentionally avoids signal spam:
  * Only CANDIDATE / STRONG_CANDIDATE rows are considered "signals" for Bale.
  * Exactly one message is sent per logical strategy structure per Tehran trading day.
  * Immediate signal notifications are EXECUTED-only; NOT_EXECUTED remains in DB/CSV/EOD.
  * Later collector refreshes or same-day status changes update MySQL but do not
    generate another Bale message for that logical structure.
  * Fresh SQLite state starts from the beginning of the current Tehran day so
    today's already-committed signals are not silently skipped.

Examples:
  python ReyT_bale_notifier_unified.py --watch
  python ReyT_bale_notifier_unified.py --test-message
  python ReyT_bale_notifier_unified.py --morning-report
  python ReyT_bale_notifier_unified.py --eod-report
  python ReyT_bale_notifier_unified.py --poll-once
  python ReyT_bale_notifier_unified.py --export-csv
"""
from __future__ import annotations

import argparse
import asyncio
import configparser
import csv
import hashlib
import json
import os
import signal
import sqlite3
import ssl
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import aiohttp
import aiomysql


# =============================================================================
# Configuration
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = Path(os.getenv("OPTIONS_CONFIG_FILE", str(BASE_DIR / "settings.ini")))
_CONFIG = configparser.ConfigParser(interpolation=None)
if CONFIG_FILE.exists():
    _CONFIG.read(CONFIG_FILE, encoding="utf-8")


def _setting(name: str, default: str = "") -> str:
    env = os.getenv(name)
    if env is not None:
        return env.strip()
    key = name.lower()
    for section in ("bale", "paper", "strategy", "mysql", "mariadb", "database", "sql", "market"):
        if _CONFIG.has_option(section, key):
            return _CONFIG.get(section, key).strip()
    return default


def _bool_setting(name: str, default: bool) -> bool:
    return _setting(name, "yes" if default else "no").lower() in {"1", "true", "yes", "y", "on"}


def _int_setting(name: str, default: int, minimum: Optional[int] = None) -> int:
    value = int(_setting(name, str(default)))
    return max(value, minimum) if minimum is not None else value


MYSQL_HOST = _setting("MYSQL_HOST", _setting("SQL_SERVER", "127.0.0.1"))
MYSQL_PORT = _int_setting("MYSQL_PORT", 3306, 1)
MYSQL_DATABASE = _setting("MYSQL_DATABASE", _setting("SQL_DATABASE", "ghazali1_ReyTOption"))
MYSQL_USER = _setting("MYSQL_USER", _setting("SQL_USERNAME", "reyt_app"))
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", _setting("SQL_PASSWORD"))
MYSQL_CHARSET = _setting("MYSQL_CHARSET", "utf8mb4")
MYSQL_CONNECT_TIMEOUT = _int_setting("MYSQL_CONNECT_TIMEOUT", 20, 1)
MYSQL_SSL = _bool_setting("MYSQL_SSL", False)
MYSQL_TIME_ZONE = _setting("MYSQL_TIME_ZONE", "+03:30")

PAPER_ACCOUNT_NAME = _setting("PAPER_ACCOUNT_NAME", "paper_100m_toman")
PAPER_ACCOUNT_NAMES = tuple(
    dict.fromkeys(
        x.strip()
        for x in _setting("PAPER_ACCOUNT_NAMES", PAPER_ACCOUNT_NAME).split(",")
        if x.strip()
    )
)
if not PAPER_ACCOUNT_NAMES:
    PAPER_ACCOUNT_NAMES = (PAPER_ACCOUNT_NAME,)

BALE_BOT_TOKEN = _setting("BALE_BOT_TOKEN", "")
BALE_CHAT_ID = _setting("BALE_CHAT_ID", "")
BALE_API_BASE = _setting("BALE_API_BASE", "https://tapi.bale.ai")
BALE_HTTP_TIMEOUT = _int_setting("BALE_HTTP_TIMEOUT", 30, 5)
BALE_HTTP_RETRIES = _int_setting("BALE_HTTP_RETRIES", 4, 1)
BALE_SIGNAL_POLL_SECONDS = float(_setting("BALE_SIGNAL_POLL_SECONDS", "2"))
BALE_SIGNAL_MODE = _setting("BALE_SIGNAL_MODE", "executed_only").strip().lower()
BALE_SIGNAL_OVERLAP_SECONDS = _int_setting("BALE_SIGNAL_OVERLAP_SECONDS", 5, 1)
BALE_QUERY_BATCH_SIZE = _int_setting("BALE_QUERY_BATCH_SIZE", 2000, 100)

STATE_DB_PATH = Path(_setting("BALE_STATE_DB_PATH", str(BASE_DIR / "reyt_bale_notifier_state.sqlite3")))
REPORT_DIR = Path(_setting("BALE_REPORT_DIR", str(BASE_DIR / "bale_reports")))

MORNING_REPORT_TIME = time(8, 30)
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(12, 30)
END_REPORT_TIME = time(13, 0)
MARKET_WEEKDAYS = {0, 1, 2, 5, 6}  # Saturday..Wednesday, Python Monday=0
TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))
TOMAN_TO_RIAL = Decimal("10")
MONEY_Q = Decimal("0.01")


def _parse_holidays(raw: str) -> set[date]:
    out: set[date] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.add(datetime.strptime(item, "%Y-%m-%d").date())
    return out


MARKET_HOLIDAYS = _parse_holidays(_setting("MARKET_HOLIDAYS", ""))

STRATEGY_FA = {
    "COVERED_CALL": "کاورد کال",
    "PROTECTIVE_PUT": "پروتکتیو پوت",
    "BULL_CALL_SPREAD": "بول کال اسپرد",
    "BEAR_PUT_SPREAD": "بر پوت اسپرد",
    "LONG_STRADDLE": "لانگ استرادل",
}

EXECUTION_REASON_FA = {
    "POSITION_OPENED": "روی حساب فرضی اجرا شد",
    "INSUFFICIENT_CASH": "نقد آزاد حساب برای اجرای معامله کافی نبود",
    "INSUFFICIENT_ORDER_BOOK_DEPTH": "عمق اردربوک برای حجم موردنیاز کافی نبود",
    "ORDER_BOOK_MISSING": "اردربوک قرارداد در دسترس نبود",
    "STALE_ORDER_BOOK": "اردربوک برای ورود بیش از حد قدیمی بود",
    "RISK_BUDGET_EXCEEDED": "ریسک معامله از سقف ۱۰ میلیون تومان عبور می‌کرد",
    "DTE_BELOW_MINIMUM": "کمتر از حداقل ۵ روز تا سررسید باقی مانده بود",
    "SCORE_BELOW_ENTRY_THRESHOLD": "امتیاز سیگنال از حداقل ورود پایین‌تر بود",
    "CC_MIN_ITM_PCT_NOT_MET": "قیمت اعمال اختیار خرید به‌اندازه حداقل تعیین‌شده پایین‌تر از قیمت دارایی پایه نیست",
    "DUPLICATE_OPEN_SIGNATURE": "همین ساختار هنوز یک پوزیشن باز دارد",
    "AUTO_TRADE_DISABLED": "اجرای خودکار Paper Trading غیرفعال بود",
    "NO_EXECUTABLE_UNITS": "پس از اعمال ریسک، نقد و عمق بازار حجم قابل اجرا صفر شد",
    "INVALID_MAX_LOSS": "محاسبه حداکثر زیان معتبر نبود",
    "INVALID_EXECUTION_METRICS": "محاسبات اجرای VWAP معتبر نبود",
    "EXECUTION_PLAN_FAILED": "برنامه اجرای Paper Trading ساخته نشد",
    "STRATEGY_FILTER_REJECTED": "فیلترهای خود استراتژی سیگنال را رد کردند",
    "EXPECTED_RETURN_NO_HISTORY": "داده تاریخی کافی برای محاسبه بازده مورد انتظار وجود نداشت",
    "EXPECTED_RETURN_NO_IV": "IV معتبر برای محاسبه بازده مورد انتظار وجود نداشت",
    "EXPECTED_RETURN_BELOW_40PCT": "بازده مورد انتظار از حدنصاب معادل ۴۰٪ مؤثر سالانه عبور نکرد",
    "VWAP_HISTORY_UNAVAILABLE": "بازده تاریخی در قیمت اجرای VWAP قابل محاسبه نبود",
    "VWAP_IV_UNAVAILABLE": "بازده مبتنی بر IV در قیمت اجرای VWAP قابل محاسبه نبود",
    "VWAP_HISTORY_BELOW_40PCT": "پس از VWAP، بازده مورد انتظار تاریخی زیر حدنصاب ۴۰٪ قرار گرفت",
    "VWAP_IV_BELOW_40PCT": "پس از VWAP، بازده مورد انتظار مبتنی بر IV زیر حدنصاب ۴۰٪ قرار گرفت",
    "VWAP_EXPECTED_RETURN_BELOW_40PCT": "پس از VWAP، بازده مورد انتظار زیر حدنصاب ۴۰٪ قرار گرفت",
}


# =============================================================================
# Helpers
# =============================================================================


def tehran_now() -> datetime:
    return datetime.now(TEHRAN_TZ).replace(tzinfo=None)


def is_market_day(d: Optional[date] = None) -> bool:
    d = d or tehran_now().date()
    return d.weekday() in MARKET_WEEKDAYS and d not in MARKET_HOLIDAYS


def dec(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else default
    except (InvalidOperation, TypeError, ValueError):
        return default


def money(value: Any) -> Decimal:
    return dec(value).quantize(MONEY_Q, rounding=ROUND_HALF_UP)


def toman(value_rial: Any) -> Decimal:
    return money(dec(value_rial) / TOMAN_TO_RIAL)


def fmt_money_toman(value_rial: Any) -> str:
    return f"{toman(value_rial):,.0f}"


def fmt_num(value: Any, digits: int = 2, empty: str = "—") -> str:
    if value is None:
        return empty
    try:
        return f"{Decimal(str(value)):,.{digits}f}"
    except Exception:
        return str(value)


def text_or_dash(value: Any) -> str:
    return "—" if value in (None, "") else str(value)


def strategy_name(code: Any) -> str:
    code = str(code or "")
    return STRATEGY_FA.get(code, code or "نامشخص")


def side_fa(side: Any) -> str:
    return "خرید" if str(side or "").upper() == "LONG" else "فروش"


def option_fa(option_type: Any) -> str:
    val = str(option_type or "").upper()
    return "Call" if val == "CALL" else "Put" if val == "PUT" else "سهم پایه"


def _jsonish(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def signal_details(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("details_json")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str) and raw.strip():
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    return {}


def logical_signal_key(row: Mapping[str, Any]) -> str:
    """Stable structure key independent of collector source_revision/signal_id."""
    raw = "|".join(
        str(row.get(k) or "")
        for k in (
            "paper_account_name", "account_id",
            "signal_date", "strategy_code", "ua_ins_code", "expiry_date",
            "leg1_kind", "leg1_ins_code", "leg1_side",
            "leg2_kind", "leg2_ins_code", "leg2_side",
        )
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def validate_configuration() -> None:
    if BALE_SIGNAL_MODE not in {"executed_only", "all_once"}:
        raise ValueError("BALE_SIGNAL_MODE must be executed_only or all_once")
    missing = []
    if not MYSQL_HOST:
        missing.append("MYSQL_HOST")
    if not MYSQL_DATABASE:
        missing.append("MYSQL_DATABASE")
    if not MYSQL_USER:
        missing.append("MYSQL_USER")
    if not MYSQL_PASSWORD:
        missing.append("MYSQL_PASSWORD")
    if not BALE_BOT_TOKEN:
        missing.append("BALE_BOT_TOKEN")
    if not BALE_CHAT_ID:
        missing.append("BALE_CHAT_ID")
    if missing:
        raise RuntimeError("Missing required configuration: " + ", ".join(missing))
    if MYSQL_PORT > 65535:
        raise RuntimeError("MYSQL_PORT must be <= 65535")


# =============================================================================
# Local persistent state
# =============================================================================


class LocalState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signal_state (
                logical_key TEXT PRIMARY KEY,
                last_signal_source TEXT,
                last_signal_id INTEGER,
                last_status TEXT,
                last_position_id INTEGER,
                last_reason_code TEXT,
                last_sent_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sent_events (
                event_key TEXT PRIMARY KEY,
                sent_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily_reports (
                report_date TEXT NOT NULL,
                report_type TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                PRIMARY KEY (report_date, report_type)
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get_meta(self, key: str) -> Optional[str]:
        assert self.conn is not None
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_signal_state(self, logical_key: str) -> Optional[sqlite3.Row]:
        assert self.conn is not None
        return self.conn.execute(
            "SELECT * FROM signal_state WHERE logical_key=?", (logical_key,)
        ).fetchone()

    def set_signal_state(
        self,
        logical_key: str,
        source: str,
        signal_id: int,
        status: str,
        position_id: Optional[int],
        reason_code: Optional[str],
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT INTO signal_state(
                logical_key,last_signal_source,last_signal_id,last_status,
                last_position_id,last_reason_code,last_sent_at
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(logical_key) DO UPDATE SET
                last_signal_source=excluded.last_signal_source,
                last_signal_id=excluded.last_signal_id,
                last_status=excluded.last_status,
                last_position_id=excluded.last_position_id,
                last_reason_code=excluded.last_reason_code,
                last_sent_at=excluded.last_sent_at
            """,
            (
                logical_key, source, signal_id, status, position_id, reason_code,
                tehran_now().isoformat(sep=" ", timespec="seconds"),
            ),
        )
        self.conn.commit()

    def event_sent(self, event_key: str) -> bool:
        assert self.conn is not None
        return self.conn.execute(
            "SELECT 1 FROM sent_events WHERE event_key=?", (event_key,)
        ).fetchone() is not None

    def mark_event_sent(self, event_key: str) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT OR IGNORE INTO sent_events(event_key,sent_at) VALUES(?,?)",
            (event_key, tehran_now().isoformat(sep=" ", timespec="seconds")),
        )
        self.conn.commit()

    def daily_sent(self, d: date, report_type: str) -> bool:
        assert self.conn is not None
        return self.conn.execute(
            "SELECT 1 FROM daily_reports WHERE report_date=? AND report_type=?",
            (d.isoformat(), report_type),
        ).fetchone() is not None

    def mark_daily_sent(self, d: date, report_type: str) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT OR IGNORE INTO daily_reports(report_date,report_type,sent_at) VALUES(?,?,?)",
            (d.isoformat(), report_type, tehran_now().isoformat(sep=" ", timespec="seconds")),
        )
        self.conn.commit()


# =============================================================================
# Bale HTTP client
# =============================================================================


class BaleClient:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None

    @property
    def api_prefix(self) -> str:
        return f"{BALE_API_BASE.rstrip('/')}/bot{self.token}"

    async def open(self) -> None:
        timeout = aiohttp.ClientTimeout(total=BALE_HTTP_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=8, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def _retry_sleep(self, response: Optional[aiohttp.ClientResponse], attempt: int) -> None:
        retry_after = None
        if response is not None:
            raw = response.headers.get("Retry-After")
            try:
                retry_after = float(raw) if raw else None
            except ValueError:
                retry_after = None
        await asyncio.sleep(retry_after if retry_after is not None else min(2 ** (attempt - 1), 15))

    async def send_message(self, text: str) -> Dict[str, Any]:
        if self.session is None:
            raise RuntimeError("Bale client is not open")
        url = f"{self.api_prefix}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}
        last_error: Optional[Exception] = None
        for attempt in range(1, BALE_HTTP_RETRIES + 1):
            response: Optional[aiohttp.ClientResponse] = None
            try:
                response = await self.session.post(url, json=payload)
                body = await response.text()
                if response.status == 429 or response.status >= 500:
                    if attempt < BALE_HTTP_RETRIES:
                        await self._retry_sleep(response, attempt)
                        continue
                if response.status >= 400:
                    raise RuntimeError(f"Bale sendMessage HTTP {response.status}: {body[:500]}")
                data = json.loads(body) if body else {}
                if isinstance(data, dict) and data.get("ok") is False:
                    raise RuntimeError(f"Bale sendMessage failed: {data}")
                return data if isinstance(data, dict) else {"result": data}
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt >= BALE_HTTP_RETRIES:
                    break
                await self._retry_sleep(response, attempt)
        raise RuntimeError(f"Bale sendMessage failed after retries: {last_error}")

    async def send_document(self, path: Path, caption: str = "") -> Dict[str, Any]:
        if self.session is None:
            raise RuntimeError("Bale client is not open")
        if not path.exists():
            raise FileNotFoundError(path)
        url = f"{self.api_prefix}/sendDocument"
        last_error: Optional[Exception] = None
        for attempt in range(1, BALE_HTTP_RETRIES + 1):
            response: Optional[aiohttp.ClientResponse] = None
            try:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(self.chat_id))
                if caption:
                    form.add_field("caption", caption)
                with path.open("rb") as fh:
                    form.add_field(
                        "document", fh,
                        filename=path.name,
                        content_type="text/csv",
                    )
                    response = await self.session.post(url, data=form)
                    body = await response.text()
                if response.status == 429 or response.status >= 500:
                    if attempt < BALE_HTTP_RETRIES:
                        await self._retry_sleep(response, attempt)
                        continue
                if response.status >= 400:
                    raise RuntimeError(f"Bale sendDocument HTTP {response.status}: {body[:500]}")
                data = json.loads(body) if body else {}
                if isinstance(data, dict) and data.get("ok") is False:
                    raise RuntimeError(f"Bale sendDocument failed: {data}")
                return data if isinstance(data, dict) else {"result": data}
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError, OSError) as exc:
                last_error = exc
                if attempt >= BALE_HTTP_RETRIES:
                    break
                await self._retry_sleep(response, attempt)
        raise RuntimeError(f"Bale sendDocument failed after retries: {last_error}")


# =============================================================================
# MySQL repository
# =============================================================================


class Repository:
    REQUIRED_OBJECTS = (
        "paper_strategy_account",
        "paper_positions",
        "paper_position_legs",
        "vw_all_strategy_signals",
    )

    def __init__(self) -> None:
        self.pool: Optional[aiomysql.Pool] = None

    async def open(self) -> None:
        ssl_context = ssl.create_default_context() if MYSQL_SSL else None
        self.pool = await aiomysql.create_pool(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE,
            charset=MYSQL_CHARSET,
            autocommit=True,
            minsize=1,
            maxsize=3,
            connect_timeout=MYSQL_CONNECT_TIMEOUT,
            cursorclass=aiomysql.DictCursor,
            ssl=ssl_context,
            init_command=f"SET time_zone = '{MYSQL_TIME_ZONE}'",
        )
        await self.verify()

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def verify(self) -> None:
        if self.pool is None:
            raise RuntimeError("Repository not open")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for obj in self.REQUIRED_OBJECTS:
                    try:
                        await cur.execute(f"SELECT 1 FROM `{obj}` LIMIT 0")
                    except Exception as exc:
                        raise RuntimeError(f"Required database object missing or unreadable: {obj}: {exc}") from exc

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
        if self.pool is None:
            raise RuntimeError("Repository not open")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(params))
                row = await cur.fetchone()
                return dict(row) if row else None

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        if self.pool is None:
            raise RuntimeError("Repository not open")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(params))
                return [dict(x) for x in await cur.fetchall()]

    async def account_snapshot(self, account_name: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, int]]:
        account = await self.fetchone(
            """
            SELECT account_id,account_name,initial_equity_rial,current_equity_rial,
                   realized_pnl_rial,unrealized_pnl_rial,reserved_risk_rial,
                   allocated_capital_rial,open_positions_count,drawdown_pct,last_scan_at
            FROM paper_strategy_account
            WHERE account_name=%s
            """,
            (account_name,),
        )
        if not account:
            raise RuntimeError(f"Paper account not found: {account_name}")
        account_id = int(account["account_id"])
        by_strategy = await self.fetchall(
            """
            SELECT strategy_code,
                   SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_count,
                   COALESCE(SUM(CASE WHEN status='OPEN' THEN unrealized_pnl_rial ELSE 0 END),0) AS open_pnl_rial,
                   COALESCE(SUM(CASE WHEN status<>'OPEN' THEN realized_pnl_rial ELSE 0 END),0) AS realized_pnl_rial
            FROM paper_positions
            WHERE account_id=%s
            GROUP BY strategy_code
            ORDER BY strategy_code
            """,
            (account_id,),
        )
        today = tehran_now().date()
        sig = await self.fetchone(
            """
            SELECT
                COUNT(*) AS total_actionable,
                SUM(CASE WHEN paper_execution_status='EXECUTED' THEN 1 ELSE 0 END) AS executed,
                SUM(CASE WHEN paper_execution_status='NOT_EXECUTED' THEN 1 ELSE 0 END) AS not_executed
            FROM vw_all_strategy_signals
            WHERE account_id=%s
              AND signal_date=%s
              AND final_signal IN ('CANDIDATE','STRONG_CANDIDATE')
            """,
            (account_id, today),
        ) or {"total_actionable": 0, "executed": 0, "not_executed": 0}
        sig_counts = {k: int(sig.get(k) or 0) for k in ("total_actionable", "executed", "not_executed")}
        return account, by_strategy, sig_counts

    async def max_signal_updated_at(self) -> Optional[datetime]:
        placeholders = ",".join(["%s"] * len(PAPER_ACCOUNT_NAMES))
        row = await self.fetchone(
            f"""
            SELECT MAX(s.updated_at) AS max_updated_at
            FROM vw_all_strategy_signals s
            JOIN paper_strategy_account a
              ON a.account_id=s.account_id
            WHERE a.account_name IN ({placeholders})
            """,
            PAPER_ACCOUNT_NAMES,
        )
        value = row.get("max_updated_at") if row else None
        return value if isinstance(value, datetime) else None

    async def changed_signals(self, since: datetime) -> List[Dict[str, Any]]:
        """Fetch actionable signal rows changed since watermark for monitored paper accounts."""
        results: List[Dict[str, Any]] = []
        offset = 0
        placeholders = ",".join(["%s"] * len(PAPER_ACCOUNT_NAMES))
        while True:
            rows = await self.fetchall(
                f"""
                SELECT s.*, a.account_name AS paper_account_name
                FROM vw_all_strategy_signals s
                JOIN paper_strategy_account a
                  ON a.account_id=s.account_id
                WHERE s.updated_at >= %s
                  AND a.account_name IN ({placeholders})
                  AND s.final_signal IN ('CANDIDATE','STRONG_CANDIDATE')
                  AND s.paper_execution_status IN ('EXECUTED','NOT_EXECUTED')
                ORDER BY s.updated_at, s.scan_time, s.signal_source_table, s.signal_id
                LIMIT {BALE_QUERY_BATCH_SIZE} OFFSET {offset}
                """,
                (since, *PAPER_ACCOUNT_NAMES),
            )
            results.extend(rows)
            if len(rows) < BALE_QUERY_BATCH_SIZE:
                break
            offset += BALE_QUERY_BATCH_SIZE
        return results

    async def executed_legs(self, position_id: int) -> List[Dict[str, Any]]:
        return await self.fetchall(
            """
            SELECT leg_no,instrument_kind,ins_code,symbol,side,option_type,
                   strike_price_rial,contract_size,contract_count,quantity_units,
                   entry_price_rial,latest_price_rial
            FROM paper_position_legs
            WHERE position_id=%s
            ORDER BY leg_no
            """,
            (position_id,),
        )

    async def position_status(self, position_id: int) -> Optional[str]:
        row = await self.fetchone(
            "SELECT status FROM paper_positions WHERE position_id=%s", (position_id,)
        )
        return str(row["status"]) if row else None

    async def export_all_signals_csv(self, path: Path) -> int:
        if self.pool is None:
            raise RuntimeError("Repository not open")
        path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT * FROM vw_all_strategy_signals ORDER BY scan_time,signal_source_table,signal_id"
                )
                columns = [str(d[0]) for d in cur.description]
                with path.open("w", encoding="utf-8-sig", newline="") as fh:
                    writer = csv.writer(fh)
                    writer.writerow(columns)
                    while True:
                        rows = await cur.fetchmany(1000)
                        if not rows:
                            break
                        for row in rows:
                            writer.writerow([_jsonish(row.get(col)) for col in columns])
                            total += 1
        return total


# =============================================================================
# Message formatting
# =============================================================================


def format_account_snapshot(
    account: Mapping[str, Any],
    by_strategy: Sequence[Mapping[str, Any]],
    sig_counts: Mapping[str, int],
    title: str,
    now: datetime,
) -> str:
    initial = dec(account.get("initial_equity_rial"))
    realized = dec(account.get("realized_pnl_rial"))
    unrealized = dec(account.get("unrealized_pnl_rial"))
    allocated = dec(account.get("allocated_capital_rial"))
    current = dec(account.get("current_equity_rial"))
    reserved = dec(account.get("reserved_risk_rial"))
    available_cash = max(Decimal("0"), initial + realized - allocated)

    lines = [
        title,
        f"🧾 حساب: {text_or_dash(account.get('account_name'))}",
        f"🕒 {now:%Y-%m-%d %H:%M} تهران",
        "",
        f"💼 سرمایه اولیه: {fmt_money_toman(initial)} تومان",
        f"📊 ارزش فعلی حساب: {fmt_money_toman(current)} تومان",
        f"💵 نقد آزاد قابل استفاده: {fmt_money_toman(available_cash)} تومان",
        f"🔒 سرمایه درگیر: {fmt_money_toman(allocated)} تومان",
        f"⚠️ مجموع Max Loss پوزیشن‌های باز: {fmt_money_toman(reserved)} تومان",
        f"📈 سود/زیان تحقق‌یافته: {fmt_money_toman(realized)} تومان",
        f"📉 سود/زیان شناور: {fmt_money_toman(unrealized)} تومان",
        f"📌 تعداد پوزیشن باز: {int(account.get('open_positions_count') or 0)}",
        f"📉 Drawdown: {fmt_num(account.get('drawdown_pct'), 2)}٪",
    ]
    if by_strategy:
        lines.extend(["", "تفکیک پوزیشن‌های باز:"])
        for row in by_strategy:
            open_count = int(row.get("open_count") or 0)
            if open_count <= 0:
                continue
            lines.append(
                f"• {strategy_name(row.get('strategy_code'))}: {open_count} پوزیشن | "
                f"P/L باز {fmt_money_toman(row.get('open_pnl_rial'))} تومان"
            )
    lines.extend(
        [
            "",
            f"سیگنال‌های قابل ورود امروز: {int(sig_counts.get('total_actionable') or 0)}",
            f"✅ اجراشده روی حساب: {int(sig_counts.get('executed') or 0)}",
            f"⛔ اجرا‌نشده روی حساب: {int(sig_counts.get('not_executed') or 0)}",
        ]
    )
    return "\n".join(lines)


def format_candidate_leg(row: Mapping[str, Any], n: int) -> str:
    kind = row.get(f"leg{n}_kind")
    symbol = text_or_dash(row.get(f"leg{n}_symbol"))
    side = side_fa(row.get(f"leg{n}_side"))
    opt = option_fa(row.get(f"leg{n}_option_type"))
    strike = row.get(f"leg{n}_strike_rial")
    px = row.get(f"leg{n}_entry_price_rial")
    if str(kind) == "UNDERLYING":
        return f"• Leg {n}: {side} سهم {symbol} | قیمت سیگنال {fmt_money_toman(px)} تومان"
    return (
        f"• Leg {n}: {side} {opt} {symbol} | Strike {fmt_money_toman(strike)} تومان | "
        f"قیمت سیگنال {fmt_money_toman(px)} تومان"
    )


def format_executed_leg(leg: Mapping[str, Any]) -> str:
    symbol = text_or_dash(leg.get("symbol"))
    side = side_fa(leg.get("side"))
    kind = str(leg.get("instrument_kind") or "")
    if kind == "UNDERLYING":
        return f"• {side} سهم {symbol} | اجرای Paper: {fmt_money_toman(leg.get('entry_price_rial'))} تومان"
    return (
        f"• {side} {option_fa(leg.get('option_type'))} {symbol} | "
        f"Strike {fmt_money_toman(leg.get('strike_price_rial'))} تومان | "
        f"اجرای Paper: {fmt_money_toman(leg.get('entry_price_rial'))} تومان"
    )


# Strategy-specific paper-policy reasons
EXECUTION_REASON_FA.update({
    "ANNUALIZED_RETURN_BELOW_50PCT":
        "بازده سالانه‌شده کاورد کال کمتر از حداقل ۵۰٪ است",
    "VWAP_ANNUALIZED_RETURN_BELOW_50PCT":
        "بازده سالانه‌شده کاورد کال پس از VWAP کمتر از ۵۰٪ شده است",
    "MIN_NET_CAPITAL_NOT_MET":
        "سرمایه خالص کاورد کال کمتر از حداقل ۲۰۰ هزار تومان است",
    "ONE_UNIT_EXCEEDS_MAX_NET_CAPITAL":
        "حتی یک واحد کامل کاورد کال بیش از سقف ۱ میلیون تومان سرمایه خالص نیاز دارد",
    "MAX_NET_CAPITAL_EXCEEDED":
        "سرمایه خالص کاورد کال از سقف ۱ میلیون تومان بیشتر است",
    "VWAP_MAX_NET_CAPITAL_EXCEEDED":
        "سرمایه خالص کاورد کال پس از VWAP از سقف ۱ میلیون تومان عبور کرده است",
    "PP_MIN_ITM_PCT_NOT_MET":
        "اختیار فروش حداقل ۱۰٪ داخل سود نیست",
    "PP_EXPECTED_RETURN_NO_HISTORY":
        "داده تاریخی کافی برای مدل بازده انتظاری پروتکتیو پوت وجود ندارد",
    "PP_EXPECTED_RETURN_NO_IV":
        "IV قابل استفاده برای مدل بازده انتظاری پروتکتیو پوت وجود ندارد",
    "PP_EXPECTED_RETURN_BELOW_50PCT":
        "بازده انتظاری History و IV هر دو از حداقل معادل ۵۰٪ سالانه عبور نکرده‌اند",
    "PP_ONE_UNIT_EXCEEDS_MAX_CAPITAL":
        "حتی یک واحد کامل پروتکتیو پوت بیش از سقف ۱ میلیون تومان سرمایه نیاز دارد",
    "PP_MAX_CAPITAL_EXCEEDED":
        "سرمایه پروتکتیو پوت از سقف ۱ میلیون تومان بیشتر است",
    "PP_VWAP_MAX_CAPITAL_EXCEEDED":
        "سرمایه پروتکتیو پوت پس از VWAP از سقف ۱ میلیون تومان عبور کرده است",
    "PP_MIN_CAPITAL_NOT_MET":
        "سرمایه پروتکتیو پوت کمتر از حداقل ۲۰۰ هزار تومان است",
    "PP_VWAP_HISTORY_UNAVAILABLE":
        "مدل History در قیمت اجرای نهایی قابل محاسبه نیست",
    "PP_VWAP_IV_UNAVAILABLE":
        "مدل IV در قیمت اجرای نهایی قابل محاسبه نیست",
    "PP_VWAP_EXPECTED_RETURN_BELOW_50PCT":
        "بازده انتظاری پروتکتیو پوت پس از VWAP از حداقل معادل ۵۰٪ سالانه عبور نکرده است",
})

def format_signal_message(
    row: Mapping[str, Any],
    executed_legs: Sequence[Mapping[str, Any]] = (),
    status_update: bool = False,
) -> str:
    executed = str(row.get("paper_execution_status")) == "EXECUTED"
    reason_code = str(row.get("paper_execution_reason_code") or "")
    reason = EXECUTION_REASON_FA.get(reason_code) or text_or_dash(
        row.get("paper_execution_reason")
    )
    details = signal_details(row)
    strategy_code = str(row.get("strategy_code") or "")
    is_covered_call = strategy_code == "COVERED_CALL"
    is_protective_put = strategy_code == "PROTECTIVE_PUT"

    style = {
        "COVERED_CALL": ("🟦", "کاورد کال"),
        "PROTECTIVE_PUT": ("🛡️", "پروتکتیو پوت"),
    }.get(strategy_code, ("🔔", strategy_name(strategy_code)))

    icon, strategy_title = style
    header_prefix = "🔄 بروزرسانی" if status_update else "🔔 سیگنال جدید"
    header = f"{header_prefix} | {icon} {strategy_title}"
    status_line = (
        "✅ روی حساب فرضی اجرا شد"
        if executed
        else "⛔ روی حساب فرضی اجرا نشد"
    )

    signal_annualized_er = details.get("annualized_return_pct")
    execution_annualized_er = details.get("execution_annualized_return_pct")
    cc_min_annualized = details.get(
        "covered_call_min_annualized_return_pct", 50
    )

    pp_hist_ann = details.get(
        "history_expected_annualized_return_pct"
    )
    pp_iv_ann = details.get(
        "iv_expected_annualized_return_pct"
    )
    pp_exec_hist_ann = details.get(
        "execution_history_expected_annualized_return_pct"
    )
    pp_exec_iv_ann = details.get(
        "execution_iv_expected_annualized_return_pct"
    )
    pp_min_annualized = details.get(
        "protective_put_min_expected_annualized_return_pct", 50
    )

    itm_depth = (
        details.get("itm_depth_pct")
        if is_covered_call
        else details.get("protective_put_itm_depth_pct")
    )

    lines = [
        header,
        f"🧾 حساب: {text_or_dash(row.get('paper_account_name'))}",
        f"📌 دارایی پایه: {text_or_dash(row.get('underlying_symbol'))}",
        (
            f"📅 سررسید: {text_or_dash(row.get('expiry_date'))} | "
            f"DTE: {int(row.get('days_to_expiry') or 0)} روز"
        ),
        (
            f"🎯 عمق ITM: {fmt_num(itm_depth, 2)}٪ | "
            f"Score: {fmt_num(row.get('strategy_score'), 2)} | "
            f"Liquidity: {fmt_num(row.get('liquidity_score'), 2)}"
        ),
    ]

    if is_covered_call:
        threshold_value = (
            execution_annualized_er
            if executed and execution_annualized_er is not None
            else signal_annualized_er
        )
        try:
            threshold_ok = (
                float(threshold_value) >= float(cc_min_annualized)
            )
        except (TypeError, ValueError):
            threshold_ok = False

        lines.extend([
            "",
            "📈 بازده",
            (
                f"• سالانه‌شده سیگنال: "
                f"{fmt_num(signal_annualized_er, 2)}٪"
            ),
        ])
        if executed:
            lines.append(
                f"• سالانه‌شده اجرای واقعی: "
                f"{fmt_num(execution_annualized_er, 2)}٪"
            )
        lines.append(
            f"• حداقل موردنیاز: {fmt_num(cc_min_annualized, 2)}٪ "
            f"{'✅' if threshold_ok else '⛔'}"
        )

    elif is_protective_put:
        values = [
            x
            for x in (
                pp_exec_hist_ann if executed else pp_hist_ann,
                pp_exec_iv_ann if executed else pp_iv_ann,
            )
            if x is not None
        ]
        try:
            threshold_ok = (
                len(values) == 2
                and min(float(x) for x in values)
                >= float(pp_min_annualized)
            )
        except (TypeError, ValueError):
            threshold_ok = False

        lines.extend([
            "",
            "📈 بازده انتظاری سالانه‌شده",
            f"• History: {fmt_num(pp_hist_ann, 2)}٪",
            f"• IV: {fmt_num(pp_iv_ann, 2)}٪",
        ])
        if executed:
            lines.extend([
                f"• History بعد از VWAP: "
                f"{fmt_num(pp_exec_hist_ann, 2)}٪",
                f"• IV بعد از VWAP: "
                f"{fmt_num(pp_exec_iv_ann, 2)}٪",
            ])
        lines.append(
            f"• حداقل هر دو مدل: "
            f"{fmt_num(pp_min_annualized, 2)}٪ "
            f"{'✅' if threshold_ok else '⛔'}"
        )

    else:
        hist_er = details.get("history_expected_return_to_expiry_pct")
        iv_er = details.get("iv_expected_return_to_expiry_pct")
        required_er = details.get("required_return_to_expiry_pct")
        lines.extend([
            "",
            f"بازده انتظاری History تا سررسید: {fmt_num(hist_er, 2)}٪",
            f"بازده انتظاری IV تا سررسید: {fmt_num(iv_er, 2)}٪",
            f"حدنصاب این DTE: {fmt_num(required_er, 2)}٪",
        ])

    lines.extend([
        "",
        "🧩 ساختار معامله",
        format_candidate_leg(row, 1),
        format_candidate_leg(row, 2),
        "",
    ])

    capital_title = (
        "سرمایه خالص"
        if is_covered_call
        else "سرمایه کل پوزیشن"
        if is_protective_put
        else "سرمایه"
    )
    lines.extend([
        f"💰 {capital_title} پیشنهادی: "
        f"{fmt_money_toman(row.get('recommended_capital_rial'))} تومان",
        f"🔢 حجم پیشنهادی: {int(row.get('recommended_units') or 0)}",
        "",
        status_line,
    ])

    if executed:
        lines.extend([
            f"Position ID: #{int(row.get('opened_position_id') or 0)}",
            f"حجم اجرای واقعی: {int(row.get('paper_executed_units') or 0)}",
            (
                f"💰 {capital_title} واقعی: "
                f"{fmt_money_toman(row.get('paper_entry_capital_rial'))} تومان"
            ),
        ])
        if is_protective_put:
            lines.append(
                f"🛡️ Max Loss واقعی: "
                f"{fmt_money_toman(row.get('paper_entry_max_loss_rial'))} تومان"
            )
        if executed_legs:
            lines.append("قیمت اجرای واقعی از Order Book:")
            lines.extend(
                format_executed_leg(x) for x in executed_legs
            )
    else:
        lines.append(f"علت: {reason}")
        if reason_code:
            lines.append(f"کد علت: {reason_code}")

    lines.extend([
        "",
        f"🕒 زمان سیگنال: {text_or_dash(row.get('scan_time'))}",
        (
            f"Signal ID: {row.get('signal_source_table')}#"
            f"{int(row.get('signal_id') or 0)}"
        ),
    ])
    return "\n".join(lines)


# =============================================================================
# Notifier service
# =============================================================================


@dataclass
class NotifyDecision:
    should_send: bool
    status_update: bool = False


class ReyTBaleNotifier:
    def __init__(self) -> None:
        self.repo = Repository()
        self.state = LocalState(STATE_DB_PATH)
        self.bale = BaleClient(BALE_BOT_TOKEN, BALE_CHAT_ID)
        self.shutdown = False

    async def open(self) -> None:
        validate_configuration()
        self.state.open()
        await self.repo.open()
        await self.bale.open()
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        if self.state.get_meta("signal_watermark") is None:
            now = tehran_now()
            baseline = datetime.combine(now.date(), time.min)
            self.state.set_meta("signal_watermark", baseline.isoformat(sep=" ", timespec="seconds"))
            print(f"[{tehran_now():%H:%M:%S}] ℹ️ Fresh state: today's signal baseline initialized at {baseline}")
        print(
            f"[{tehran_now():%H:%M:%S}] ✅ Bale notifier ready | "
            f"accounts={','.join(PAPER_ACCOUNT_NAMES)} | "
            f"signal_mode={BALE_SIGNAL_MODE}"
        )

    async def close(self) -> None:
        await self.bale.close()
        await self.repo.close()
        self.state.close()

    def _notify_decision(self, row: Mapping[str, Any]) -> NotifyDecision:
        # Final notification policy:
        #   executed_only -> send only when a logical structure becomes EXECUTED.
        #                    A prior NOT_EXECUTED state does not suppress the later fill.
        #                    Once EXECUTED has been sent, never send that logical structure again.
        #   all_once      -> legacy behavior: first committed EXECUTED/NOT_EXECUTED once/day.
        logical = logical_signal_key(row)
        previous = self.state.get_signal_state(logical)
        status = str(row.get("paper_execution_status") or "")

        if BALE_SIGNAL_MODE == "executed_only":
            if status != "EXECUTED":
                return NotifyDecision(False, False)
            if previous is not None and str(previous["last_status"] or "") == "EXECUTED":
                return NotifyDecision(False, False)
            return NotifyDecision(True, previous is not None)

        return NotifyDecision(previous is None, False)

    async def _process_row(self, row: Mapping[str, Any]) -> bool:
        decision = self._notify_decision(row)
        logical = logical_signal_key(row)
        status = str(row.get("paper_execution_status") or "")
        reason_code = str(row.get("paper_execution_reason_code") or "") or None
        position_id = int(row["opened_position_id"]) if row.get("opened_position_id") is not None else None

        if not decision.should_send:
            return False

        source = str(row.get("signal_source_table") or "")
        signal_id = int(row.get("signal_id") or 0)
        event_key = f"signal:{source}:{signal_id}:{status}:{position_id or 0}"
        if self.state.event_sent(event_key):
            return False

        legs: List[Dict[str, Any]] = []
        if status == "EXECUTED" and position_id is not None:
            legs = await self.repo.executed_legs(position_id)
        message = format_signal_message(row, legs, status_update=decision.status_update)
        await self.bale.send_message(message)
        self.state.mark_event_sent(event_key)
        self.state.set_signal_state(logical, source, signal_id, status, position_id, reason_code)
        print(f"[{tehran_now():%H:%M:%S}] 📣 Signal sent | {source}#{signal_id} | {status}")
        return True

    async def poll_signals_once(self) -> int:
        watermark_raw = self.state.get_meta("signal_watermark")
        if watermark_raw is None:
            latest = await self.repo.max_signal_updated_at() or tehran_now()
            self.state.set_meta("signal_watermark", latest.isoformat(sep=" ", timespec="seconds"))
            return 0
        try:
            watermark = datetime.fromisoformat(watermark_raw)
        except ValueError:
            watermark = tehran_now() - timedelta(seconds=BALE_SIGNAL_OVERLAP_SECONDS)
        since = watermark - timedelta(seconds=BALE_SIGNAL_OVERLAP_SECONDS)
        rows = await self.repo.changed_signals(since)
        sent = 0
        max_updated = watermark
        for row in rows:
            updated = row.get("updated_at")
            if isinstance(updated, datetime) and updated > max_updated:
                max_updated = updated
            if await self._process_row(row):
                sent += 1
        self.state.set_meta("signal_watermark", max_updated.isoformat(sep=" ", timespec="seconds"))
        return sent

    async def send_snapshot(self, title: str) -> None:
        for account_name in PAPER_ACCOUNT_NAMES:
            try:
                account, by_strategy, sig_counts = await self.repo.account_snapshot(
                    account_name
                )
            except RuntimeError as exc:
                print(
                    f"[{tehran_now():%H:%M:%S}] ⚠️ Snapshot skipped for "
                    f"{account_name}: {exc}"
                )
                continue
            message = format_account_snapshot(
                account, by_strategy, sig_counts, title, tehran_now()
            )
            await self.bale.send_message(message)

    async def export_csv(self, report_date: Optional[date] = None) -> Tuple[Path, int]:
        d = report_date or tehran_now().date()
        path = REPORT_DIR / f"ReyT_all_paper_signals_{d:%Y-%m-%d}.csv"
        count = await self.repo.export_all_signals_csv(path)
        return path, count

    async def send_end_of_day_csv(self, d: date) -> Tuple[Path, int]:
        path, count = await self.export_csv(d)
        caption = (
            f"📎 CSV کامل Paper Trading تا پایان {d.isoformat()}\n"
            f"تعداد ردیف سیگنال: {count:,}\n"
            "استراتژی‌های فعال: کاورد کال + پروتکتیو پوت\n"
            "شامل حساب مستقل هر استراتژی، وضعیت اجرا، علت عدم اجرا و اطلاعات Paper Trading"
        )
        await self.bale.send_document(path, caption=caption)
        return path, count

    async def scheduled_actions_once(self) -> None:
        now = tehran_now()
        d = now.date()
        if not is_market_day(d):
            return

        # Morning report is only meaningful before actual trading starts. If the
        # service was down until after 09:00, we do not fake a historical 08:30 snapshot.
        if MORNING_REPORT_TIME <= now.time() < MARKET_OPEN and not self.state.daily_sent(d, "morning"):
            await self.send_snapshot("🌅 گزارش ابتدای بازار — حساب فرضی ReyT")
            self.state.mark_daily_sent(d, "morning")
            print(f"[{now:%H:%M:%S}] ✅ 08:30 account snapshot sent")

        # End-of-day data remains valid after 13:00, so same-day restart can catch up.
        if now.time() >= END_REPORT_TIME:
            if not self.state.daily_sent(d, "eod_text"):
                await self.send_snapshot("🌇 گزارش پایان بازار — حساب فرضی ReyT")
                self.state.mark_daily_sent(d, "eod_text")
                print(f"[{now:%H:%M:%S}] ✅ 13:00 account snapshot sent")
            if not self.state.daily_sent(d, "eod_csv"):
                path, count = await self.send_end_of_day_csv(d)
                self.state.mark_daily_sent(d, "eod_csv")
                print(f"[{now:%H:%M:%S}] ✅ EOD CSV sent | {count} rows | {path}")

    async def watch(self) -> None:
        print("=" * 78)
        print("ReyT Bale Notifier")
        print("=" * 78)
        print(f"08:30 snapshot   : market days only")
        print(f"Signal polling   : {BALE_SIGNAL_POLL_SECONDS:g}s during market hours | mode={BALE_SIGNAL_MODE}")
        print(f"13:00 snapshot   : market days only")
        print(f"13:00 CSV        : ALL historical strategy signals")
        print(f"State SQLite     : {STATE_DB_PATH}")
        print("=" * 78)

        while not self.shutdown:
            try:
                await self.scheduled_actions_once()
                now = tehran_now()
                if is_market_day(now.date()) and MARKET_OPEN <= now.time() <= MARKET_CLOSE:
                    await self.poll_signals_once()
                    await asyncio.sleep(max(0.5, BALE_SIGNAL_POLL_SECONDS))
                else:
                    await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{tehran_now():%H:%M:%S}] ❌ Notifier loop error: {exc}")
                await asyncio.sleep(10)


# =============================================================================
# CLI
# =============================================================================


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ReyT Bale notifier")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--watch", action="store_true", help="Run the notifier service continuously (default).")
    mode.add_argument("--test-message", action="store_true", help="Send one Bale connectivity test message.")
    mode.add_argument("--morning-report", action="store_true", help="Send an account snapshot immediately.")
    mode.add_argument("--eod-report", action="store_true", help="Send EOD snapshot + complete CSV immediately.")
    mode.add_argument("--poll-once", action="store_true", help="Process changed signal notifications once.")
    mode.add_argument("--export-csv", action="store_true", help="Export complete signal CSV locally without sending it.")
    return p


async def async_main(args: argparse.Namespace) -> None:
    notifier = ReyTBaleNotifier()

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        notifier.shutdown = True
        print("\n🛑 Shutdown requested...")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    await notifier.open()
    try:
        if args.test_message:
            await notifier.bale.send_message(
                f"✅ ReyT Bale Notifier connected successfully.\n🕒 {tehran_now():%Y-%m-%d %H:%M:%S} Tehran"
            )
            print("✅ Test message sent.")
            return
        if args.morning_report:
            await notifier.send_snapshot("🌅 گزارش حساب فرضی ReyT")
            return
        if args.eod_report:
            await notifier.send_snapshot("🌇 گزارش پایان بازار — حساب فرضی ReyT")
            path, count = await notifier.send_end_of_day_csv(tehran_now().date())
            print(f"✅ EOD report sent | CSV rows={count} | {path}")
            return
        if args.poll_once:
            sent = await notifier.poll_signals_once()
            print(f"✅ Signal poll finished | messages sent={sent}")
            return
        if args.export_csv:
            path, count = await notifier.export_csv()
            print(f"✅ CSV exported | rows={count} | {path}")
            return
        await notifier.watch()
    finally:
        await notifier.close()


def main() -> None:
    args = build_parser().parse_args()
    if not any((args.watch, args.test_message, args.morning_report, args.eod_report, args.poll_once, args.export_csv)):
        args.watch = True
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"❌ Fatal notifier error: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
