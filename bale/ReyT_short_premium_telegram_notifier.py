#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Telegram notifier for the isolated ReyT short-premium engine."""

from __future__ import annotations

import argparse
import asyncio
import configparser
import json
import os
import signal
import sqlite3
import ssl
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

import aiohttp
import aiomysql
from andro_cfw import CFWSession


TEHRAN_TZ = ZoneInfo("Asia/Tehran")
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
    for section in ("short_premium_telegram", "database", "mysql", "mariadb", "sql"):
        if CFG.has_option(section, key):
            return CFG.get(section, key).strip()
    return default


def _int(name: str, default: int, minimum: int = 0) -> int:
    return max(minimum, int(_setting(name, str(default))))


def _clock(name: str, default: str) -> time:
    return datetime.strptime(_setting(name, default), "%H:%M").time()


MYSQL_HOST = _setting("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = _int("MYSQL_PORT", 3306, 1)
MYSQL_DATABASE = _setting("MYSQL_DATABASE", "ghazali1_ReyTOption")
MYSQL_USER = _setting("MYSQL_USER", "reyt_app")
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", "")
MYSQL_CHARSET = _setting("MYSQL_CHARSET", "utf8mb4")
MYSQL_TIME_ZONE = _setting("MYSQL_TIME_ZONE", "+03:30")
MYSQL_CONNECT_TIMEOUT = _int("MYSQL_CONNECT_TIMEOUT", 20, 1)

BOT_TOKEN = _setting("BOT_TOKEN", "")
CHAT_ID = _setting("CHAT_ID", "")
HTTP_TIMEOUT = _int("HTTP_TIMEOUT", 30, 5)
HTTP_RETRIES = _int("HTTP_RETRIES", 4, 1)
POLL_SECONDS = float(_setting("POLL_SECONDS", "2"))
CFW_SESSION_PATH = Path(_setting("CFW_SESSION", "/var/lib/reyt/telegram/cfw.session"))
MORNING_TIME = _clock("REPORT_TIME_MORNING", "08:30")
EOD_TIME = _clock("REPORT_TIME_EOD", "13:00")
STATE_PATH = Path(
    _setting(
        "STATE_DB_PATH",
        "/var/lib/reyt/short-premium/notifier_state.sqlite3",
    )
)

TOMAN_TO_RIAL = Decimal("10")


def now_tehran() -> datetime:
    return datetime.now(TEHRAN_TZ).replace(tzinfo=None)


def is_market_day(d: date) -> bool:
    return d.weekday() in {0, 1, 2, 5, 6}


def dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def toman(value_rial: Any) -> str:
    return f"{dec(value_rial) / TOMAN_TO_RIAL:,.0f}"


def num(value: Any, digits: int = 2) -> str:
    try:
        return f"{dec(value):,.{digits}f}"
    except Exception:
        return "—"


def strategy_fa(code: Any) -> str:
    return {
        "SHORT_STRADDLE": "شورت استرادل",
        "SHORT_STRANGLE": "شورت استرانگل",
    }.get(str(code or ""), str(code or "نامشخص"))


class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def get(self, key: str) -> Optional[str]:
        assert self.conn is not None
        row = self.conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def set(self, key: str, value: str) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        self.conn.commit()


class TelegramClient:
    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_prefix = ""

    async def open(self) -> None:
        if not BOT_TOKEN or not CHAT_ID:
            raise RuntimeError("Short-premium Telegram BOT_TOKEN/CHAT_ID are missing.")
        cfw = CFWSession.load(str(CFW_SESSION_PATH))
        self.api_prefix = f"{cfw.worker_url.rstrip('/')}/bot{BOT_TOKEN}"
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        )

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def send(self, message: str) -> None:
        if not self.session:
            raise RuntimeError("Telegram client is closed.")
        url = f"{self.api_prefix}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message}
        last: Optional[Exception] = None
        for attempt in range(1, HTTP_RETRIES + 1):
            try:
                async with self.session.post(url, json=payload) as resp:
                    body = await resp.text()
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < HTTP_RETRIES:
                            await asyncio.sleep(min(10, attempt * 2))
                            continue
                    if resp.status >= 400:
                        raise RuntimeError(f"Telegram HTTP {resp.status}: {body[:500]}")
                    data = json.loads(body)
                    if not data.get("ok"):
                        raise RuntimeError(f"Telegram send failed: {data}")
                    return
            except Exception as exc:
                last = exc
                if attempt < HTTP_RETRIES:
                    await asyncio.sleep(min(10, attempt * 2))
        raise RuntimeError(f"Telegram send failed after retries: {last}")


class Repo:
    def __init__(self) -> None:
        self.pool: Optional[aiomysql.Pool] = None

    async def open(self) -> None:
        if not MYSQL_PASSWORD:
            raise RuntimeError("MYSQL_PASSWORD is missing.")
        self.pool = await aiomysql.create_pool(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE,
            charset=MYSQL_CHARSET,
            minsize=1,
            maxsize=2,
            autocommit=False,
            connect_timeout=MYSQL_CONNECT_TIMEOUT,
            cursorclass=aiomysql.DictCursor,
            init_command=f"SET time_zone = '{MYSQL_TIME_ZONE}'",
            ssl=ssl.create_default_context() if _setting("MYSQL_SSL", "no").lower() in {"1","yes","true"} else None,
        )

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def fetchall(self, sql: str, args: Sequence[Any] = ()) -> list[Dict[str, Any]]:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(args))
                return [dict(x) for x in await cur.fetchall()]

    async def pending_events(self) -> list[Dict[str, Any]]:
        return await self.fetchall(
            """
            SELECT e.*,a.account_name
            FROM short_premium_events e
            JOIN short_premium_accounts a ON a.account_id=e.account_id
            WHERE e.notified_at IS NULL
            ORDER BY e.event_id
            LIMIT 200
            """
        )

    async def mark_sent(self, event_id: int) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE short_premium_events SET notified_at=%s,notification_error=NULL WHERE event_id=%s",
                    (now_tehran(), event_id),
                )
            await conn.commit()

    async def mark_error(self, event_id: int, error: str) -> None:
        assert self.pool
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE short_premium_events SET notification_error=%s WHERE event_id=%s",
                    (error[:1000], event_id),
                )
            await conn.commit()

    async def accounts(self) -> list[Dict[str, Any]]:
        return await self.fetchall(
            """
            SELECT * FROM short_premium_accounts
            ORDER BY FIELD(strategy_code,'SHORT_STRADDLE','SHORT_STRANGLE')
            """
        )

    async def open_positions(self, account_id: int) -> list[Dict[str, Any]]:
        return await self.fetchall(
            """
            SELECT position_id,underlying_symbol,expiry_date,status,current_margin_rial,
                   lower_breakeven_rial,upper_breakeven_rial,net_pnl_rial
            FROM short_premium_positions
            WHERE account_id=%s AND status<>'CLOSED'
            ORDER BY expiry_date,underlying_symbol,position_id
            """,
            (account_id,),
        )


def details(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("details_json")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    try:
        value = json.loads(str(raw or "{}"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def format_event(row: Mapping[str, Any]) -> str:
    d = details(row)
    typ = str(row.get("event_type") or "")
    strategy = strategy_fa(row.get("strategy_code"))
    pid = int(row.get("position_id") or 0)
    header = {
        "ENTRY": "🔔 ورود جدید ReyT",
        "ADJUSTMENT": "🛠 تعدیل پوزیشن ReyT",
        "FORCED_EXIT_START": "⚠️ خروج اجباری ReyT",
        "SCHEDULED_EXIT_START": "🕛 خروج زمان‌بندی‌شده ReyT",
        "PARTIAL_EXIT": "🚪 خروج ناقص ReyT",
        "POSITION_CLOSED": "✅ پوزیشن بسته شد ReyT",
    }.get(typ, "ℹ️ رویداد ReyT")

    lines = [header, f"استراتژی: {strategy}", f"Position ID: #{pid}"]

    if typ == "ENTRY":
        lines.extend(
            [
                f"دارایی پایه: {d.get('underlying','—')}",
                f"سررسید: {d.get('expiry','—')}",
                f"Spot: {toman(d.get('spot_rial'))} تومان",
                "",
            ]
        )
        if str(row.get("strategy_code")) == "SHORT_STRADDLE":
            lines.extend(
                [
                    f"Strike مشترک: {toman(d.get('put_strike_rial'))} تومان",
                    f"Put: {d.get('put_symbol','—')} | Bid {toman(d.get('put_bid_rial'))} تومان",
                    f"Call: {d.get('call_symbol','—')} | Bid {toman(d.get('call_bid_rial'))} تومان",
                ]
            )
        else:
            lines.extend(
                [
                    f"Put OTM: {d.get('put_symbol','—')} | Strike {toman(d.get('put_strike_rial'))} | Bid {toman(d.get('put_bid_rial'))}",
                    f"Call OTM: {d.get('call_symbol','—')} | Strike {toman(d.get('call_strike_rial'))} | Bid {toman(d.get('call_bid_rial'))}",
                ]
            )
        lines.extend(
            [
                f"حجم اجرا: {d.get('qty',0)} قرارداد در هر سمت",
                "",
                f"Premium ناخالص: {toman(d.get('gross_premium_rial'))} تومان",
                f"Premium خالص: {toman(d.get('net_premium_rial'))} تومان",
                f"کارمزد ورود: {toman(d.get('fees_rial'))} تومان",
                f"وجه تضمین: {toman(d.get('margin_rial'))} تومان",
                f"Premium/Margin: {num(d.get('net_premium_margin_pct'))}٪",
                f"Lower BE: {toman(d.get('lower_be_rial'))} تومان",
                f"Upper BE: {toman(d.get('upper_be_rial'))} تومان",
                f"Stress Filter: ±{num(d.get('stress_pct'),0)}٪ ✅",
            ]
        )
    elif typ == "ADJUSTMENT":
        side = "Call" if d.get("side") == "CALL" else "Put"
        lines.extend(
            [
                f"دارایی پایه: {d.get('underlying','—')} | سررسید: {d.get('expiry','—')}",
                f"Spot: {toman(d.get('spot_rial'))} تومان",
                f"Loss Distance قبل: {num(d.get('loss_distance_before_pct'))}٪",
                f"سمت تعدیل: فروش {side}",
                f"قرارداد: {d.get('symbol','—')} | Strike {toman(d.get('strike_rial'))} تومان",
                f"Best Bid: {toman(d.get('best_bid_rial'))} تومان",
                f"حجم هدف/اجرا: {d.get('target_qty',0)} / {d.get('executed_qty',0)}",
                f"Margin افزوده: {toman(d.get('added_margin_rial'))} تومان",
                f"Margin بعد: {toman(d.get('margin_after_rial'))} تومان",
                f"Lower BE جدید: {toman(d.get('lower_be_after_rial'))} تومان",
                f"Upper BE جدید: {toman(d.get('upper_be_after_rial'))} تومان",
                f"Loss Distance بعد: {num(d.get('loss_distance_after_pct'))}٪",
            ]
        )
    elif typ in {"FORCED_EXIT_START", "SCHEDULED_EXIT_START"}:
        lines.extend(
            [
                f"دارایی پایه: {d.get('underlying','—')} | سررسید: {d.get('expiry','—')}",
                f"نوع خروج: {'اجباری' if typ == 'FORCED_EXIT_START' else 'روز قبل سررسید'}",
                f"علت: {d.get('reason','—')}",
                "وضعیت: EXITING — تعدیل جدید متوقف شد.",
            ]
        )
        if typ == "SCHEDULED_EXIT_START":
            lines.append("اجرا: Ask Level 1 → در صورت کمبود Ask Level 2.")
    elif typ == "PARTIAL_EXIT":
        lines.extend(
            [
                f"قراردادهای باز باقی‌مانده: {d.get('remaining_contracts',0)}",
                f"Lower BE فعلی: {toman(d.get('lower_be_rial'))} تومان",
                f"Upper BE فعلی: {toman(d.get('upper_be_rial'))} تومان",
                f"P&L خالص فعلی: {toman(d.get('net_pnl_rial'))} تومان",
                "وضعیت: EXITING — در سیکل بعد خروج ادامه پیدا می‌کند.",
            ]
        )
    elif typ == "POSITION_CLOSED":
        lines.extend(
            [
                f"دارایی پایه: {d.get('underlying','—')} | سررسید: {d.get('expiry','—')}",
                f"نوع خروج: {d.get('exit_mode') or '—'}",
                f"Gross P&L: {toman(d.get('gross_pnl_rial'))} تومان",
                f"کل کارمزد: {toman(d.get('total_fees_rial'))} تومان",
                f"Net P&L: {toman(d.get('net_pnl_rial'))} تومان",
            ]
        )

    return "\n".join(lines)


async def account_report(repo: Repo, title: str) -> str:
    lines = [title, f"🕒 {now_tehran():%Y-%m-%d %H:%M} تهران"]
    for a in await repo.accounts():
        account_id = int(a["account_id"])
        positions = await repo.open_positions(account_id)
        lines.extend(
            [
                "",
                f"📌 {strategy_fa(a['strategy_code'])}",
                f"🧾 حساب: {a['account_name']}",
                f"💼 سرمایه اولیه: {toman(a['initial_equity_rial'])} تومان",
                f"📊 Equity فعلی: {toman(a['current_equity_rial'])} تومان",
                f"📈 P&L تحقق‌یافته: {toman(a['realized_pnl_rial'])} تومان",
                f"📉 P&L شناور: {toman(a['unrealized_pnl_rial'])} تومان",
                f"🟦 هدف Entry 70٪: {toman(a['entry_bucket_target_rial'])} تومان",
                f"🔒 Entry درگیر: {toman(a['entry_capital_used_rial'])} تومان",
                f"🟨 هدف Adjustment 30٪: {toman(a['adjustment_bucket_target_rial'])} تومان",
                f"🛠 Adjustment درگیر: {toman(a['adjustment_capital_used_rial'])} تومان",
                f"📌 پوزیشن باز/درحال خروج: {len(positions)}",
                f"📉 Drawdown: {num(a['drawdown_pct'])}٪",
            ]
        )
        for p in positions[:20]:
            lines.append(
                f"• #{p['position_id']} {p['underlying_symbol']} {p['expiry_date']} "
                f"[{p['status']}] | Margin {toman(p['current_margin_rial'])} | "
                f"P/L {toman(p['net_pnl_rial'])}"
            )
    return "\n".join(lines)


class Notifier:
    def __init__(self) -> None:
        self.repo = Repo()
        self.tg = TelegramClient()
        self.state = State(STATE_PATH)
        self.shutdown = asyncio.Event()

    async def open(self) -> None:
        self.state.open()
        await self.repo.open()
        await self.tg.open()

    async def close(self) -> None:
        await self.tg.close()
        await self.repo.close()
        self.state.close()

    async def poll_events(self) -> None:
        for row in await self.repo.pending_events():
            try:
                await self.tg.send(format_event(row))
                await self.repo.mark_sent(int(row["event_id"]))
            except Exception as exc:
                await self.repo.mark_error(int(row["event_id"]), str(exc))
                raise

    async def reports(self) -> None:
        now = now_tehran()
        if not is_market_day(now.date()):
            return
        key_day = now.date().isoformat()

        if now.time() >= MORNING_TIME:
            key = f"morning:{key_day}"
            if self.state.get(key) != "sent":
                await self.tg.send(await account_report(self.repo, "🌅 گزارش شروع روز — Short Premium ReyT"))
                self.state.set(key, "sent")

        if now.time() >= EOD_TIME:
            key = f"eod:{key_day}"
            if self.state.get(key) != "sent":
                await self.tg.send(await account_report(self.repo, "🌇 گزارش پایان روز — Short Premium ReyT"))
                self.state.set(key, "sent")

    async def watch(self) -> None:
        while not self.shutdown.is_set():
            try:
                await self.poll_events()
                await self.reports()
            except Exception as exc:
                print(f"[{now_tehran():%H:%M:%S}] notifier error: {exc}")
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def main_async(args: argparse.Namespace) -> None:
    app = Notifier()
    loop = asyncio.get_running_loop()

    def stop() -> None:
        app.shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    await app.open()
    try:
        if args.test_message:
            await app.tg.send("✅ بات مستقل Short Straddle / Short Strangle ReyT آماده است.")
        elif args.report:
            await app.tg.send(await account_report(app.repo, "📊 گزارش Short Premium ReyT"))
        else:
            print(f"[{now_tehran():%H:%M:%S}] short-premium Telegram notifier ready")
            await app.watch()
    finally:
        await app.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true")
    p.add_argument("--test-message", action="store_true")
    p.add_argument("--report", action="store_true")
    args = p.parse_args()
    if not args.test_message and not args.report:
        args.watch = True
    return args


if __name__ == "__main__":
    asyncio.run(main_async(parse_args()))
