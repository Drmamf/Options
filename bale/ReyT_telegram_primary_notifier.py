# -*- coding: utf-8 -*-
"""
ReyT notification router.

Channel 1: Telegram through andro-cfw / Cloudflare Worker
Channel 2: Bale

Every notification is attempted on both channels.
A single-channel failure is logged; delivery fails only if both channels fail.

All message formatting, DB reads, scheduling, SQLite state and CSV generation
remain in ReyT_bale_notifier_unified.py.
"""

from __future__ import annotations

import asyncio
import configparser
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from andro_cfw import CFWSession

import ReyT_bale_notifier_unified as legacy


# =============================================================================
# Telegram configuration
# =============================================================================

TELEGRAM_CONFIG_FILE = Path(
    os.getenv("TELEGRAM_CONFIG_FILE", "/etc/reyt/telegram/settings.ini")
)

TELEGRAM_CFW_SESSION = Path(
    os.getenv(
        "TELEGRAM_CFW_SESSION",
        "/var/lib/reyt/telegram/cfw.session",
    )
)

_tg_config = configparser.ConfigParser(interpolation=None)

if TELEGRAM_CONFIG_FILE.exists():
    _tg_config.read(TELEGRAM_CONFIG_FILE, encoding="utf-8")


def _tg_setting(name: str, default: str = "") -> str:
    env = os.getenv(name)
    if env is not None:
        return env.strip()

    key = name.lower()

    # settings.ini currently uses:
    # bot_token =
    # chat_id =
    aliases = {
        "telegram_bot_token": "bot_token",
        "telegram_chat_id": "chat_id",
    }

    config_key = aliases.get(key, key)

    if _tg_config.has_option("telegram", config_key):
        return _tg_config.get("telegram", config_key).strip()

    return default


TELEGRAM_BOT_TOKEN = _tg_setting("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = _tg_setting("TELEGRAM_CHAT_ID")

TELEGRAM_HTTP_TIMEOUT = int(
    _tg_setting("TELEGRAM_HTTP_TIMEOUT", "30")
)

TELEGRAM_HTTP_RETRIES = max(
    1,
    int(_tg_setting("TELEGRAM_HTTP_RETRIES", "4")),
)


# =============================================================================
# Telegram client through andro-cfw
# =============================================================================

class TelegramClient:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_prefix: Optional[str] = None
        self.startup_error: Optional[Exception] = None

    async def open(self) -> None:
        if not self.token:
            raise RuntimeError("Telegram bot token is missing")

        if not self.chat_id:
            raise RuntimeError("Telegram chat_id is missing")

        cfw = CFWSession.load(str(TELEGRAM_CFW_SESSION))

        self.api_prefix = (
            f"{cfw.worker_url.rstrip('/')}/bot{self.token}"
        )

        timeout = aiohttp.ClientTimeout(
            total=TELEGRAM_HTTP_TIMEOUT
        )

        connector = aiohttp.TCPConnector(
            limit=8,
            ttl_dns_cache=300,
        )

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
        )

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    async def _retry_sleep(
        self,
        response: Optional[aiohttp.ClientResponse],
        attempt: int,
    ) -> None:
        retry_after = None

        if response is not None:
            raw = response.headers.get("Retry-After")
            try:
                retry_after = float(raw) if raw else None
            except ValueError:
                retry_after = None

        delay = (
            retry_after
            if retry_after is not None
            else min(2 ** (attempt - 1), 15)
        )

        await asyncio.sleep(delay)

    async def send_message(
        self,
        text: str,
    ) -> Dict[str, Any]:

        if self.session is None or self.api_prefix is None:
            raise RuntimeError(
                f"Telegram client unavailable: {self.startup_error or 'not open'}"
            )

        url = f"{self.api_prefix}/sendMessage"

        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        last_error: Optional[Exception] = None

        for attempt in range(
            1,
            TELEGRAM_HTTP_RETRIES + 1,
        ):
            response = None

            try:
                response = await self.session.post(
                    url,
                    json=payload,
                )

                body = await response.text()

                if (
                    response.status == 429
                    or response.status >= 500
                ):
                    if attempt < TELEGRAM_HTTP_RETRIES:
                        await self._retry_sleep(
                            response,
                            attempt,
                        )
                        continue

                if response.status >= 400:
                    raise RuntimeError(
                        f"Telegram sendMessage HTTP "
                        f"{response.status}: {body[:500]}"
                    )

                data = json.loads(body) if body else {}

                if (
                    isinstance(data, dict)
                    and data.get("ok") is False
                ):
                    raise RuntimeError(
                        f"Telegram sendMessage failed: {data}"
                    )

                return (
                    data
                    if isinstance(data, dict)
                    else {"result": data}
                )

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                ValueError,
                RuntimeError,
            ) as exc:

                last_error = exc

                if attempt >= TELEGRAM_HTTP_RETRIES:
                    break

                await self._retry_sleep(
                    response,
                    attempt,
                )

        raise RuntimeError(
            "Telegram sendMessage failed after retries: "
            f"{last_error}"
        )

    async def send_document(
        self,
        path: Path,
        caption: str = "",
    ) -> Dict[str, Any]:

        if self.session is None or self.api_prefix is None:
            raise RuntimeError(
                f"Telegram client unavailable: {self.startup_error or 'not open'}"
            )

        if not path.exists():
            raise FileNotFoundError(path)

        url = f"{self.api_prefix}/sendDocument"

        last_error: Optional[Exception] = None

        for attempt in range(
            1,
            TELEGRAM_HTTP_RETRIES + 1,
        ):
            response = None

            try:
                form = aiohttp.FormData()

                form.add_field(
                    "chat_id",
                    str(self.chat_id),
                )

                if caption:
                    form.add_field(
                        "caption",
                        caption,
                    )

                with path.open("rb") as fh:
                    form.add_field(
                        "document",
                        fh,
                        filename=path.name,
                        content_type="text/csv",
                    )

                    response = await self.session.post(
                        url,
                        data=form,
                    )

                    body = await response.text()

                if (
                    response.status == 429
                    or response.status >= 500
                ):
                    if attempt < TELEGRAM_HTTP_RETRIES:
                        await self._retry_sleep(
                            response,
                            attempt,
                        )
                        continue

                if response.status >= 400:
                    raise RuntimeError(
                        f"Telegram sendDocument HTTP "
                        f"{response.status}: {body[:500]}"
                    )

                data = json.loads(body) if body else {}

                if (
                    isinstance(data, dict)
                    and data.get("ok") is False
                ):
                    raise RuntimeError(
                        f"Telegram sendDocument failed: {data}"
                    )

                return (
                    data
                    if isinstance(data, dict)
                    else {"result": data}
                )

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                ValueError,
                RuntimeError,
                OSError,
            ) as exc:

                last_error = exc

                if attempt >= TELEGRAM_HTTP_RETRIES:
                    break

                await self._retry_sleep(
                    response,
                    attempt,
                )

        raise RuntimeError(
            "Telegram sendDocument failed after retries: "
            f"{last_error}"
        )


# =============================================================================
# Telegram + Bale dual delivery
# =============================================================================

class DualChannelClient:
    def __init__(
        self,
        telegram: TelegramClient,
        bale: legacy.BaleClient,
    ) -> None:
        self.telegram = telegram
        self.bale = bale
        self.bale_startup_error: Optional[Exception] = None

    async def open(self) -> None:
        try:
            await self.telegram.open()

            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                "✅ Telegram channel ready"
            )

        except Exception as exc:
            self.telegram.startup_error = exc

            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"⚠️ Telegram unavailable at startup: {exc}"
            )

        try:
            await self.bale.open()

            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                "✅ Bale channel ready"
            )

        except Exception as exc:
            self.bale_startup_error = exc

            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"⚠️ Bale unavailable at startup: {exc}"
            )

    async def close(self) -> None:
        try:
            await self.telegram.close()
        except Exception as exc:
            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"⚠️ Telegram close failed: {exc}"
            )

        try:
            await self.bale.close()
        except Exception as exc:
            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"⚠️ Bale close failed: {exc}"
            )

    async def _capture(
        self,
        channel: str,
        operation: str,
        awaitable,
    ):
        try:
            result = await awaitable

            icon = "📎" if operation == "document" else "📨"

            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"{icon} {operation.capitalize()} delivered via {channel}"
            )

            return result, None

        except Exception as exc:
            print(
                f"[{legacy.tehran_now():%H:%M:%S}] "
                f"⚠️ {channel} {operation} delivery failed: {exc}"
            )

            return None, exc

    async def send_message(
        self,
        text: str,
    ) -> Dict[str, Any]:

        telegram_result, bale_result = await asyncio.gather(
            self._capture(
                "Telegram",
                "message",
                self.telegram.send_message(text),
            ),
            self._capture(
                "Bale",
                "message",
                self.bale.send_message(text),
            ),
        )

        telegram_data, telegram_error = telegram_result
        bale_data, bale_error = bale_result

        if telegram_error is not None and bale_error is not None:
            raise RuntimeError(
                "Both notification channels failed. "
                f"Telegram={telegram_error}; "
                f"Bale={bale_error}"
            )

        return {
            "ok": True,
            "telegram": telegram_data,
            "bale": bale_data,
        }

    async def send_document(
        self,
        path: Path,
        caption: str = "",
    ) -> Dict[str, Any]:

        telegram_result, bale_result = await asyncio.gather(
            self._capture(
                "Telegram",
                "document",
                self.telegram.send_document(
                    path,
                    caption=caption,
                ),
            ),
            self._capture(
                "Bale",
                "document",
                self.bale.send_document(
                    path,
                    caption=caption,
                ),
            ),
        )

        telegram_data, telegram_error = telegram_result
        bale_data, bale_error = bale_result

        if telegram_error is not None and bale_error is not None:
            raise RuntimeError(
                "Both document channels failed. "
                f"Telegram={telegram_error}; "
                f"Bale={bale_error}"
            )

        return {
            "ok": True,
            "telegram": telegram_data,
            "bale": bale_data,
        }


# =============================================================================
# Re-use the complete existing ReyT notifier
# =============================================================================

class ReyTNotifier(legacy.ReyTBaleNotifier):
    def __init__(self) -> None:
        super().__init__()

        bale_channel = self.bale

        telegram = TelegramClient(
            TELEGRAM_BOT_TOKEN,
            TELEGRAM_CHAT_ID,
        )

        # Existing notifier code calls self.bale.send_message()
        # and self.bale.send_document().
        #
        # Replace only the transport object. Every delivery is
        # attempted independently on both Telegram and Bale.
        self.bale = DualChannelClient(
            telegram,
            bale_channel,
        )

    async def open(self) -> None:
        await super().open()

        print(
            f"[{legacy.tehran_now():%H:%M:%S}] "
            "✅ Notification routing: "
            "Telegram + Bale DUAL SEND"
        )


def main() -> None:
    # legacy.async_main() normally creates ReyTBaleNotifier.
    # Replace that class only for this process.
    legacy.ReyTBaleNotifier = ReyTNotifier
    legacy.main()


if __name__ == "__main__":
    main()
