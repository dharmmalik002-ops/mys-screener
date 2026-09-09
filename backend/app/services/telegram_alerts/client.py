"""Telegram Bot API client.

Owns the retry ladder and the per-chat pacing, because getting those wrong is
how a digest either gets rate-limited into silence or spams a 400 loop.

Rate-limit envelope Telegram actually enforces: ~30 messages/second globally
but only ~1/second sustained to a single chat -- and a ``sendMediaGroup`` of N
items counts as N messages. So sends are strictly sequential and paced.

Graceful degradation follows ``AIAnalysisService``: construct without raising,
expose :attr:`available`, and gate every call on it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Sequence

import httpx

logger = logging.getLogger(__name__)

API_ROOT = "https://api.telegram.org"

# Album limits: 2..10 items. One item is a 400, so callers must use send_photo.
MEDIA_GROUP_MIN = 2
MEDIA_GROUP_MAX = 10

MAX_ATTEMPTS = 4
BACKOFF_SECONDS = (2.0, 5.0, 12.0)

PACE_MESSAGE_SECONDS = 1.2
PACE_PER_PHOTO_SECONDS = 0.9


class TelegramApiError(RuntimeError):
    def __init__(self, status_code: int, description: str, retry_after: float | None = None):
        super().__init__(f"HTTP {status_code}: {description}")
        self.status_code = status_code
        self.description = description
        self.retry_after = retry_after

    @property
    def is_fatal(self) -> bool:
        """400 is a caption/entity or photo-constraint bug and 403 means the bot
        is blocked. Retrying either just burns the send window -- and hammering
        a 403 is how bots earn a restriction."""
        return self.status_code in (400, 403, 404)


class TelegramClient:
    def __init__(
        self,
        token: str | None,
        *,
        timeout_seconds: float = 60.0,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        pacing_enabled: bool = True,
    ) -> None:
        self._token = str(token or "").strip() or None
        # Generous read/write: an album upload is ~1 MB from the Space.
        self._timeout = httpx.Timeout(connect=10.0, read=timeout_seconds, write=timeout_seconds, pool=10.0)
        self._sleep = sleeper or asyncio.sleep
        self._pacing_enabled = pacing_enabled
        self.calls: list[dict] = []   # populated only by the file-sink subclass

    @property
    def available(self) -> bool:
        return bool(self._token)

    def _url(self, method: str) -> str:
        return f"{API_ROOT}/bot{self._token}/{method}"

    # ---- transport --------------------------------------------------------

    async def _request(
        self,
        method: str,
        *,
        data: dict | None = None,
        files: dict | None = None,
    ) -> Any:
        if not self.available:
            raise TelegramApiError(0, "telegram bot token is not configured")

        last_error: TelegramApiError | None = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(self._url(method), data=data or {}, files=files)
                payload = _safe_json(response)
                if response.status_code == 200 and payload.get("ok"):
                    return payload.get("result")

                description = str(payload.get("description") or response.text)[:400]
                retry_after = _retry_after(payload)
                error = TelegramApiError(response.status_code, description, retry_after)

                if response.status_code == 429 and retry_after is not None:
                    # Trust the server's number; never back off less than it says.
                    logger.info("telegram %s rate limited, retry_after=%s", method, retry_after)
                    await self._sleep(retry_after + 1.0)
                    last_error = error
                    continue
                if error.is_fatal:
                    raise error
                last_error = error
            except httpx.HTTPError as exc:
                last_error = TelegramApiError(0, f"transport error: {exc}")

            if attempt < MAX_ATTEMPTS - 1:
                await self._sleep(BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)])

        raise last_error or TelegramApiError(0, f"{method} failed after {MAX_ATTEMPTS} attempts")

    async def _pace(self, seconds: float) -> None:
        if self._pacing_enabled and seconds > 0:
            await self._sleep(seconds)

    # ---- sending ----------------------------------------------------------

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: dict | None = None,
        parse_mode: str | None = "HTML",
        disable_web_page_preview: bool = True,
    ) -> Any:
        data: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": json.dumps(bool(disable_web_page_preview)),
        }
        if parse_mode:
            data["parse_mode"] = parse_mode
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        try:
            result = await self._request("sendMessage", data=data)
        except TelegramApiError as exc:
            # Almost always an entity-parsing complaint. Retry once as plain
            # text so the information still gets delivered.
            if exc.status_code == 400 and parse_mode:
                logger.warning("telegram sendMessage 400 (%s); retrying without parse_mode", exc.description)
                data.pop("parse_mode", None)
                result = await self._request("sendMessage", data=data)
            else:
                raise
        await self._pace(PACE_MESSAGE_SECONDS)
        return result

    async def send_photo(
        self,
        chat_id: int,
        photo: bytes,
        *,
        filename: str = "chart.png",
        caption: str | None = None,
        parse_mode: str | None = "HTML",
    ) -> Any:
        data: dict[str, Any] = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption
            if parse_mode:
                data["parse_mode"] = parse_mode
        files = {"photo": (filename, photo, "image/png")}
        result = await self._request("sendPhoto", data=data, files=files)
        await self._pace(PACE_MESSAGE_SECONDS)
        return result

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[tuple[str, bytes]],
        *,
        caption: str | None = None,
        parse_mode: str | None = "HTML",
    ) -> Any:
        """Send 2..10 photos as one album.

        The caption goes on item 0 ONLY. A group with two or more captioned
        items renders with no visible text at all in the timeline -- per-item
        captions surface only in the fullscreen viewer, one swipe at a time.
        """
        if not (MEDIA_GROUP_MIN <= len(items) <= MEDIA_GROUP_MAX):
            raise ValueError(
                f"sendMediaGroup takes {MEDIA_GROUP_MIN}..{MEDIA_GROUP_MAX} items, got {len(items)}"
            )

        media: list[dict] = []
        files: dict[str, tuple[str, bytes, str]] = {}
        for index, (filename, payload) in enumerate(items):
            key = f"p{index}"
            entry: dict[str, Any] = {"type": "photo", "media": f"attach://{key}"}
            if index == 0 and caption:
                entry["caption"] = caption
                if parse_mode:
                    entry["parse_mode"] = parse_mode
            media.append(entry)
            files[key] = (filename, payload, "image/png")

        data = {"chat_id": chat_id, "media": json.dumps(media, ensure_ascii=False)}
        result = await self._request("sendMediaGroup", data=data, files=files)
        await self._pace(max(PACE_MESSAGE_SECONDS, PACE_PER_PHOTO_SECONDS * len(items)))
        return result

    # ---- interaction ------------------------------------------------------

    async def answer_callback_query(
        self, callback_query_id: str, *, text: str | None = None, show_alert: bool = False
    ) -> Any:
        data: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text[:200]
        if show_alert:
            data["show_alert"] = json.dumps(True)
        return await self._request("answerCallbackQuery", data=data)

    async def edit_message_reply_markup(
        self, chat_id: int, message_id: int, reply_markup: dict | None
    ) -> Any:
        data: dict[str, Any] = {"chat_id": chat_id, "message_id": message_id}
        data["reply_markup"] = json.dumps(reply_markup or {})
        try:
            return await self._request("editMessageReplyMarkup", data=data)
        except TelegramApiError as exc:
            # Raised on a double-tap or a Telegram-side retry when the markup is
            # byte-identical. The most common source of noisy 400s in bot logs.
            if exc.status_code == 400 and "not modified" in exc.description.lower():
                return None
            raise

    # ---- webhook ----------------------------------------------------------

    async def set_webhook(
        self,
        url: str,
        *,
        secret_token: str,
        allowed_updates: Sequence[str] = ("message", "callback_query"),
        drop_pending_updates: bool = False,
        max_connections: int = 2,
    ) -> Any:
        return await self._request(
            "setWebhook",
            data={
                "url": url,
                "secret_token": secret_token,
                "allowed_updates": json.dumps(list(allowed_updates)),
                "drop_pending_updates": json.dumps(bool(drop_pending_updates)),
                "max_connections": int(max_connections),
            },
        )

    async def get_webhook_info(self) -> dict:
        return await self._request("getWebhookInfo") or {}

    async def delete_webhook(self, *, drop_pending_updates: bool = False) -> Any:
        return await self._request(
            "deleteWebhook", data={"drop_pending_updates": json.dumps(bool(drop_pending_updates))}
        )

    async def set_my_commands(self, commands: Sequence[tuple[str, str]]) -> Any:
        payload = [{"command": name, "description": description} for name, description in commands]
        return await self._request("setMyCommands", data={"commands": json.dumps(payload)})


def _safe_json(response: httpx.Response) -> dict:
    try:
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _retry_after(payload: dict) -> float | None:
    parameters = payload.get("parameters")
    if isinstance(parameters, dict) and "retry_after" in parameters:
        try:
            return float(parameters["retry_after"])
        except (TypeError, ValueError):
            return None
    return None


class FileSinkTelegramClient(TelegramClient):
    """Same interface, writes to disk instead of opening a socket.

    This is what makes the whole digest verifiable before Telegram is involved
    at all: ``messages.md`` is byte-identical to what would be sent (the only
    practical way to check HTML escaping against real company names), and
    ``calls.json`` is the ordered call log with arguments.
    """

    def __init__(self, out_dir, **kwargs) -> None:
        from pathlib import Path

        kwargs.setdefault("pacing_enabled", False)   # a dry run should be instant
        super().__init__("dry-run", **kwargs)
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.calls = []
        self._image_index = 0
        self._transcript: list[str] = []

    @property
    def available(self) -> bool:
        return True

    async def _request(self, method, *, data=None, files=None):  # pragma: no cover - never called
        raise AssertionError("FileSinkTelegramClient must not perform HTTP requests")

    def _record(self, method: str, **kwargs) -> None:
        self.calls.append({"method": method, **kwargs})

    async def send_message(self, chat_id, text, *, reply_markup=None, parse_mode="HTML", **_):
        self._record("sendMessage", chat_id=chat_id, text=text, parse_mode=parse_mode,
                     reply_markup=reply_markup)
        self._transcript.append(f"### sendMessage\n\n```html\n{text}\n```\n")
        return {"message_id": len(self.calls)}

    async def send_photo(self, chat_id, photo, *, filename="chart.png", caption=None,
                         parse_mode="HTML"):
        path = self._write_image(filename, photo)
        self._record("sendPhoto", chat_id=chat_id, file=str(path), caption=caption,
                     parse_mode=parse_mode)
        self._transcript.append(
            f"### sendPhoto — `{path.name}`\n\n```html\n{caption or ''}\n```\n"
        )
        return {"message_id": len(self.calls)}

    async def send_media_group(self, chat_id, items, *, caption=None, parse_mode="HTML"):
        if not (MEDIA_GROUP_MIN <= len(items) <= MEDIA_GROUP_MAX):
            raise ValueError(
                f"sendMediaGroup takes {MEDIA_GROUP_MIN}..{MEDIA_GROUP_MAX} items, got {len(items)}"
            )
        paths = [self._write_image(name, payload) for name, payload in items]
        self._record("sendMediaGroup", chat_id=chat_id, files=[str(p) for p in paths],
                     caption=caption, parse_mode=parse_mode, count=len(items))
        listing = "\n".join(f"- `{p.name}`" for p in paths)
        self._transcript.append(
            f"### sendMediaGroup — {len(items)} photos\n\n{listing}\n\n"
            f"Album caption (item 0 only):\n\n```html\n{caption or ''}\n```\n"
        )
        return [{"message_id": len(self.calls)}]

    async def answer_callback_query(self, callback_query_id, *, text=None, show_alert=False):
        self._record("answerCallbackQuery", text=text, show_alert=show_alert)
        return True

    async def edit_message_reply_markup(self, chat_id, message_id, reply_markup):
        self._record("editMessageReplyMarkup", chat_id=chat_id, message_id=message_id,
                     reply_markup=reply_markup)
        return None

    def _write_image(self, filename: str, payload: bytes):
        self._image_index += 1
        path = self.out_dir / f"{self._image_index:03d}-{filename}"
        path.write_bytes(payload)
        return path

    def flush(self) -> None:
        """Write messages.md and calls.json."""
        (self.out_dir / "messages.md").write_text(
            "# Digest dry run\n\n" + "\n".join(self._transcript), encoding="utf-8"
        )
        (self.out_dir / "calls.json").write_text(
            json.dumps(self.calls, indent=2, ensure_ascii=False), encoding="utf-8"
        )
