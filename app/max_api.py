from __future__ import annotations

import asyncio
import logging
import re
import time
from urllib.parse import quote, unquote, urlparse

import httpx

logger = logging.getLogger(__name__)

_RATE_LIMIT_TIMEOUT = 300.0  # 5 минут


class RateLimitError(Exception):
    """MAX API вернул 429 и исчерпан лимит ожидания."""


def _chat_link_candidates(raw: str) -> list[str]:
    """Фрагменты из URL для GET /chats/{link} (см. документацию MAX)."""
    raw = raw.strip()
    if not raw:
        return []
    if re.fullmatch(r"-?\d+", raw):
        return []
    out: list[str] = []
    url = raw
    if not re.match(r"^https?://", raw, re.I):
        if "/" not in raw and "?" not in raw:
            out.append(raw.lstrip("@"))
            return list(dict.fromkeys(out))
        url = "https://" + raw.lstrip("/")
    try:
        p = urlparse(url)
        segs = [unquote(s) for s in p.path.strip("/").split("/") if s]
        for seg in reversed(segs):
            seg = seg.split("?")[0].strip()
            if not seg or re.fullmatch(r"-?\d+", seg):
                continue
            out.append(seg.lstrip("@"))
    except Exception:
        pass
    if not out:
        t = raw.lstrip("@").strip()
        if t and not re.fullmatch(r"-?\d+", t):
            out.append(t)
    return list(dict.fromkeys(out))


def _chat_id_from_payload(data: dict) -> int | None:
    for k in ("chat_id", "id"):
        v = data.get(k)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return None


class MaxApiClient:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token
        self._auth_mode = "bearer"
        self.client = httpx.AsyncClient(
            base_url="https://botapi.max.ru",
            headers={"Authorization": f"Bearer {bot_token}"},
            timeout=20.0,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _request(
        self, method: str, path: str, *, fast_fail_429: bool = False, **kwargs
    ) -> httpx.Response:
        """
        fast_fail_429=True — при 429 не ждать, сразу вернуть ответ.
        Используется для edit/ack чтобы webhook возвращал 200 быстро.
        """
        deadline = time.monotonic() + _RATE_LIMIT_TIMEOUT
        attempt = 0
        while True:
            response = await self.client.request(method, path, **kwargs)

            if response.status_code == 429:
                if fast_fail_429:
                    logger.warning("429 на %s %s — fast_fail, не ждём", method, path)
                    return response
                retry_after = float(response.headers.get("Retry-After", min(2 ** attempt, 60)))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RateLimitError(
                        f"MAX API rate limit: 429 на {method} {path} после {_RATE_LIMIT_TIMEOUT:.0f}с ожидания"
                    )
                wait = min(retry_after, remaining)
                logger.warning("429 Too Many Requests — ждём %.1fs (осталось %.0fs)", wait, remaining)
                await asyncio.sleep(wait)
                attempt += 1
                continue

            if response.status_code == 401 and self._auth_mode == "bearer":
                self.client.headers["Authorization"] = self._bot_token
                self._auth_mode = "raw"
                continue

            return response

    async def subscribe_webhook(self, url: str, secret: str | None = None) -> None:
        payload: dict = {
            "url": url,
            "update_types": ["message_created", "message_callback", "bot_started"],
        }
        if secret:
            payload["secret"] = secret
        response = await self._request("POST", "/subscriptions", json=payload)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and data.get("success") is False:
            raise RuntimeError(data.get("message") or "MAX /subscriptions success=false")

    async def unsubscribe_webhook(self, url: str) -> None:
        response = await self._request("DELETE", "/subscriptions", params={"url": url})
        response.raise_for_status()

    async def send_message(self, user_id: int, text: str) -> None:
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        response = await self._request("POST", "/messages", params=params, json={"text": text})
        response.raise_for_status()

    async def send_message_with_keyboard(self, user_id: int, text: str, buttons: list) -> dict | None:
        """Отправить сообщение с inline-клавиатурой. buttons — list of rows (list of dicts)."""
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        payload: dict = {
            "text": text,
            "attachments": [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
        }
        response = await self._request("POST", "/messages", params=params, json=payload)
        if response.status_code >= 400:
            logger.warning(
                "POST /messages keyboard failed %s: %s",
                response.status_code,
                (response.text or "")[:800],
            )
        response.raise_for_status()
        return response.json().get("message")

    async def send_message_with_button(self, user_id: int, text: str, button_text: str, button_url: str) -> None:
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        payload: dict = {
            "text": text,
            "attachments": [
                {
                    "type": "inline_keyboard",
                    "payload": {
                        "buttons": [[{"type": "link", "text": button_text, "url": button_url}]]
                    },
                }
            ],
        }
        response = await self._request("POST", "/messages", params=params, json=payload)
        response.raise_for_status()

    async def get_me(self) -> dict:
        """Получить информацию о боте (user_id и др.)."""
        try:
            resp = await self._request("GET", "/me")
            return resp.json() if resp.status_code == 200 else {}
        except Exception:
            return {}

    async def get_chat_by_link_token(self, token: str) -> dict | None:
        """GET /chats/{link} — публичный идентификатор чата из ссылки."""
        t = token.strip().lstrip("@")
        if not t:
            return None
        path = quote(t, safe="")
        try:
            resp = await self._request("GET", f"/chats/{path}")
            if resp.status_code != 200:
                return None
            body = resp.json()
            if not isinstance(body, dict):
                return None
            if isinstance(body.get("chat"), dict):
                return body["chat"]
            return body
        except Exception as exc:
            logger.debug("get_chat_by_link_token %r: %s", token, exc)
            return None

    async def resolve_chat_from_invite_url(self, raw: str) -> tuple[bool, int | None, str]:
        """
        Найти chat_id по ссылке-приглашению или публичной ссылке на канал.
        Возвращает (успех, chat_id, title_или_текст_ошибки).
        """
        candidates = _chat_link_candidates(raw)
        if not candidates:
            return False, None, "Отправьте ссылку на канал или приглашение, а не число (chat_id)."
        for cand in candidates:
            data = await self.get_chat_by_link_token(cand)
            if not data:
                continue
            cid = _chat_id_from_payload(data)
            title = str(data.get("title") or data.get("name") or cand)
            if cid is not None:
                return True, cid, title
        return False, None, (
            "Не удалось определить канал по ссылке. Проверьте ссылку и что бот видит этот канал в MAX."
        )

    async def check_chat_access(self, chat_id: int) -> tuple[bool, str]:
        """Проверить доступ бота к чату. Возвращает (ok, title/описание_ошибки)."""
        try:
            resp = await self._request("GET", f"/chats/{chat_id}")
            if resp.status_code == 200:
                data = resp.json()
                title = data.get("title") or data.get("chat_id") or str(chat_id)
                return True, str(title)
            if resp.status_code == 403:
                return False, "Бот не является участником канала/чата. Добавьте бота в канал."
            if resp.status_code == 404:
                return False, "Канал/чат не найден. Проверьте chat_id."
            return False, f"Ошибка доступа: HTTP {resp.status_code}"
        except Exception as e:
            return False, f"Ошибка проверки: {e}"

    async def check_bot_is_channel_admin(self, chat_id: int) -> tuple[bool, str]:
        """
        Проверить что бот является администратором канала.
        Возвращает (ok, title_канала) или (False, описание_ошибки).
        """
        me = await self.get_me()
        bot_user_id = me.get("user_id") or me.get("id")
        if not bot_user_id:
            return False, "Не удалось получить user_id бота."

        ok, title = await self.check_chat_access(chat_id)
        if not ok:
            return False, title

        member = await self.get_chat_member(chat_id, int(bot_user_id))
        if member is None:
            return False, "Бот не является участником канала. Добавьте бота в канал."

        role = str(member.get("role", "")).lower()
        if role not in ("admin", "owner", "creator"):
            return False, (
                f"Бот в канале, но не администратор (роль: «{role or '—'}»). "
                "Выдайте боту права администратора — иначе нельзя проверять подписку."
            )
        return True, title

    async def get_chat_member(self, chat_id: int, user_id: int) -> dict | None:
        """Проверить членство user_id в чате/канале. None = не в чате или ошибка."""
        try:
            response = await self._request("GET", f"/chats/{chat_id}/members/{user_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None

    async def upload_file(self, file_bytes: bytes, filename: str) -> str | None:
        """Загрузить файл в MAX, вернуть token для использования в сообщении."""
        try:
            upload_resp = await self._request("POST", "/uploads", params={"type": "file"})
            upload_resp.raise_for_status()
            upload_url = upload_resp.json().get("url")
            if not upload_url:
                return None
            put_resp = await self.client.put(
                upload_url,
                content=file_bytes,
                headers={"Content-Type": "application/octet-stream", "Content-Disposition": f'attachment; filename="{filename}"'},
            )
            put_resp.raise_for_status()
            token = put_resp.json().get("token")
            return token
        except Exception as exc:
            logger.warning("File upload failed: %s", exc)
            return None

    async def send_file(self, user_id: int, token: str, caption: str = "") -> None:
        """Отправить загруженный файл пользователю."""
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        payload: dict = {
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }
        if caption:
            payload["text"] = caption
        response = await self._request("POST", "/messages", params=params, json=payload)
        response.raise_for_status()

    async def answer_callback_with_edit(
        self, callback_id: str, text: str, buttons: list | None = None
    ) -> bool:
        """
        Обновить сообщение через POST /answers с message-payload.
        Это атомарно (ack + edit в одном запросе) и обычно не попадает
        под тот же rate-limit что PUT /messages.
        Возвращает True если успешно.
        """
        msg_body: dict = {"text": text}
        if buttons:
            msg_body["attachments"] = [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]
        else:
            msg_body["attachments"] = []
        try:
            response = await self._request(
                "POST", "/answers",
                fast_fail_429=True,
                params={"callback_id": callback_id},
                json={"message": msg_body, "notification": " "},
            )
            if response.status_code in (200, 204):
                return True
            logger.warning("answer_callback_with_edit failed %s: %s", response.status_code, response.text[:300])
            return False
        except Exception as exc:
            logger.warning("answer_callback_with_edit exception: %s", exc)
            return False

    async def edit_message(self, message_id: str, text: str, buttons: list | None = None) -> bool:
        """
        Редактировать сообщение через PUT /messages. Fast-fail на 429.
        Используется как fallback если answer_callback_with_edit недоступен.
        """
        body: dict = {"text": text}
        if buttons:
            body["attachments"] = [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]
        else:
            body["attachments"] = []
        try:
            response = await self._request(
                "PUT", "/messages",
                fast_fail_429=True,
                params={"message_id": message_id},
                json=body,
            )
            if response.status_code in (200, 204):
                return True
            logger.warning("edit_message failed %s body=%s", response.status_code, response.text[:300])
            return False
        except Exception as exc:
            logger.warning("edit_message exception: %s", exc)
            return False

    async def answer_callback(self, callback_id: str, notification: str = " ") -> None:
        """Подтвердить callback без изменения сообщения. Fast-fail на 429."""
        try:
            await self._request(
                "POST", "/answers",
                fast_fail_429=True,
                params={"callback_id": callback_id},
                json={"notification": notification},
            )
        except Exception:
            pass  # ack не критичен
