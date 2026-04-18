from __future__ import annotations

import asyncio
import logging
import re
import time

import httpx

logger = logging.getLogger(__name__)

_RATE_LIMIT_TIMEOUT = 300.0  # 5 минут


class RateLimitError(Exception):
    """MAX API вернул 429 и исчерпан лимит ожидания."""


def normalize_max_url(url: str) -> str:
    """Как в max_users_resend: единый вид ссылки для сравнения."""
    u = (url or "").strip()
    if not u.startswith("http"):
        u = "https://" + u
    return u.rstrip("/")


def extract_join_token(url: str) -> str:
    m = re.search(r"/join/([^/?#]+)", url, re.IGNORECASE)
    return m.group(1) if m else ""


def links_match(a: str, b: str) -> bool:
    return normalize_max_url(a).lower() == normalize_max_url(b).lower()


def try_parse_chat_id_from_text(text: str) -> int | None:
    """Число или max.ru/c/-123/... в тексте ссылки."""
    raw = text.strip()
    if re.fullmatch(r"-?\d+", raw):
        try:
            return int(raw)
        except ValueError:
            return None
    m = re.search(r"/c/(-?\d+)", raw)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _unwrap_chat_dict(data: dict | None) -> dict | None:
    if not data:
        return None
    if isinstance(data.get("chat"), dict):
        return data["chat"]
    return data


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

    async def fetch_chat_by_id(self, chat_id: int) -> dict | None:
        """GET /chats/{chat_id} — как в max_users_resend."""
        try:
            resp = await self._request("GET", f"/chats/{chat_id}")
            if resp.status_code != 200:
                logger.debug("GET /chats/%s -> %s", chat_id, resp.status_code)
                return None
            body = resp.json()
            if not isinstance(body, dict):
                return None
            return _unwrap_chat_dict(body)
        except Exception as exc:
            logger.warning("fetch_chat_by_id %s: %s", chat_id, exc)
            return None

    async def find_chat_by_invite_url(self, url: str) -> tuple[int | None, dict | None, str]:
        """
        Список чатов бота GET /chats (с marker), сопоставление link и токена /join/...
        Логика скопирована из max_users_resend.Config.find_chat_by_invite_url.
        """
        norm = normalize_max_url(url)
        token = extract_join_token(norm)
        marker: int | None = None
        while True:
            params: dict = {"count": 100}
            if marker is not None:
                params["marker"] = marker
            try:
                resp = await self._request("GET", "/chats", params=params)
                if resp.status_code != 200:
                    return None, None, f"Не удалось получить список чатов (HTTP {resp.status_code})."
                data = resp.json()
            except Exception as e:
                return None, None, f"Ошибка запроса списка чатов: {e}"
            chats = data.get("chats") or []
            if not isinstance(chats, list):
                chats = []
            for c in chats:
                if not isinstance(c, dict):
                    continue
                cid_raw = c.get("chat_id")
                clink = (c.get("link") or "").strip()
                if clink and links_match(clink, norm):
                    try:
                        cid = int(cid_raw) if cid_raw is not None else None
                    except (TypeError, ValueError):
                        cid = None
                    if cid is not None:
                        return cid, c, ""
                if token and clink:
                    if extract_join_token(clink) == token:
                        try:
                            cid = int(cid_raw) if cid_raw is not None else None
                        except (TypeError, ValueError):
                            cid = None
                        if cid is not None:
                            return cid, c, ""
            next_m = data.get("marker")
            if next_m is None or not chats:
                break
            try:
                marker = int(next_m)
            except (TypeError, ValueError):
                break
        return None, None, (
            "Канал не найден среди чатов бота. Добавьте бота в канал как администратора "
            "и пришлите ссылку-приглашение ещё раз."
        )

    async def resolve_chat_from_invite_url(self, raw: str) -> tuple[bool, int | None, str]:
        """
        Найти chat_id по ссылке (как max_users_resend.resolve_chat_from_input).
        Возвращает (успех, chat_id, title_или_текст_ошибки).
        """
        raw = raw.strip()
        if not raw:
            return False, None, "Отправьте ссылку на канал или приглашение."

        maybe_id = try_parse_chat_id_from_text(raw)
        if maybe_id is not None:
            info = await self.fetch_chat_by_id(maybe_id)
            if info:
                cid = _chat_id_from_payload(info) or maybe_id
                title = str(info.get("title") or info.get("name") or str(cid))
                return True, cid, title
            return False, None, "Канал не найден по id из ссылки. Проверьте, что бот состоит в этом чате."

        url_raw = raw if raw.startswith("http") else "https://" + raw.lstrip("/")
        cid, info, err = await self.find_chat_by_invite_url(url_raw)
        if cid is None or info is None:
            return False, None, err
        title = str(info.get("title") or info.get("name") or str(cid))
        return True, cid, title

    async def check_chat_access(self, chat_id: int) -> tuple[bool, str]:
        """Проверить доступ бота к чату. Возвращает (ok, title/описание_ошибки)."""
        try:
            resp = await self._request("GET", f"/chats/{chat_id}")
            if resp.status_code == 200:
                data = resp.json()
                u = _unwrap_chat_dict(data) or data
                title = u.get("title") or u.get("name") or u.get("chat_id") or str(chat_id)
                return True, str(title)
            if resp.status_code == 403:
                return False, "Бот не является участником канала/чата. Добавьте бота в канал."
            if resp.status_code == 404:
                # Часто в MAX chat_id приходит с другим знаком — пробуем инверсию
                if chat_id != 0:
                    alt = -chat_id
                    resp2 = await self._request("GET", f"/chats/{alt}")
                    if resp2.status_code == 200:
                        data = resp2.json()
                        u = _unwrap_chat_dict(data) or data
                        title = u.get("title") or u.get("name") or str(alt)
                        return True, str(title)
                return False, "Канал/чат не найден. Проверьте chat_id."
            return False, f"Ошибка доступа: HTTP {resp.status_code}"
        except Exception as e:
            return False, f"Ошибка проверки: {e}"

    async def get_bot_membership(self, chat_id: int) -> tuple[dict | None, str, int | None]:
        """
        GET /chats/{chat_id}/members/me — как в max_users_resend (надёжнее, чем /members/{user_id}).
        Возвращает (membership, ошибка, chat_id с которым сработал запрос — для сохранения в БД).
        """
        async def _one(cid: int) -> tuple[dict | None, int]:
            try:
                r = await self._request("GET", f"/chats/{cid}/members/me")
                if r.status_code != 200:
                    return None, r.status_code
                data = r.json()
                if not isinstance(data, dict):
                    return None, r.status_code
                return data, r.status_code
            except Exception as exc:
                logger.warning("GET /chats/%s/members/me: %s", cid, exc)
                return None, 0

        m, code = await _one(chat_id)
        if m is not None:
            return m, "", chat_id
        if chat_id != 0:
            m2, _ = await _one(-chat_id)
            if m2 is not None:
                return m2, "", -chat_id
        return None, f"Не удалось получить участие бота в канале (HTTP {code}). Добавьте бота в канал.", None

    def _membership_allows_channel_admin(self, m: dict) -> bool:
        """Поля как в max_users_resend: is_owner / is_admin / role / permissions."""
        if m.get("is_owner"):
            return True
        if m.get("is_admin"):
            return True
        role = str(m.get("role", "") or "").lower()
        if role in ("admin", "owner", "creator", "administrator", "channel_admin"):
            return True
        perms = m.get("permissions")
        if isinstance(perms, list) and perms:
            ps = {str(x).lower() for x in perms}
            if ps & {"admin", "all", "owner"}:
                return True
        return False

    async def check_bot_is_channel_admin(self, chat_id: int) -> tuple[bool, str, int | None]:
        """
        Проверить что бот — администратор канала (через /members/me, как max_users_resend).
        Возвращает (ok, title_канала или ошибка, chat_id для БД если отличается от входного — знак MAX).
        """
        ok, title = await self.check_chat_access(chat_id)
        if not ok:
            return False, title, None

        member, err, eff_id = await self.get_bot_membership(chat_id)
        if member is None:
            return False, err or "Не удалось проверить участие бота в канале.", None

        if not self._membership_allows_channel_admin(member):
            role = str(member.get("role", "") or "—")
            return False, (
                f"Бот в канале, но недостаточно прав для проверки подписки (роль: «{role}»). "
                "Назначьте бота администратором канала."
            ), None
        store_id = eff_id if eff_id is not None else chat_id
        return True, title, store_id

    async def get_chat_member(self, chat_id: int, user_id: int) -> dict | None:
        """Проверить членство user_id в чате/канале. None = не в чате или ошибка. Пробуем оба знака chat_id."""
        async def _one(cid: int) -> dict | None:
            try:
                response = await self._request("GET", f"/chats/{cid}/members/{user_id}")
                if response.status_code == 200:
                    return response.json()
                return None
            except Exception:
                return None

        m = await _one(chat_id)
        if m is not None:
            return m
        if chat_id != 0:
            return await _one(-chat_id)
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
