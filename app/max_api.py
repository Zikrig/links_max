from __future__ import annotations

import asyncio
import logging
import re
import time

import httpx

logger = logging.getLogger(__name__)

_RATE_LIMIT_TIMEOUT = 300.0  # 5 минут
_MIN_REQUEST_INTERVAL = 0.5  # микрофриз между запросами к MAX API


def _token_from_max_upload_response(body: dict) -> str | None:
    """
    После POST на upload URL MAX может вернуть token в корне или вложить в
    photos / files / images: {"photos": {"<id>": {"token": "..."}}}.
    """
    t = body.get("token")
    if t:
        return str(t).strip()
    for key in ("photos", "files", "images"):
        block = body.get(key)
        if isinstance(block, dict):
            for item in block.values():
                if isinstance(item, dict):
                    tok = item.get("token")
                    if tok:
                        return str(tok).strip()
    return None


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
    # Общий лимитер на процесс: разные экземпляры клиента не должны обгонять друг друга.
    _global_request_lock = asyncio.Lock()
    _global_last_request_ts = 0.0

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
            # Микрофриз: сглаживаем burst и снижаем риск 429.
            async with self.__class__._global_request_lock:
                now = time.monotonic()
                elapsed = now - self.__class__._global_last_request_ts
                if elapsed < _MIN_REQUEST_INTERVAL:
                    await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
                self.__class__._global_last_request_ts = time.monotonic()
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

    async def send_message_with_image_and_keyboard(
        self, user_id: int, text: str, image_token: str, buttons: list
    ) -> None:
        """Текст + вложение image + inline-клавиатура (картинка не дублируется ссылкой в тексте)."""
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        body_text = (text or "").strip() or " "
        kb = {"type": "inline_keyboard", "payload": {"buttons": buttons}}
        payload: dict = {
            "text": body_text,
            "attachments": [
                {"type": "image", "payload": {"token": image_token}},
                kb,
            ],
        }
        for attempt in range(8):
            response = await self._request("POST", "/messages", params=params, json=payload)
            if response.status_code < 400:
                response.raise_for_status()
                return
            err_body = (response.text or "")[:800]
            if response.status_code == 400 and "attachment.not.ready" in err_body and attempt < 7:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            logger.warning(
                "POST /messages image+keyboard failed %s: %s",
                response.status_code,
                err_body,
            )
            response.raise_for_status()

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

    async def upload_image(self, file_bytes: bytes, filename: str) -> str | None:
        """Загрузить изображение (POST /uploads?type=image), вернуть token для вложения."""
        try:
            upload_resp = await self._request("POST", "/uploads", params={"type": "image"})
            upload_resp.raise_for_status()
            meta = upload_resp.json()
            if not isinstance(meta, dict):
                logger.warning("POST /uploads image: не JSON-объект: %s", (upload_resp.text or "")[:500])
                return None
            upload_url = meta.get("url")
            if not upload_url:
                logger.warning("POST /uploads image: нет url: %s", (upload_resp.text or "")[:800])
                return None
            auth = self.client.headers.get("Authorization") or f"Bearer {self._bot_token}"
            up_resp = await self.client.post(
                upload_url,
                files={"data": (filename, file_bytes)},
                headers={"Authorization": auth},
            )
            if up_resp.status_code >= 400:
                logger.warning(
                    "Image upload POST failed %s: %s",
                    up_resp.status_code,
                    (up_resp.text or "")[:800],
                )
                return None
            try:
                body = up_resp.json()
            except Exception as exc:
                logger.warning("Image upload response not JSON: %s", exc)
                return None
            if not isinstance(body, dict):
                return None
            token = _token_from_max_upload_response(body)
            if not token:
                logger.warning("Image upload OK but no token: %s", (up_resp.text or "")[:800])
            return token
        except Exception as exc:
            logger.warning("Image upload failed: %s", exc)
            return None

    async def resolve_broadcast_image_token(self, stored: str | None) -> str | None:
        """Один раз на рассылку: https — скачать и получить token; иначе считаем, что это уже token."""
        if not (stored or "").strip():
            return None
        raw = stored.strip()
        if raw.startswith("//"):
            raw = "https:" + raw
        if raw.startswith("http://") or raw.startswith("https://"):
            filename = "image.jpg"
            try:
                path_part = raw.split("?", 1)[0].rstrip("/").split("/")[-1]
                if path_part and "." in path_part:
                    filename = path_part[-120:]
            except Exception:
                pass
            try:
                img_resp = await self.client.get(raw, follow_redirects=True, timeout=45.0)
                if img_resp.status_code == 200 and img_resp.content:
                    return await self.upload_image(img_resp.content, filename)
            except Exception as exc:
                logger.warning("broadcast: не удалось скачать картинку %s: %s", raw, exc)
            return None
        return raw

    async def send_broadcast_message(
        self,
        user_id: int,
        text: str,
        button_text: str,
        button_url: str,
        image_url: str | None = None,
    ) -> None:
        """
        Рассылка: текст + кнопка; при image_url — вложение image (значение — token MAX,
        заранее получите через resolve_broadcast_image_token).
        При ошибке вложения — только текст + кнопка (без ссылки на картинку в тексте).
        """
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        kb = {
            "type": "inline_keyboard",
            "payload": {
                "buttons": [[{"type": "link", "text": button_text, "url": button_url}]]
            },
        }

        if not (image_url or "").strip():
            await self.send_message_with_button(user_id, text, button_text, button_url)
            return

        token = image_url.strip()

        if token:
            body_text = text.strip() or " "
            payload: dict = {
                "text": body_text,
                "attachments": [
                    {"type": "image", "payload": {"token": token}},
                    kb,
                ],
            }
            for attempt in range(5):
                response = await self._request("POST", "/messages", params=params, json=payload)
                if response.status_code < 400:
                    response.raise_for_status()
                    return
                err_body = (response.text or "")[:800]
                if response.status_code == 400 and "attachment.not.ready" in err_body:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.warning(
                    "POST /messages broadcast (image+keyboard) failed %s: %s",
                    response.status_code,
                    err_body,
                )
                break

        logger.warning(
            "broadcast: не удалось отправить сообщение с картинкой, только текст+кнопка (token=%s)",
            (token[:24] + "…") if len(token) > 24 else token,
        )
        body = text.strip() or " "
        await self.send_message_with_button(user_id, body, button_text, button_url)

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

    @staticmethod
    def _member_dict_user_id(mem: dict) -> int | None:
        if not isinstance(mem, dict):
            return None
        uid = mem.get("user_id")
        if uid is None:
            u = mem.get("user")
            if isinstance(u, dict):
                uid = u.get("user_id")
        if uid is None:
            return None
        try:
            return int(uid)
        except (TypeError, ValueError):
            return None

    async def _fetch_members_by_user_ids(self, chat_id: int, user_id: int) -> list:
        """GET /chats/{id}/members?user_ids=… — надёжнее точечного /members/{uid} при сбоях API."""

        async def _one(cid: int) -> list:
            try:
                response = await self._request(
                    "GET",
                    f"/chats/{cid}/members",
                    params=[("user_ids", user_id)],
                )
                if response.status_code != 200:
                    return []
                data = response.json()
                if not isinstance(data, dict):
                    return []
                return data.get("members") or []
            except Exception:
                return []

        ms = await _one(chat_id)
        if ms:
            return ms
        if chat_id != 0:
            return await _one(-chat_id)
        return []

    async def _fetch_members_page(self, chat_id: int, marker: int | None) -> tuple[list, int | None]:
        params: list = [("count", 100)]
        if marker is not None:
            params.append(("marker", marker))
        try:
            response = await self._request("GET", f"/chats/{chat_id}/members", params=params)
            if response.status_code != 200:
                return [], None
            data = response.json()
            if not isinstance(data, dict):
                return [], None
            members = data.get("members") or []
            next_m = data.get("marker")
            try:
                next_marker = int(next_m) if next_m is not None else None
            except (TypeError, ValueError):
                next_marker = None
            return members, next_marker
        except Exception:
            return [], None

    async def is_user_member_of_channel(self, chat_id: int, user_id: int) -> bool:
        """
        Участник канала/чата или нет. Сначала GET …/members/{user_id}, затем
        …/members?user_ids=… и постраничный обход (как в check_subscribe / MAX_README).
        """
        raw = await self.get_chat_member(chat_id, user_id)
        if raw is not None and not (isinstance(raw, dict) and not raw):
            return True

        filtered = await self._fetch_members_by_user_ids(chat_id, user_id)
        if filtered:
            logger.debug("member check: user_ids filter matched chat_id=%s user_id=%s", chat_id, user_id)
            return True

        for cid in (chat_id, -chat_id) if chat_id != 0 else (chat_id,):
            marker: int | None = None
            for _ in range(40):
                members, marker = await self._fetch_members_page(cid, marker)
                for mem in members:
                    if self._member_dict_user_id(mem) == user_id:
                        logger.debug("member check: page scan matched chat_id=%s user_id=%s", cid, user_id)
                        return True
                if marker is None:
                    break
        return False

    async def upload_file(self, file_bytes: bytes, filename: str) -> str | None:
        """Загрузить файл в MAX, вернуть token для использования в сообщении."""
        # См. https://dev.max.ru/docs-api/methods/POST/uploads — загрузка по url:
        # POST multipart/form-data, поле data, заголовок Authorization как у API.
        try:
            upload_resp = await self._request("POST", "/uploads", params={"type": "file"})
            upload_resp.raise_for_status()
            meta = upload_resp.json()
            if not isinstance(meta, dict):
                logger.warning("POST /uploads: не JSON-объект: %s", (upload_resp.text or "")[:500])
                return None
            upload_url = meta.get("url")
            if not upload_url:
                logger.warning("POST /uploads: нет url в ответе: %s", (upload_resp.text or "")[:800])
                return None
            auth = self.client.headers.get("Authorization") or f"Bearer {self._bot_token}"
            up_resp = await self.client.post(
                upload_url,
                files={"data": (filename, file_bytes)},
                headers={"Authorization": auth},
            )
            if up_resp.status_code >= 400:
                logger.warning(
                    "Upload POST failed %s: %s",
                    up_resp.status_code,
                    (up_resp.text or "")[:800],
                )
                return None
            try:
                body = up_resp.json()
            except Exception as exc:
                logger.warning("Upload response not JSON: %s", exc)
                return None
            if not isinstance(body, dict):
                return None
            token = _token_from_max_upload_response(body)
            if not token:
                logger.warning("Upload OK but no token in body: %s", (up_resp.text or "")[:800])
            return token
        except Exception as exc:
            logger.warning("File upload failed: %s", exc)
            return None

    async def send_file(self, user_id: int, token: str, caption: str = "") -> None:
        """Отправить загруженный файл пользователю."""
        params = {"user_id": user_id} if user_id > 0 else {"chat_id": user_id}
        payload: dict = {
            "text": (caption.strip() or " "),
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }
        # MAX: сразу после POST на upload URL файл может ещё обрабатываться — 400 attachment.not.ready
        for attempt in range(8):
            response = await self._request("POST", "/messages", params=params, json=payload)
            if response.status_code < 400:
                response.raise_for_status()
                return
            err_body = (response.text or "")[:800]
            if response.status_code == 400 and "attachment.not.ready" in err_body and attempt < 7:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            logger.warning(
                "POST /messages file failed %s: %s",
                response.status_code,
                err_body,
            )
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
