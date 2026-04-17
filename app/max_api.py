from __future__ import annotations

import httpx


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

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        response = await self.client.request(method, path, **kwargs)
        if response.status_code != 401 or self._auth_mode == "raw":
            return response
        # Некоторые инсталляции MAX Bot API принимают токен без Bearer-префикса.
        self.client.headers["Authorization"] = self._bot_token
        self._auth_mode = "raw"
        return await self.client.request(method, path, **kwargs)

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

    async def answer_callback(self, callback_id: str, notification: str = " ") -> None:
        """Подтвердить callback без изменения сообщения (безопасный ack из MAX_README)."""
        response = await self._request(
            "POST", "/answers",
            params={"callback_id": callback_id},
            json={"message": None, "notification": notification},
        )
        if response.status_code not in (200, 204):
            pass  # не падаем — ack не критичен
