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
