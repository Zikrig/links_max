from functools import lru_cache
from typing import Annotated, Set
from urllib.parse import urlparse

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "prod"
    bot_token: str
    webhook_base_url: str
    webhook_path: str = "/max/webhook"
    webhook_secret: str
    admin_user_ids: Annotated[Set[int], NoDecode] = set()
    sqlite_path: str = "/data/max_bot.sqlite3"
    tz: str = "Europe/Moscow"
    personal_data_policy_url: str

    @field_validator("admin_user_ids", mode="before")
    @classmethod
    def parse_admin_ids(cls, value: str | list[int] | set[int]) -> set[int]:
        if isinstance(value, (set, list)):
            return {int(v) for v in value}
        if not value:
            return set()
        raw_items = [item.strip() for item in str(value).split(",") if item.strip()]
        return {int(item) for item in raw_items}

    @property
    def webhook_url(self) -> str:
        return f"{self.webhook_base_url.rstrip('/')}{self.webhook_path}"

    @property
    def normalized_webhook(self) -> tuple[str, str]:
        raw = self.webhook_url.strip()
        parsed = urlparse(raw)
        if parsed.scheme != "https":
            raise ValueError("WEBHOOK URL должен начинаться с https://")
        path = (parsed.path or "").strip()
        if not path or path == "/":
            if not parsed.netloc:
                raise ValueError("WEBHOOK URL: не указан host")
            return f"https://{parsed.netloc}/webhook", "/webhook"
        if not path.startswith("/"):
            path = "/" + path
        return f"https://{parsed.netloc}{path}", path


@lru_cache
def get_settings() -> Settings:
    return Settings()
