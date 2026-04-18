"""Реплики с подборкой последних офферов (кнопки-ссылки на сценарии)."""

from __future__ import annotations

from app.config import Settings
from app.db.repo import Repo
from app.max_api import MaxApiClient

DEFAULT_REPLICA_STRANGER = "Привет, у нас есть для вас пара акций."
DEFAULT_REPLICA_AFTER_LINK = "У нас есть еще пара предложений для вас!"

_OFFER_BTN_MAX_LEN = 64
_N_OFFERS = 10


def _btn_link(text: str, url: str) -> dict:
    return {"type": "link", "text": text, "url": url}


def _truncate_label(s: str, max_len: int = _OFFER_BTN_MAX_LEN) -> str:
    t = (s or "").strip()
    if len(t) <= max_len:
        return t or "Оффер"
    return t[: max_len - 1] + "…"


def _resolve_offer_entry_url(repo: Repo, settings: Settings, offer_id: int) -> str | None:
    scenario = repo.get_scenario_for_offer(offer_id)
    if not scenario:
        return None
    bl = repo.get_bot_link_for_scenario(scenario.id)
    if bl and (bl.deep_link or "").strip():
        return bl.deep_link.strip()
    if settings.bot_username:
        return f"https://max.ru/{settings.bot_username}?start={scenario.code}"
    return f"https://max.ru/start?start={scenario.code}"


def build_replica_offers_keyboard(repo: Repo, settings: Settings, limit: int = _N_OFFERS) -> list:
    offers = repo.list_offers_recent(limit)
    rows: list = []
    for offer in offers:
        url = _resolve_offer_entry_url(repo, settings, offer.id)
        if not url:
            continue
        rows.append([_btn_link(_truncate_label(offer.name), url)])
    return rows


async def send_replica_with_offers(
    api: MaxApiClient,
    repo: Repo,
    settings: Settings,
    user_id: int,
    *,
    body_text: str,
) -> None:
    body = (body_text or "").strip() or " "
    buttons = build_replica_offers_keyboard(repo, settings)
    if buttons:
        await api.send_message_with_keyboard(user_id, body, buttons)
    else:
        await api.send_message(user_id, body)
