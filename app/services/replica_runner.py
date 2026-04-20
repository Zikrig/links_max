"""Отложенное пост-сообщение оффера (через 5 минут после выдачи ссылки)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.config import get_settings
from app.db.database import SessionLocal
from app.db.repo import Repo
from app.max_api import MaxApiClient
from app.services.broadcast_runner import get_scheduler
from app.db.models import Offer

logger = logging.getLogger(__name__)

_AFTER_DELAY = timedelta(minutes=5)


def schedule_offer_post_message(user_id: int, offer_id: int) -> None:
    """Одна отложенная отправка пост-сообщения на пользователя; новый вызов переносит время."""
    run_at = datetime.now(timezone.utc) + _AFTER_DELAY
    get_scheduler().add_job(
        run_offer_post_message_job,
        "date",
        run_date=run_at,
        args=[user_id, offer_id],
        id=f"offer_post_{user_id}",
        replace_existing=True,
    )


async def run_offer_post_message_job(user_id: int, offer_id: int) -> None:
    settings = get_settings()
    api = MaxApiClient(settings.bot_token)
    try:
        db = SessionLocal()
        try:
            repo = Repo(db)
            offer = db.get(Offer, offer_id)
            if not offer or not offer.post_enabled:
                return
            body = (offer.post_text or "").strip() or f"Предложение по офферу «{offer.name}»"
            btn_text = (offer.post_button_text or "").strip() or "Перейти к акции"
            btn_url = (offer.post_button_url or "").strip()
            if not btn_url:
                return
            buttons = [[{"type": "link", "text": btn_text, "url": btn_url}]]
            image_ref = (offer.post_image_url or "").strip()
            if image_ref:
                token = await api.resolve_broadcast_image_token(image_ref)
                if token:
                    await api.send_message_with_image_and_keyboard(user_id, body, token, buttons)
                    return
            await api.send_message_with_keyboard(user_id, body, buttons)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("offer_post user_id=%s offer_id=%s: %s", user_id, offer_id, exc)
    finally:
        await api.close()
