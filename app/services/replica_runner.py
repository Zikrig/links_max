"""Отложенная реплика «после акции» (через 5 минут после выдачи финальной ссылки)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.config import get_settings
from app.db.database import SessionLocal
from app.db.repo import Repo
from app.max_api import MaxApiClient
from app.services.broadcast_runner import get_scheduler
from app.services.replica_messages import (
    DEFAULT_REPLICA_AFTER_LINK,
    send_replica_with_offers,
)

logger = logging.getLogger(__name__)

_AFTER_DELAY = timedelta(minutes=5)


def schedule_after_link_replica(user_id: int) -> None:
    """Одна отложенная отправка на пользователя; новый вызов переносит время."""
    run_at = datetime.now(timezone.utc) + _AFTER_DELAY
    get_scheduler().add_job(
        run_after_link_replica_job,
        "date",
        run_date=run_at,
        args=[user_id],
        id=f"replica_after_{user_id}",
        replace_existing=True,
    )


async def run_after_link_replica_job(user_id: int) -> None:
    settings = get_settings()
    api = MaxApiClient(settings.bot_token)
    try:
        db = SessionLocal()
        try:
            repo = Repo(db)
            rs = repo.get_replica_settings()
            text = (rs.after_link_text or "").strip() or DEFAULT_REPLICA_AFTER_LINK
            await send_replica_with_offers(api, repo, settings, user_id, body_text=text)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("replica_after user_id=%s: %s", user_id, exc)
    finally:
        await api.close()
