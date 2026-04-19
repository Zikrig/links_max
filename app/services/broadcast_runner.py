"""Асинхронная рассылка по Broadcast + планировщик AsyncIOScheduler."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import get_settings
from app.db.database import SessionLocal
from app.db.repo import Repo
from app.max_api import MaxApiClient

logger = logging.getLogger(__name__)

_SEND_DELAY_SEC = 0.4

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone=ZoneInfo("UTC"))
    return _scheduler


def schedule_broadcast_job(broadcast_id: int, run_date: datetime) -> None:
    """Запланировать одноразовую отправку по UTC."""
    sch = get_scheduler()
    sch.add_job(
        run_broadcast,
        "date",
        run_date=run_date,
        args=[broadcast_id],
        id=f"broadcast_{broadcast_id}",
        replace_existing=True,
    )


async def reschedule_pending_broadcasts() -> None:
    """После рестарта — восстановить отложенные и просроченные рассылки."""
    db = SessionLocal()
    try:
        repo = Repo(db)
        pending = repo.list_scheduled_broadcasts_with_send_at()
        now = datetime.utcnow()
        sch = get_scheduler()
        for b in pending:
            if not b.send_at:
                continue
            if b.send_at <= now:
                asyncio.create_task(run_broadcast(b.id))
            else:
                sch.add_job(
                    run_broadcast,
                    "date",
                    run_date=b.send_at,
                    args=[b.id],
                    id=f"broadcast_{b.id}",
                    replace_existing=True,
                )
    finally:
        db.close()


def launch_broadcast_now(broadcast_id: int) -> None:
    """Запуск из webhook без блокировки (send now)."""

    async def _wrap() -> None:
        try:
            await run_broadcast(broadcast_id)
        except Exception:
            logger.exception("run_broadcast failed broadcast_id=%s", broadcast_id)

    asyncio.create_task(_wrap())


async def run_broadcast(broadcast_id: int) -> None:
    settings = get_settings()
    api = MaxApiClient(settings.bot_token)
    try:
        db = SessionLocal()
        try:
            repo = Repo(db)
            b0 = repo.get_broadcast(broadcast_id)
            if not b0:
                return
            if b0.status == "cancelled":
                logger.info("run_broadcast: skip id=%s (cancelled)", broadcast_id)
                return
            if b0.send_at and b0.send_at > datetime.utcnow() + timedelta(seconds=3):
                logger.info("run_broadcast: skip id=%s (send_at in future)", broadcast_id)
                return

            b = repo.try_claim_broadcast_for_sending(broadcast_id)
            if not b:
                return

            recipients = repo.list_distinct_lead_user_ids()
            if not recipients:
                repo.update_broadcast_status(broadcast_id, "failed")
                warn = "⚠️ Рассылка не выполнена: нет получателей (в базе нет лидов)."
                # Уведомляем админов из .env и модераторов из БД (как и доступ к админ-боту).
                notify_ids = set(settings.admin_user_ids) | set(repo.list_moderator_user_ids())
                for uid in notify_ids:
                    try:
                        await api.send_message(uid, warn)
                    except Exception as exc:
                        logger.warning("notify staff %s: %s", uid, exc)
                return

            text = b.text
            title = b.title
            button_text = b.button_text
            button_url = b.button_url
            image_stored = b.image_url
        finally:
            db.close()

        body_text = (text or "").strip() or (title or "").strip() or " "

        image_token = await api.resolve_broadcast_image_token(image_stored)
        if image_stored and not image_token:
            logger.warning("broadcast id=%s: изображение не подготовлено (пропуск вложения)", broadcast_id)

        try:
            for uid in recipients:
                try:
                    await api.send_broadcast_message(
                        uid,
                        body_text,
                        button_text,
                        button_url,
                        image_url=image_token,
                    )
                except Exception as exc:
                    logger.warning("broadcast to user_id=%s: %s", uid, exc)
                await asyncio.sleep(_SEND_DELAY_SEC)

            db2 = SessionLocal()
            try:
                repo2 = Repo(db2)
                repo2.mark_broadcast_sent(broadcast_id)
            finally:
                db2.close()
        except Exception:
            dbx = SessionLocal()
            try:
                repo_failed = Repo(dbx)
                repo_failed.update_broadcast_status(broadcast_id, "failed")
            finally:
                dbx.close()
            raise
    finally:
        await api.close()
