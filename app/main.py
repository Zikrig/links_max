from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from sqlalchemy import text

from app.config import get_settings
from app.db.database import Base, engine
from app.max_api import MaxApiClient
from app.routers.admin import router as admin_router
from app.routers.user import router as user_router
from app.webhook import router as webhook_router

logger = logging.getLogger(__name__)

_MIGRATIONS = [
    "ALTER TABLE leads ADD COLUMN max_name VARCHAR(255)",
    "ALTER TABLE leads ADD COLUMN max_username VARCHAR(120)",
]


def _run_migrations() -> None:
    with engine.connect() as conn:
        for sql in _MIGRATIONS:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # колонка уже существует


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    _run_migrations()
    settings = get_settings()
    max_api = MaxApiClient(settings.bot_token)
    subscribed = False
    webhook_url = ""
    try:
        webhook_url, _webhook_path = settings.normalized_webhook
        await max_api.subscribe_webhook(webhook_url, settings.webhook_secret)
        subscribed = True
        logger.info("Webhook subscribed: %s", webhook_url)
    except Exception as exc:
        logger.error("Webhook subscribe failed: %s", exc)
    try:
        yield
    finally:
        if subscribed and webhook_url:
            try:
                await max_api.unsubscribe_webhook(webhook_url)
                logger.info("Webhook unsubscribed: %s", webhook_url)
            except Exception as exc:
                logger.warning("Webhook unsubscribe failed: %s", exc)
        await max_api.close()


app = FastAPI(title="MAX Lead Bot", lifespan=lifespan)

# Важен порядок: сначала admin роуты, потом user.
app.include_router(admin_router)
app.include_router(user_router)
app.include_router(webhook_router)
