from contextlib import asynccontextmanager
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from sqlalchemy import text

from app.config import get_settings
from app.db.database import Base, engine
from app.max_api import MaxApiClient
from app.routers.admin import router as admin_router
from app.routers.user import router as user_router
from app.services.broadcast_runner import get_scheduler, reschedule_pending_broadcasts
from app.webhook import router as webhook_router

logger = logging.getLogger(__name__)

_MIGRATIONS = [
    "ALTER TABLE leads ADD COLUMN max_name VARCHAR(255)",
    "ALTER TABLE leads ADD COLUMN max_username VARCHAR(120)",
    "ALTER TABLE offers ADD COLUMN base_url TEXT DEFAULT ''",
    "ALTER TABLE offers ADD COLUMN subid_param VARCHAR(80) DEFAULT ''",
    # channel_chat_id / channel_title больше не используются (заменены ScenarioChannel),
    # но миграции безвредны для существующих БД
    "ALTER TABLE scenarios ADD COLUMN channel_chat_id INTEGER",
    "ALTER TABLE scenarios ADD COLUMN channel_title VARCHAR(200)",
    "ALTER TABLE scenarios ADD COLUMN check_subscription BOOLEAN DEFAULT 0",
    (
        "CREATE TABLE IF NOT EXISTS scenario_channels ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "scenario_id INTEGER NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE, "
        "chat_id INTEGER NOT NULL, "
        "title VARCHAR(200) NOT NULL, "
        "invite_link VARCHAR(255)"
        ")"
    ),
]


def _fix_scenarios_description_nullable(conn) -> None:
    """SQLite не поддерживает ALTER COLUMN — пересоздаём таблицу если description NOT NULL."""
    rows = conn.execute(text("PRAGMA table_info(scenarios)")).fetchall()
    for row in rows:
        # row: (cid, name, type, notnull, dflt_value, pk)
        if row[1] == "description" and row[3] == 1:  # notnull=1 => нужна миграция
            for sql in [
                "CREATE TABLE scenarios_new ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "offer_id INTEGER NOT NULL REFERENCES offers(id), "
                "code VARCHAR(80) NOT NULL UNIQUE, "
                "title VARCHAR(200) NOT NULL, "
                "description TEXT, "
                "image_url TEXT, "
                "check_subscription BOOLEAN NOT NULL DEFAULT 0, "
                "created_at DATETIME)",
                "INSERT INTO scenarios_new "
                "SELECT id, offer_id, code, title, description, image_url, check_subscription, created_at "
                "FROM scenarios",
                "DROP TABLE scenarios",
                "ALTER TABLE scenarios_new RENAME TO scenarios",
            ]:
                conn.execute(text(sql))
            conn.commit()
            logger.info("Migration applied: scenarios.description made nullable")
            break


def _run_migrations() -> None:
    with engine.connect() as conn:
        for migration in _MIGRATIONS:
            try:
                if isinstance(migration, list):
                    for sql in migration:
                        conn.execute(text(sql))
                else:
                    conn.execute(text(migration))
                conn.commit()
            except Exception:
                pass  # колонка уже существует или миграция уже применена
        _fix_scenarios_description_nullable(conn)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    now_local = datetime.now(ZoneInfo(settings.tz))
    now_utc = now_local.astimezone(ZoneInfo("UTC"))
    logger.info(
        "Время на сервере при старте: %s (%s), %s UTC",
        now_local.strftime("%Y-%m-%d %H:%M:%S"),
        settings.tz,
        now_utc.strftime("%Y-%m-%d %H:%M:%S"),
    )

    Base.metadata.create_all(bind=engine)
    _run_migrations()
    max_api = MaxApiClient(settings.bot_token)
    subscribed = False
    webhook_url = ""
    if not settings.bot_username:
        me = await max_api.get_me()
        logger.info("Bot /me response: %s", me)
        username = me.get("username") or me.get("login") or me.get("name") or ""
        if username:
            settings.bot_username = username
            logger.info("Bot username resolved from API: %s", username)
        else:
            logger.warning("Could not resolve bot username from /me: %s", me)

    try:
        webhook_url, _webhook_path = settings.normalized_webhook
        await max_api.subscribe_webhook(webhook_url, settings.webhook_secret)
        subscribed = True
        logger.info("Webhook subscribed: %s", webhook_url)
    except Exception as exc:
        logger.error("Webhook subscribe failed: %s", exc)

    scheduler = get_scheduler()
    scheduler.start()
    try:
        await reschedule_pending_broadcasts()
    except Exception as exc:
        logger.error("Broadcast scheduler recovery failed: %s", exc)

    try:
        yield
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception as exc:
            logger.warning("Scheduler shutdown: %s", exc)
        if subscribed and webhook_url:
            try:
                await max_api.unsubscribe_webhook(webhook_url)
                logger.info("Webhook unsubscribed: %s", webhook_url)
            except Exception as exc:
                logger.warning("Webhook unsubscribe failed: %s", exc)
        await max_api.close()


logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

app = FastAPI(title="MAX Lead Bot", lifespan=lifespan)

# Важен порядок: сначала admin роуты, потом user.
app.include_router(admin_router)
app.include_router(user_router)
app.include_router(webhook_router)
