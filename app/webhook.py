import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.repo import Repo
from app.keyboards.admin import (
    admin_bot_links_keyboard,
    admin_channels_keyboard,
    admin_export_platforms_keyboard,
    admin_main_keyboard,
    admin_offers_keyboard,
    admin_platforms_keyboard,
    admin_scenarios_keyboard,
    build_keyboard_attachment,
)
from app.max_api import MaxApiClient

router = APIRouter(tags=["webhook"])
logger = logging.getLogger(__name__)


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/wh_links_8081")
def webhook_info():
    return {
        "ok": True,
        "webhook": True,
        "detail": "MAX отправляет события POST-запросом на этот endpoint.",
    }


def _extract_event(payload: dict) -> tuple[int, str, str, str]:
    """Возвращает (user_id, text, update_type, callback_id)."""
    update_type = payload.get("update_type", "")
    if update_type == "message_created":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        body = msg.get("body", {}) or {}
        return int(sender.get("user_id") or 0), str(body.get("text", "")).strip(), update_type, ""
    if update_type == "message_callback":
        cb = payload.get("callback", {}) or {}
        user = cb.get("user", {}) or {}
        return int(user.get("user_id") or 0), str(cb.get("payload", "")).strip(), update_type, str(cb.get("callback_id", ""))
    if update_type == "bot_started":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        return int(sender.get("user_id") or 0), "/start", update_type, ""
    return int(payload.get("user_id", 0)), str(payload.get("text", "")).strip(), update_type, ""


async def _handle_admin_callback(api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str) -> None:
    """Обработка нажатий кнопок в админке."""
    await api.answer_callback(callback_id)

    if cb_payload == "admin:main":
        await api.send_message_with_keyboard(
            user_id, "Админ-меню:", admin_main_keyboard()
        )
        return

    if cb_payload == "admin:platforms":
        platforms = repo.list_platforms()
        text = "Платформы:" if platforms else "Платформ пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_platforms_keyboard(platforms))
        return

    if cb_payload == "admin:platform_add":
        await api.send_message(user_id, "Введите название новой платформы:")
        return

    if cb_payload.startswith("admin:platform_delete:"):
        platform_id = int(cb_payload.split(":")[-1])
        try:
            repo.delete_platform(platform_id)
            platforms = repo.list_platforms()
            await api.send_message_with_keyboard(user_id, "Платформа удалена.", admin_platforms_keyboard(platforms))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка удаления: {e}")
        return

    if cb_payload == "admin:offers":
        offers = repo.list_offers()
        text = "Офферы:" if offers else "Офферов пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_offers_keyboard(offers))
        return

    if cb_payload == "admin:offer_add":
        await api.send_message(user_id, "Выберите платформу для нового оффера. Введите название оффера:")
        return

    if cb_payload.startswith("admin:offer_delete:"):
        offer_id = int(cb_payload.split(":")[-1])
        try:
            repo.delete_offer(offer_id)
            offers = repo.list_offers()
            await api.send_message_with_keyboard(user_id, "Оффер удалён.", admin_offers_keyboard(offers))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка удаления: {e}")
        return

    if cb_payload == "admin:scenarios":
        scenarios = repo.list_scenarios()
        text = "Сценарии:" if scenarios else "Сценариев пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_scenarios_keyboard(scenarios))
        return

    if cb_payload == "admin:bot_links":
        await api.send_message_with_keyboard(user_id, "Ссылки на бот:", admin_bot_links_keyboard())
        return

    if cb_payload == "admin:bot_link_list":
        links = repo.list_bot_links()
        if not links:
            await api.send_message_with_keyboard(user_id, "Ссылок пока нет.", admin_bot_links_keyboard())
        else:
            text = "\n".join(f"• {lnk.deep_link}" for lnk in links)
            await api.send_message_with_keyboard(user_id, f"Ссылки:\n{text}", admin_bot_links_keyboard())
        return

    if cb_payload == "admin:channels":
        channels = repo.list_required_channels()
        text = "Каналы подписки:" if channels else "Каналов пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_channels_keyboard(channels))
        return

    if cb_payload.startswith("admin:channel_delete:"):
        channel_id = int(cb_payload.split(":")[-1])
        try:
            repo.delete_required_channel(channel_id)
            channels = repo.list_required_channels()
            await api.send_message_with_keyboard(user_id, "Канал удалён.", admin_channels_keyboard(channels))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка удаления: {e}")
        return

    if cb_payload == "admin:export":
        platforms = repo.list_platforms()
        text = "Выберите платформу для экспорта:" if platforms else "Платформ нет — нечего экспортировать."
        await api.send_message_with_keyboard(user_id, text, admin_export_platforms_keyboard(platforms))
        return

    if cb_payload.startswith("admin:export_platform:"):
        platform_id = int(cb_payload.split(":")[-1])
        await api.send_message(user_id, f"Экспорт для платформы {platform_id} — используйте API /admin/command export.")
        return

    if cb_payload == "admin:broadcast":
        await api.send_message(user_id, "Рассылка: используйте API /admin/command для отправки уведомлений.")
        return

    logger.warning("Неизвестный callback от admin: %r", cb_payload)


@router.post("/wh_links_8081")
async def handle_max_webhook(
    request: Request,
    x_max_bot_api_secret: str | None = Header(default=None),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    if settings.webhook_secret and x_max_bot_api_secret != settings.webhook_secret:
        logger.warning("Webhook secret mismatch")
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("Bad webhook JSON: %s", exc)
        return Response(status_code=400)
    if not isinstance(payload, dict):
        return Response(status_code=400)

    user_id, text, update_type, callback_id = _extract_event(payload)
    logger.info("Webhook update_type=%r user_id=%r text=%r", update_type, user_id, text)

    api = MaxApiClient(settings.bot_token)
    try:
        if not user_id:
            return Response(status_code=200)

        repo = Repo(db)
        is_admin = user_id in settings.admin_user_ids

        # --- Callback от inline-кнопок ---
        if update_type == "message_callback":
            if is_admin and text.startswith("admin:"):
                await _handle_admin_callback(api, repo, user_id, text, callback_id)
            else:
                if callback_id:
                    await api.answer_callback(callback_id)
            return Response(status_code=200)

        # --- Текстовые команды ---
        if text in ("admin", "/admin") and is_admin:
            await api.send_message_with_keyboard(
                user_id, "Добро пожаловать в админ-меню:", admin_main_keyboard()
            )
            return Response(status_code=200)

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            scenario_code = parts[1] if len(parts) > 1 else ""
            scenario = next((s for s in repo.list_scenarios() if s.code == scenario_code), None)
            if scenario:
                await api.send_message(user_id, scenario.description or scenario.title)
            else:
                await api.send_message(user_id, "Сценарий не найден. Используйте корректную ссылку.")
            return Response(status_code=200)

        await api.send_message(user_id, "Используйте ссылку для начала работы с ботом.")
        return Response(status_code=200)

    except Exception as exc:
        logger.error("Webhook handler error: %s", exc, exc_info=True)
        return Response(status_code=200)
    finally:
        await api.close()
