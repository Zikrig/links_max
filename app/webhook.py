import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app import fsm
from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.repo import Repo
from app.keyboards.admin import (
    admin_bot_links_keyboard,
    admin_channels_keyboard,
    admin_export_platforms_keyboard,
    admin_main_keyboard,
    admin_offer_select_platform_keyboard,
    admin_offers_keyboard,
    admin_platforms_keyboard,
    admin_scenarios_keyboard,
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
        return (
            int(user.get("user_id") or 0),
            str(cb.get("payload", "")).strip(),
            update_type,
            str(cb.get("callback_id", "")),
        )
    if update_type == "bot_started":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        return int(sender.get("user_id") or 0), "/start", update_type, ""
    return int(payload.get("user_id", 0)), str(payload.get("text", "")).strip(), update_type, ""


# ---------------------------------------------------------------------------
# FSM-обработчики текстовых сообщений от админа
# ---------------------------------------------------------------------------

async def _handle_admin_fsm_text(api: MaxApiClient, repo: Repo, user_id: int, text: str) -> bool:
    """Возвращает True если сообщение было обработано FSM."""
    st = fsm.get_state(user_id)
    if not st:
        return False

    state = st.state

    # --- Платформа ---
    if state == "platform_add":
        if not text:
            await api.send_message(user_id, "Название не может быть пустым. Введите название платформы:")
            return True
        repo.create_platform(text)
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        await api.send_message_with_keyboard(
            user_id, f"✅ Платформа «{text}» добавлена.", admin_platforms_keyboard(platforms)
        )
        return True

    # --- Оффер: шаг 1 — название ---
    if state == "offer_add_name":
        fsm.update_data(user_id, name=text)
        fsm.set_state(user_id, "offer_add_link_prefix", st.data | {"name": text})
        await api.send_message(user_id, "Введите первую часть реф. ссылки до SUBID\n(например: https://trckcp.com/dl/OrvoJLhNcSbf/97/?):")
        return True

    # --- Оффер: шаг 2 — link_prefix ---
    if state == "offer_add_link_prefix":
        fsm.update_data(user_id, link_prefix=text)
        fsm.set_state(user_id, "offer_add_subid_static", st.data | {"link_prefix": text})
        await api.send_message(user_id, "Введите неизменяемую часть SUBID\n(например: sub_id1=):")
        return True

    # --- Оффер: шаг 3 — subid_static_part ---
    if state == "offer_add_subid_static":
        fsm.update_data(user_id, subid_static_part=text)
        fsm.set_state(user_id, "offer_add_link_suffix", st.data | {"subid_static_part": text})
        await api.send_message(user_id, "Введите финальную часть ссылки после SUBID\n(например: &erid=2SDnjcLekU9):")
        return True

    # --- Оффер: шаг 4 — link_suffix → создать оффер ---
    if state == "offer_add_link_suffix":
        data = st.data | {"link_suffix": text}
        fsm.clear_state(user_id)
        try:
            repo.create_offer(
                platform_id=data["platform_id"],
                name=data["name"],
                link_prefix=data["link_prefix"],
                subid_static_part=data["subid_static_part"],
                link_suffix=text,
            )
            offers = repo.list_offers()
            await api.send_message_with_keyboard(
                user_id, f"✅ Оффер «{data['name']}» добавлен.", admin_offers_keyboard(offers)
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка создания оффера: {e}")
        return True

    # --- Канал: шаг 1 — название ---
    if state == "channel_add_title":
        fsm.set_state(user_id, "channel_add_id", {"title": text})
        await api.send_message(user_id, "Введите chat_id канала\n(отрицательное число, например: -1001234567890):")
        return True

    # --- Канал: шаг 2 — chat_id ---
    if state == "channel_add_id":
        try:
            chat_id = int(text)
        except ValueError:
            await api.send_message(user_id, "chat_id должен быть числом. Попробуйте ещё раз:")
            return True
        fsm.set_state(user_id, "channel_add_link", st.data | {"chat_id": chat_id})
        await api.send_message(user_id, "Введите ссылку-приглашение в канал\n(или напишите «-» чтобы пропустить):")
        return True

    # --- Канал: шаг 3 — invite_link → создать ---
    if state == "channel_add_link":
        invite_link = None if text == "-" else text
        data = st.data
        fsm.clear_state(user_id)
        try:
            repo.add_required_channel(
                title=data["title"],
                chat_id=data["chat_id"],
                invite_link=invite_link,
            )
            channels = repo.list_required_channels()
            await api.send_message_with_keyboard(
                user_id, f"✅ Канал «{data['title']}» добавлен.", admin_channels_keyboard(channels)
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка добавления канала: {e}")
        return True

    return False


# ---------------------------------------------------------------------------
# Callback-обработчики кнопок админки
# ---------------------------------------------------------------------------

async def _handle_admin_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str
) -> None:
    await api.answer_callback(callback_id)

    if cb_payload == "admin:main":
        fsm.clear_state(user_id)
        await api.send_message_with_keyboard(user_id, "Админ-меню:", admin_main_keyboard())
        return

    # --- Платформы ---
    if cb_payload == "admin:platforms":
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        text = "Платформы:" if platforms else "Платформ пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_platforms_keyboard(platforms))
        return

    if cb_payload == "admin:platform_add":
        fsm.set_state(user_id, "platform_add")
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

    # --- Офферы ---
    if cb_payload == "admin:offers":
        fsm.clear_state(user_id)
        offers = repo.list_offers()
        text = "Офферы:" if offers else "Офферов пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_offers_keyboard(offers))
        return

    if cb_payload == "admin:offer_add":
        platforms = repo.list_platforms()
        if not platforms:
            await api.send_message(user_id, "Сначала добавьте хотя бы одну платформу.")
            return
        await api.send_message_with_keyboard(
            user_id, "Выберите платформу для нового оффера:", admin_offer_select_platform_keyboard(platforms)
        )
        return

    if cb_payload.startswith("admin:offer_select_platform:"):
        platform_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "offer_add_name", {"platform_id": platform_id})
        await api.send_message(user_id, "Введите название оффера (карты):")
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

    # --- Сценарии ---
    if cb_payload == "admin:scenarios":
        fsm.clear_state(user_id)
        scenarios = repo.list_scenarios()
        text = "Сценарии:" if scenarios else "Сценариев пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_scenarios_keyboard(scenarios))
        return

    # --- Ссылки на бот ---
    if cb_payload == "admin:bot_links":
        fsm.clear_state(user_id)
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

    # --- Каналы ---
    if cb_payload == "admin:channels":
        fsm.clear_state(user_id)
        channels = repo.list_required_channels()
        text = "Каналы подписки:" if channels else "Каналов пока нет."
        await api.send_message_with_keyboard(user_id, text, admin_channels_keyboard(channels))
        return

    if cb_payload == "admin:channel_add":
        fsm.set_state(user_id, "channel_add_title")
        await api.send_message(user_id, "Введите название канала:")
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

    # --- Экспорт ---
    if cb_payload == "admin:export":
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        text = "Выберите платформу для экспорта:" if platforms else "Платформ нет — нечего экспортировать."
        await api.send_message_with_keyboard(user_id, text, admin_export_platforms_keyboard(platforms))
        return

    if cb_payload.startswith("admin:export_platform:"):
        platform_id = int(cb_payload.split(":")[-1])
        await api.send_message(user_id, f"Экспорт для платформы {platform_id} — функция в разработке.")
        return

    # --- Рассылка ---
    if cb_payload == "admin:broadcast":
        fsm.clear_state(user_id)
        await api.send_message(user_id, "Рассылка — функция в разработке.")
        return

    logger.warning("Неизвестный admin callback: %r", cb_payload)


# ---------------------------------------------------------------------------
# Главный обработчик webhook
# ---------------------------------------------------------------------------

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

        # --- Текстовые сообщения ---
        if update_type not in ("message_created", "bot_started", ""):
            return Response(status_code=200)

        # Проверяем FSM-состояние у админа (ввод данных через диалог)
        if is_admin and update_type == "message_created":
            handled = await _handle_admin_fsm_text(api, repo, user_id, text)
            if handled:
                return Response(status_code=200)

        # Команды
        if text in ("admin", "/admin") and is_admin:
            fsm.clear_state(user_id)
            await api.send_message_with_keyboard(
                user_id, "Добро пожаловать в админ-меню:", admin_main_keyboard()
            )
            return Response(status_code=200)

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            scenario_code = parts[1] if len(parts) > 1 else ""
            scenario = repo.get_scenario_by_code(scenario_code) if scenario_code else None
            if scenario:
                await api.send_message(user_id, scenario.description or scenario.title)
            else:
                await api.send_message(user_id, "Сценарий не найден. Используйте корректную ссылку.")
            return Response(status_code=200)

        # Молчим на всё остальное
        return Response(status_code=200)

    except Exception as exc:
        logger.error("Webhook handler error: %s", exc, exc_info=True)
        return Response(status_code=200)
    finally:
        await api.close()
