import logging
import secrets as _secrets
from dataclasses import dataclass, field
from types import SimpleNamespace

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app import fsm
from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.models import Offer, Scenario
from app.db.repo import Repo
from app.keyboards.admin import (
    admin_bot_links_keyboard,
    admin_channels_keyboard,
    admin_confirm_delete_keyboard,
    admin_export_offers_keyboard,
    admin_export_platforms_keyboard,
    admin_main_keyboard,
    admin_offer_select_platform_keyboard,
    admin_offer_view_keyboard,
    admin_offers_keyboard,
    admin_platform_view_keyboard,
    admin_platforms_keyboard,
    admin_scenario_channels_keyboard,
    admin_scenario_select_offer_keyboard,
    admin_scenario_settings_keyboard,
    admin_scenario_view_keyboard,
    admin_scenarios_keyboard,
)
from app.keyboards.user import (
    user_card_keyboard,
    user_channels_keyboard,
    user_material_keyboard,
    user_subscribe_keyboard,
)
from app.max_api import MaxApiClient, RateLimitError
from app.services.export_service import ExportService
from app.services.link_builder import build_offer_link

router = APIRouter(tags=["webhook"])
logger = logging.getLogger(__name__)


def _get_cached_settings() -> Settings:
    return get_settings()


# ---------------------------------------------------------------------------
# Парсинг входящего события
# ---------------------------------------------------------------------------

@dataclass
class Event:
    user_id: int = 0
    text: str = ""
    update_type: str = ""
    callback_id: str = ""
    message_id: str = ""   # mid текущего сообщения (для edit)
    max_name: str = ""
    max_username: str = ""


def _extract_event(payload: dict) -> Event:
    update_type = payload.get("update_type", "")

    if update_type == "message_created":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        body = msg.get("body", {}) or {}
        return Event(
            user_id=int(sender.get("user_id") or 0),
            text=str(body.get("text", "")).strip(),
            update_type=update_type,
            message_id=str(body.get("mid", "") or ""),
            max_name=str(sender.get("name", "") or ""),
            max_username=str(sender.get("username", "") or ""),
        )

    if update_type == "message_callback":
        cb = payload.get("callback", {}) or {}
        user = cb.get("user", {}) or {}
        # В MAX API "message" находится на верхнем уровне payload, НЕ внутри "callback"
        top_msg = payload.get("message", {}) or {}
        top_body = top_msg.get("body", {}) or {}
        mid = str(top_body.get("mid", "") or "")
        if not mid:
            logger.warning("CALLBACK_STRUCT: mid not found. payload keys=%s cb keys=%s msg keys=%s",
                           list(payload.keys()), list(cb.keys()), list(top_msg.keys()))
        return Event(
            user_id=int(user.get("user_id") or 0),
            text=str(cb.get("payload", "")).strip(),
            update_type=update_type,
            callback_id=str(cb.get("callback_id", "")),
            message_id=mid,
            max_name=str(user.get("name", "") or ""),
            max_username=str(user.get("username", "") or ""),
        )

    if update_type == "bot_started":
        user = payload.get("user", {}) or {}
        if not user:
            msg = payload.get("message", {}) or {}
            user = msg.get("sender", {}) or {}
        return Event(
            user_id=int(user.get("user_id") or 0),
            text=str(payload.get("payload", "") or "").strip(),
            update_type=update_type,
            max_name=str(user.get("name", "") or ""),
            max_username=str(user.get("username", "") or ""),
        )

    return Event(
        user_id=int(payload.get("user_id", 0)),
        text=str(payload.get("text", "")).strip(),
        update_type=update_type,
    )


# ---------------------------------------------------------------------------
# FSM: подписчик (текстовый ввод больше не нужен в user flow, оставляем заглушку)
# ---------------------------------------------------------------------------

async def _handle_user_fsm_text(
    api: MaxApiClient, repo: Repo, user_id: int, text: str, settings: Settings
) -> bool:
    return False


async def _issue_link(
    api: MaxApiClient, repo: Repo, user_id: int, scenario_code: str,
    max_name: str = "", max_username: str = "",
) -> None:
    scenario = repo.get_scenario_by_code(scenario_code)
    if not scenario:
        await api.send_message(user_id, "Ошибка: сценарий не найден.")
        return
    try:
        subid = repo.next_subid(offer_id=scenario.offer_id)
    except ValueError as e:
        await api.send_message(user_id, f"Ошибка: {e}")
        return

    final_link = build_offer_link(offer=scenario.offer, subid_value=subid)
    repo.create_lead(
        user_id=user_id,
        scenario_id=scenario.id,
        offer_id=scenario.offer_id,
        subid_value=subid,
        max_name=max_name or None,
        max_username=max_username or None,
    )
    await api.send_message_with_keyboard(
        user_id,
        "Ваша персональная ссылка готова. Перейдите по ней для оформления:",
        user_card_keyboard(final_link),
    )


async def _handle_user_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str,
    message_id: str, max_name: str, max_username: str, settings: Settings,
) -> None:
    await api.answer_callback(callback_id)

    if cb_payload == "user:noop":
        return

    if cb_payload.startswith("user:check_sub:"):
        scenario_code = cb_payload[len("user:check_sub:"):]
        scenario = repo.get_scenario_by_code(scenario_code)
        if not scenario:
            await api.send_message(user_id, "Сценарий не найден.")
            return

        channels = repo.list_scenario_channels(scenario.id)
        not_subscribed = []
        for ch in channels:
            member = await api.get_chat_member(ch.chat_id, user_id)
            if not member:
                not_subscribed.append(ch)

        if not_subscribed:
            await api.send_message_with_keyboard(
                user_id,
                "Вы ещё не подписаны на все каналы:",
                user_subscribe_keyboard(not_subscribed, scenario_code),
            )
            return

        await _issue_link(api, repo, user_id, scenario_code, max_name, max_username)
        return


# ---------------------------------------------------------------------------
# FSM: админ — текстовый ввод
# ---------------------------------------------------------------------------

async def _handle_admin_fsm_text(api: MaxApiClient, repo: Repo, user_id: int, text: str) -> bool:
    st = fsm.get_state(user_id)
    if not st:
        return False

    state = st.state
    msg_id: str = st.data.get("_msg_id", "")

    async def _reply(reply_text: str, buttons: list | None = None) -> None:
        """Если есть сохранённый msg_id — редактируем его, иначе новое сообщение."""
        if msg_id and buttons is not None:
            await api.edit_message(msg_id, reply_text, buttons)
        else:
            await api.send_message_with_keyboard(user_id, reply_text, buttons or [])

    if state == "platform_add":
        if not text:
            await api.send_message(user_id, "Название не может быть пустым. Введите название платформы:")
            return True
        repo.create_platform(text)
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        await _reply(f"✅ Платформа «{text}» добавлена.", admin_platforms_keyboard(platforms))
        return True

    if state == "offer_add_name":
        fsm.set_state(user_id, "offer_add_base_url", st.data | {"name": text})
        await api.send_message(
            user_id,
            "Введите основную ссылку оффера целиком\n"
            "(например: https://trckcp.com/dl/OrvoJLhNcSbf/97/?erid=2SDnjcLekU9):"
        )
        return True

    if state == "offer_add_base_url":
        fsm.set_state(user_id, "offer_add_subid_param", st.data | {"base_url": text})
        await api.send_message(
            user_id,
            "Введите имя переменной для SUBID\n"
            "(например: sub_id1)\n\n"
            "Бот сам добавит & или ? перед ней в зависимости от ссылки."
        )
        return True

    if state == "offer_add_subid_param":
        data = st.data
        fsm.clear_state(user_id)
        subid_param = text.strip().lstrip("?&").strip()
        try:
            repo.create_offer(
                platform_id=data["platform_id"],
                name=data["name"],
                base_url=data["base_url"],
                subid_param=subid_param,
            )
            platform_id = data["platform_id"]
            offers = repo.list_offers_for_platform(platform_id)
            example_link = data["base_url"]
            sep = "&" if "?" in example_link else "?"
            await _reply(
                f"✅ Оффер «{data['name']}» добавлен.\n\nПример ссылки:\n{example_link}{sep}{subid_param}=0001",
                admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}", platform_id=platform_id),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка создания оффера: {e}")
        return True

    if state == "channel_add_title":
        fsm.set_state(user_id, "channel_add_id", st.data | {"title": text})
        await api.send_message(user_id, "Введите chat_id канала\n(отрицательное число, например: -1001234567890):")
        return True

    if state == "channel_add_id":
        try:
            chat_id = int(text)
        except ValueError:
            await api.send_message(user_id, "chat_id должен быть числом. Попробуйте ещё раз:")
            return True
        fsm.set_state(user_id, "channel_add_link", st.data | {"chat_id": chat_id})
        await api.send_message(user_id, "Введите ссылку-приглашение в канал\n(или напишите «-» чтобы пропустить):")
        return True

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
            await _reply(f"✅ Канал «{data['title']}» добавлен.", admin_channels_keyboard(channels))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка добавления канала: {e}")
        return True

    if state == "scenario_add_title":
        fsm.set_state(user_id, "scenario_add_description", st.data | {"title": text})
        await api.send_message(user_id, "Введите описание акции (текст, который увидит подписчик):")
        return True

    if state == "scenario_add_description":
        fsm.set_state(user_id, "scenario_add_image", st.data | {"description": text})
        await api.send_message(user_id, "Введите ссылку на картинку акции\n(или «-» чтобы пропустить):")
        return True

    if state == "scenario_add_image":
        image_url = None if text == "-" else text
        data = st.data
        fsm.clear_state(user_id)
        try:
            code = f"sc{_secrets.token_hex(4)}"
            scenario = repo.create_scenario(
                offer_id=data["offer_id"],
                code=code,
                title=data["title"],
                description=data.get("description"),
                image_url=image_url,
            )
            settings = _get_cached_settings()
            if settings.bot_username:
                deep_link = f"https://max.ru/join/{settings.bot_username}?start={scenario.code}"
            else:
                deep_link = f"https://max.ru/start?start={scenario.code}"
            repo.create_or_update_bot_link(scenario.id, deep_link)
            scenarios = repo.list_scenarios()
            await _reply(
                f"✅ Сценарий «{data['title']}» создан.\n\nКод: {scenario.code}\nСсылка: {deep_link}",
                admin_scenarios_keyboard(scenarios),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка создания сценария: {e}")
        return True

    # --- Редактирование полей существующего сценария ---

    if state == "scenario_edit_image":
        scenario_id = int(st.data.get("scenario_id", 0))
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Сценарий не найден.")
            return True
        image_url = None if text == "-" else text
        repo.update_scenario_field(scenario_id, image_url=image_url)
        fsm.clear_state(user_id)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _reply(
            "✅ Картинка обновлена." if image_url else "✅ Картинка удалена.",
            admin_scenario_settings_keyboard(scenario, channels),
        )
        return True

    if state == "scenario_edit_text":
        scenario_id = int(st.data.get("scenario_id", 0))
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Сценарий не найден.")
            return True
        description = None if text == "-" else text
        repo.update_scenario_field(scenario_id, description=description)
        fsm.clear_state(user_id)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _reply(
            "✅ Текст обновлён." if description else "✅ Текст удалён.",
            admin_scenario_settings_keyboard(scenario, channels),
        )
        return True

    if state == "scenario_channel_add":
        scenario_id = int(st.data.get("scenario_id", 0))
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Сценарий не найден.")
            return True

        try:
            chat_id = int(text)
        except ValueError:
            await api.send_message(user_id, "chat_id должен быть числом (например: -1001234567890).\nПопробуйте ещё раз:")
            return True

        settings_ch = _get_cached_settings()
        api_ch = MaxApiClient(settings_ch.bot_token)
        try:
            ok, detail = await api_ch.check_bot_is_channel_admin(chat_id)
        finally:
            await api_ch.close()

        if not ok:
            await api.send_message(
                user_id,
                f"⚠️ {detail}\n\nВведите другой chat_id или нажмите «Назад»:"
            )
            return True

        if st.data.get("_invite_step"):
            invite_link = None if text == "-" else text
            ch_data = st.data
            fsm.clear_state(user_id)
            repo.add_scenario_channel(
                scenario_id=scenario_id,
                chat_id=ch_data["_pending_chat_id"],
                title=ch_data["_pending_title"],
                invite_link=invite_link,
            )
            channels = repo.list_scenario_channels(scenario_id)
            await _reply(f"✅ Канал «{ch_data['_pending_title']}» добавлен.", admin_scenario_channels_keyboard(scenario_id, channels))
            return True

        fsm.set_state(user_id, "scenario_channel_add", st.data | {
            "_invite_step": True,
            "_pending_chat_id": chat_id,
            "_pending_title": detail,
        })
        await api.send_message(
            user_id,
            f"Канал «{detail}» найден. ✅\n\nВведите ссылку-приглашение для канала\n(или «-» пропустить):"
        )
        return True

    return False


# ---------------------------------------------------------------------------
# Callback-обработчики: admin
# ---------------------------------------------------------------------------

async def _handle_admin_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str, message_id: str
) -> None:
    """
    Паттерн smena_new:
    1. Редактируем сообщение (PUT /messages)
    2. Потом подтверждаем callback (POST /answers)
    При неудаче edit — fallback: новое сообщение.
    Callback всегда подтверждается через try/finally.
    """
    _acked = False

    async def _ack() -> None:
        nonlocal _acked
        if not _acked:
            _acked = True
            await api.answer_callback(callback_id)

    async def _edit(text: str, buttons: list | None = None) -> None:
        edited = False
        if message_id:
            edited = await api.edit_message(message_id, text, buttons)
            if not edited:
                logger.warning("edit_message failed for mid=%r, falling back to new message", message_id)
        if not edited:
            await api.send_message_with_keyboard(user_id, text, buttons or [])
        await _ack()

    async def _edit_then_ask(text_edit: str, question: str) -> None:
        """Убрать кнопки в текущем сообщении, задать вопрос новым. Сохраняет message_id в FSM."""
        if message_id:
            await api.edit_message(message_id, text_edit, buttons=None)
            fsm.update_data(user_id, _msg_id=message_id)
        await _ack()
        await api.send_message(user_id, question)

    if cb_payload == "admin:main":
        fsm.clear_state(user_id)
        await _edit("Админ-меню:", admin_main_keyboard())
        return

    # --- Платформы ---
    if cb_payload == "admin:platforms":
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        text = "Платформы:" if platforms else "Платформ пока нет."
        await _edit(text, admin_platforms_keyboard(platforms))
        return

    if cb_payload == "admin:platform_add":
        fsm.set_state(user_id, "platform_add")
        await _edit_then_ask("Добавление платформы:", "Введите название новой платформы:")
        return

    if cb_payload.startswith("admin:platform_view:"):
        platform_id = int(cb_payload.split(":")[-1])
        from app.db.models import Platform as _Platform
        platform = repo.db.get(_Platform, platform_id)
        if not platform:
            return
        offers = repo.list_offers_for_platform(platform_id)
        offers_text = f"\nОфферов: {len(offers)}" if offers else "\nОфферов пока нет."
        await _edit(f"Платформа: {platform.name}{offers_text}", admin_platform_view_keyboard(platform_id))
        return

    if cb_payload.startswith("admin:platform_offers:"):
        platform_id = int(cb_payload.split(":")[-1])
        offers = repo.list_offers_for_platform(platform_id)
        text = "Офферы платформы:" if offers else "Офферов пока нет."
        await _edit(text, admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}", platform_id=platform_id))
        return

    if cb_payload.startswith("admin:platform_delete:"):
        platform_id = int(cb_payload.split(":")[-1])
        from app.db.models import Platform as _Platform2
        platform = repo.db.get(_Platform2, platform_id)
        name = platform.name if platform else f"#{platform_id}"
        await _edit(
            f"Удалить платформу «{name}»?\nВсе офферы платформы также будут удалены.",
            admin_confirm_delete_keyboard(f"admin:platform_delete_yes:{platform_id}", f"admin:platform_view:{platform_id}"),
        )
        return

    if cb_payload.startswith("admin:platform_delete_yes:"):
        platform_id = int(cb_payload.split(":")[-1])
        try:
            repo.delete_platform(platform_id)
            platforms = repo.list_platforms()
            await _edit("✅ Платформа удалена.", admin_platforms_keyboard(platforms))
        except Exception as e:
            await _edit(f"Ошибка удаления: {e}")
        return

    # --- Офферы ---
    if cb_payload == "admin:offers":
        fsm.clear_state(user_id)
        offers = repo.list_offers()
        text = "Все офферы:" if offers else "Офферов пока нет."
        await _edit(text, admin_offers_keyboard(offers))
        return

    if cb_payload.startswith("admin:offer_add:"):
        platform_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "offer_add_name", {"platform_id": platform_id})
        await _edit_then_ask("Добавление оффера:", "Введите название оффера (карты):")
        return

    if cb_payload == "admin:offer_add":
        platforms = repo.list_platforms()
        if not platforms:
            await _edit("Сначала добавьте хотя бы одну платформу.", [])
            return
        await _edit("Выберите платформу для нового оффера:", admin_offer_select_platform_keyboard(platforms))
        return

    if cb_payload.startswith("admin:offer_select_platform:"):
        platform_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "offer_add_name", {"platform_id": platform_id})
        await _edit_then_ask("Добавление оффера:", "Введите название оффера (карты):")
        return

    def _offer_kbd(offer, scenario) -> list:
        bl = repo.get_bot_link_for_scenario(scenario.id) if scenario else None
        return admin_offer_view_keyboard(offer, scenario, has_bot_link=bool(bl))

    if cb_payload.startswith("admin:offer_view:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        scenario = repo.get_scenario_for_offer(offer_id)
        sep = "&" if "?" in (offer.base_url or "") else "?"
        base = offer.base_url or "—"
        example = f"{base}{sep}{offer.subid_param}=0001" if offer.subid_param else base
        await _edit(
            f"Оффер: {offer.name}\nПример ссылки:\n{example}",
            _offer_kbd(offer, scenario),
        )
        return

    if cb_payload.startswith("admin:offer_link:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        sep = "&" if "?" in (offer.base_url or "") else "?"
        base = offer.base_url or "—"
        if offer.subid_param:
            example = f"{base}{sep}{offer.subid_param}=0001"
            text_out = f"Ссылка оффера «{offer.name}»:\n\n🔗 {example}\n\nГде «0001» — уникальный SUBID подписчика."
        else:
            text_out = f"Ссылка оффера «{offer.name}»:\n\n{base}\n\n⚠️ Параметр SUBID не задан."
        scenario = repo.get_scenario_for_offer(offer_id)
        await _edit(text_out, _offer_kbd(offer, scenario))
        return

    if cb_payload.startswith("admin:offer_botlink:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        scenario = repo.get_scenario_for_offer(offer_id)
        if not scenario:
            await _edit(
                f"Для оффера «{offer.name}» нет сценария.\nСначала настройте Сценарий.",
                _offer_kbd(offer, scenario),
            )
            return
        bot_link_obj = repo.get_bot_link_for_scenario(scenario.id)
        deep_link = bot_link_obj.deep_link if bot_link_obj else None
        if deep_link:
            await _edit(
                f"Ссылка на бот для «{offer.name}»:\n\n🔗 {deep_link}\n\nОтправьте эту ссылку подписчикам.",
                _offer_kbd(offer, scenario),
            )
        else:
            await _edit(
                f"Ссылка на бот для «{offer.name}» не создана.\n\nНастройте сценарий — ссылка сгенерируется автоматически.",
                _offer_kbd(offer, scenario),
            )
        return

    if cb_payload.startswith("admin:offer_scenario:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        scenario = repo.get_scenario_for_offer(offer_id)
        if scenario:
            channels = repo.list_scenario_channels(scenario.id)
            await _edit(
                f"Сценарий оффера «{offer.name}»:",
                admin_scenario_settings_keyboard(scenario, channels),
            )
        else:
            fsm.set_state(user_id, "scenario_add_title", {"offer_id": offer_id, "_msg_id": message_id})
            await _edit_then_ask(
                f"Настройка сценария для «{offer.name}»:",
                "Введите заголовок сценария (название акции):"
            )
        return

    if cb_payload.startswith("admin:offer_scenario_view:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        channels = repo.list_scenario_channels(scenario_id)
        await _edit(
            f"Сценарий оффера:",
            admin_scenario_settings_keyboard(scenario, channels),
        )
        return

    if cb_payload.startswith("admin:scenario_set_image:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        fsm.set_state(user_id, "scenario_edit_image", {"scenario_id": scenario_id})
        cur = f"\nТекущая: {scenario.image_url}" if scenario.image_url else "\nСейчас не задана."
        await _edit_then_ask(
            "Картинка сценария:",
            f"Введите URL картинки (JPG/PNG){cur}\nИли «-» чтобы удалить:"
        )
        return

    if cb_payload.startswith("admin:scenario_set_text:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        fsm.set_state(user_id, "scenario_edit_text", {"scenario_id": scenario_id})
        cur = f"\nТекущий:\n{scenario.description}" if scenario.description else "\nСейчас не задан."
        await _edit_then_ask(
            "Текст для подписчика:",
            f"Введите текст акции, который увидит подписчик.{cur}\nИли «-» чтобы удалить:"
        )
        return

    if cb_payload.startswith("admin:scenario_toggle_sub:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        new_val = not scenario.check_subscription
        repo.update_scenario_field(scenario_id, check_subscription=new_val)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        status = "включена ✅" if new_val else "выключена"
        await _edit(
            f"Проверка подписки {status}.",
            admin_scenario_settings_keyboard(scenario, channels),
        )
        return

    if cb_payload.startswith("admin:scenario_channels:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        channels = repo.list_scenario_channels(scenario_id)
        text_ch = f"Каналы сценария ({len(channels)}):" if channels else "Каналов пока нет."
        await _edit(text_ch, admin_scenario_channels_keyboard(scenario_id, channels))
        return

    if cb_payload.startswith("admin:scenario_ch_add:"):
        scenario_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "scenario_channel_add", {"scenario_id": scenario_id})
        await _edit_then_ask(
            "Добавление канала:",
            "Введите chat_id канала (число, например: -1001234567890).\n"
            "Бот должен быть администратором в этом канале."
        )
        return

    if cb_payload.startswith("admin:scenario_ch_del:"):
        channel_id = int(cb_payload.split(":")[-1])
        from app.db.models import ScenarioChannel as _SC
        ch = repo.db.get(_SC, channel_id)
        if not ch:
            return
        await _edit(
            f"Удалить канал «{ch.title}» из сценария?",
            admin_confirm_delete_keyboard(
                f"admin:scenario_ch_del_yes:{channel_id}",
                f"admin:scenario_channels:{ch.scenario_id}",
            ),
        )
        return

    if cb_payload.startswith("admin:scenario_ch_del_yes:"):
        channel_id = int(cb_payload.split(":")[-1])
        from app.db.models import ScenarioChannel as _SC2
        ch = repo.db.get(_SC2, channel_id)
        scenario_id = ch.scenario_id if ch else 0
        try:
            repo.delete_scenario_channel(channel_id)
            channels = repo.list_scenario_channels(scenario_id)
            await _edit("✅ Канал удалён.", admin_scenario_channels_keyboard(scenario_id, channels))
        except Exception as e:
            await _edit(f"Ошибка удаления: {e}")
        return

    if cb_payload.startswith("admin:offer_delete:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        name = offer.name
        await _edit(
            f"Удалить оффер «{name}»?\nСценарий и все лиды также будут удалены.",
            admin_confirm_delete_keyboard(f"admin:offer_delete_yes:{offer_id}", f"admin:offer_view:{offer_id}"),
        )
        return

    if cb_payload.startswith("admin:offer_delete_yes:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        platform_id = offer.platform_id if offer else None
        try:
            repo.delete_offer(offer_id)
            if platform_id:
                offers = repo.list_offers_for_platform(platform_id)
                await _edit("✅ Оффер удалён.", admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}", platform_id=platform_id))
            else:
                await _edit("✅ Оффер удалён.")
        except Exception as e:
            await _edit(f"Ошибка удаления: {e}")
        return

    # --- Сценарии ---
    if cb_payload == "admin:scenarios":
        fsm.clear_state(user_id)
        scenarios = repo.list_scenarios()
        text = "Сценарии:" if scenarios else "Сценариев пока нет."
        await _edit(text, admin_scenarios_keyboard(scenarios))
        return

    if cb_payload == "admin:scenario_add":
        offers = repo.list_offers()
        if not offers:
            await _edit("Сначала добавьте хотя бы один оффер.")
            return
        await _edit("Выберите оффер для нового сценария:", admin_scenario_select_offer_keyboard(offers))
        return

    if cb_payload.startswith("admin:scenario_select_offer:"):
        offer_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "scenario_add_title", {"offer_id": offer_id})
        await _edit_then_ask("Добавление сценария:", "Введите название сценария (заголовок акции):")
        return

    if cb_payload.startswith("admin:scenario_view:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        bot_link = getattr(scenario, "bot_link", None)
        link_text = f"\nСсылка: {bot_link.deep_link}" if bot_link else ""
        await _edit(
            f"Сценарий: {scenario.title}\nКод: {scenario.code}\nОписание: {scenario.description}{link_text}",
            admin_scenario_view_keyboard(scenario_id),
        )
        return

    if cb_payload.startswith("admin:scenario_delete:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        name = scenario.title if scenario else f"#{scenario_id}"
        await _edit(
            f"Удалить сценарий «{name}»?",
            admin_confirm_delete_keyboard(f"admin:scenario_delete_yes:{scenario_id}", f"admin:scenario_view:{scenario_id}"),
        )
        return

    if cb_payload.startswith("admin:scenario_delete_yes:"):
        scenario_id = int(cb_payload.split(":")[-1])
        try:
            scenario = repo.db.get(Scenario, scenario_id)
            if scenario:
                repo.db.delete(scenario)
                repo.db.commit()
            scenarios = repo.list_scenarios()
            await _edit("✅ Сценарий удалён.", admin_scenarios_keyboard(scenarios))
        except Exception as e:
            await _edit(f"Ошибка удаления: {e}")
        return

    # --- Ссылки на бот ---
    if cb_payload == "admin:bot_links":
        fsm.clear_state(user_id)
        await _edit("Ссылки на бот:", admin_bot_links_keyboard())
        return

    if cb_payload == "admin:bot_link_list":
        links = repo.list_bot_links()
        if not links:
            await _edit("Ссылок пока нет.", admin_bot_links_keyboard())
        else:
            text = "\n".join(f"• {lnk.deep_link}" for lnk in links)
            await _edit(f"Ссылки:\n{text}", admin_bot_links_keyboard())
        return

    # --- Каналы ---
    if cb_payload == "admin:channels":
        fsm.clear_state(user_id)
        channels = repo.list_required_channels()
        text = "Каналы подписки:" if channels else "Каналов пока нет."
        await _edit(text, admin_channels_keyboard(channels))
        return

    if cb_payload == "admin:channel_add":
        fsm.set_state(user_id, "channel_add_title")
        await _edit_then_ask("Добавление канала:", "Введите название канала:")
        return

    if cb_payload.startswith("admin:channel_delete:"):
        channel_id = int(cb_payload.split(":")[-1])
        from app.db.models import RequiredChannel as _RC
        ch = repo.db.get(_RC, channel_id)
        name = ch.title if ch else f"#{channel_id}"
        await _edit(
            f"Удалить канал «{name}»?",
            admin_confirm_delete_keyboard(f"admin:channel_delete_yes:{channel_id}", "admin:channels"),
        )
        return

    if cb_payload.startswith("admin:channel_delete_yes:"):
        channel_id = int(cb_payload.split(":")[-1])
        try:
            repo.delete_required_channel(channel_id)
            channels = repo.list_required_channels()
            await _edit("✅ Канал удалён.", admin_channels_keyboard(channels))
        except Exception as e:
            await _edit(f"Ошибка удаления: {e}")
        return

    # --- Экспорт ---
    if cb_payload == "admin:export":
        fsm.clear_state(user_id)
        platforms = repo.list_platforms()
        text = "Выберите платформу для экспорта:" if platforms else "Платформ нет — нечего экспортировать."
        await _edit(text, admin_export_platforms_keyboard(platforms))
        return

    if cb_payload.startswith("admin:export_platform:"):
        platform_id = int(cb_payload.split(":")[-1])
        offers = repo.list_offers_for_platform(platform_id)
        if not offers:
            await _edit("У этой платформы нет офферов с данными.")
            return
        await _edit("Выберите оффер для экспорта:", admin_export_offers_keyboard(offers, platform_id))
        return

    if cb_payload.startswith("admin:export_offer:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            return
        await _edit("⏳ Генерирую файл...")
        try:
            svc = ExportService(repo.db)
            path = svc.export_leads_xlsx(platform_id=offer.platform_id, offer_id=offer_id)
            file_bytes = path.read_bytes()
            token = await api.upload_file(file_bytes, path.name)
            if token:
                await api.send_file(user_id, token, f"Экспорт: {offer.name}")
            else:
                await api.send_message(user_id, f"Файл создан, но загрузка в MAX не удалась.\nПуть: {path}")
        except Exception as e:
            await api.send_message(user_id, f"Ошибка экспорта: {e}")
        return

    # --- Рассылка ---
    if cb_payload == "admin:broadcast":
        fsm.clear_state(user_id)
        await _edit("Рассылка — функция в разработке.")
        return

    logger.warning("Неизвестный admin callback: %r", cb_payload)
    await _ack()


async def _dispatch_admin_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str, message_id: str
) -> None:
    """Обёртка с гарантированным ack даже при необработанных исключениях."""
    try:
        await _handle_admin_callback(api, repo, user_id, cb_payload, callback_id, message_id)
    except Exception as exc:
        logger.error("Admin callback error payload=%r: %s", cb_payload, exc, exc_info=True)
        try:
            await api.answer_callback(callback_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Роуты
# ---------------------------------------------------------------------------

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

    ev = _extract_event(payload)
    logger.info("Webhook update_type=%r user_id=%r text=%r", ev.update_type, ev.user_id, ev.text)

    api = MaxApiClient(settings.bot_token)
    try:
        if not ev.user_id:
            return Response(status_code=200)

        repo = Repo(db)
        is_admin = ev.user_id in settings.admin_user_ids

        # --- Callbacks ---
        if ev.update_type == "message_callback":
            if ev.text.startswith("user:"):
                await _handle_user_callback(api, repo, ev.user_id, ev.text, ev.callback_id, ev.message_id, ev.max_name, ev.max_username, settings)
            elif is_admin and ev.text.startswith("admin:"):
                await _dispatch_admin_callback(api, repo, ev.user_id, ev.text, ev.callback_id, ev.message_id)
            else:
                if ev.callback_id:
                    await api.answer_callback(ev.callback_id)
            return Response(status_code=200)

        # --- Текстовые сообщения ---
        if ev.update_type not in ("message_created", "bot_started", ""):
            return Response(status_code=200)

        # FSM: подписчик
        if ev.update_type == "message_created":
            handled = await _handle_user_fsm_text(api, repo, ev.user_id, ev.text, settings)
            if handled:
                return Response(status_code=200)

        # FSM: admin-ввод
        if is_admin and ev.update_type == "message_created":
            handled = await _handle_admin_fsm_text(api, repo, ev.user_id, ev.text)
            if handled:
                return Response(status_code=200)

        # Команды
        if ev.text in ("admin", "/admin") and is_admin:
            fsm.clear_state(ev.user_id)
            await api.send_message_with_keyboard(
                ev.user_id, "Добро пожаловать в админ-меню:", admin_main_keyboard()
            )
            return Response(status_code=200)

        # bot_started: text = значение ?start= из deep link
        # message_created: поддерживаем /start <code> как fallback для тестов
        scenario_code = ""
        scenario_code = ""
        if ev.update_type == "bot_started":
            scenario_code = ev.text
        elif ev.text.startswith("/start"):
            parts = ev.text.split(maxsplit=1)
            scenario_code = parts[1] if len(parts) > 1 else ""

        if scenario_code:
            scenario = repo.get_scenario_by_code(scenario_code)
            if not scenario:
                await api.send_message(ev.user_id, "Сценарий не найден. Используйте корректную ссылку.")
                return Response(status_code=200)

            # Формируем текст материала
            parts = []
            if scenario.image_url:
                parts.append(scenario.image_url)
            if scenario.description:
                parts.append(scenario.description)
            msg = "\n\n".join(parts) if parts else scenario.title

            if scenario.check_subscription:
                # Показываем материал + список каналов + «Я подписался»
                channels = repo.list_scenario_channels(scenario.id)
                if channels:
                    await api.send_message_with_keyboard(
                        ev.user_id, msg,
                        user_subscribe_keyboard(channels, scenario_code),
                    )
                else:
                    # check_subscription включён, но каналов нет — выдаём ссылку сразу
                    await _issue_link(api, repo, ev.user_id, scenario_code, ev.max_name, ev.max_username)
            else:
                # Без проверки подписки — генерируем ссылку сразу, показываем в том же сообщении
                try:
                    subid = repo.next_subid(offer_id=scenario.offer_id)
                    final_link = build_offer_link(offer=scenario.offer, subid_value=subid)
                    repo.create_lead(
                        user_id=ev.user_id,
                        scenario_id=scenario.id,
                        offer_id=scenario.offer_id,
                        subid_value=subid,
                        max_name=ev.max_name or None,
                        max_username=ev.max_username or None,
                    )
                    await api.send_message_with_keyboard(
                        ev.user_id, msg,
                        user_material_keyboard(scenario_code, final_link),
                    )
                except ValueError as e:
                    await api.send_message(ev.user_id, f"Ошибка: {e}")

            return Response(status_code=200)

        return Response(status_code=200)

    except RateLimitError as exc:
        logger.error("Rate limit exhausted: %s", exc)
        try:
            uid = ev.user_id
            await api.client.post(
                "/messages",
                params={"user_id": uid} if uid > 0 else {"chat_id": uid},
                json={"text": "⚠️ MAX API временно недоступен (rate limit). Попробуйте через несколько минут."},
            )
        except Exception:
            pass
        return Response(status_code=200)
    except Exception as exc:
        logger.error("Webhook handler error: %s", exc, exc_info=True)
        return Response(status_code=200)
    finally:
        await api.close()
