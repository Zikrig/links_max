import logging
import secrets as _secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone as tz_utc
from zoneinfo import ZoneInfo
from types import SimpleNamespace

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app import fsm
from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.models import Offer, Scenario
from app.db.repo import Repo
from app.keyboards.admin import (
    BROADCAST_MANAGE_PAGE_SIZE,
    admin_bot_links_keyboard,
    admin_broadcast_default_button_keyboard,
    admin_broadcast_detail_keyboard,
    admin_broadcast_entry_keyboard,
    admin_broadcast_manage_cancel_keyboard,
    admin_broadcast_manage_keyboard,
    admin_broadcast_preview_keyboard,
    admin_broadcast_schedule_cancel_keyboard,
    admin_broadcast_skip_image_keyboard,
    admin_broadcast_skip_text_keyboard,
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
    admin_replica_cancel_keyboard,
    admin_replicas_menu_keyboard,
    admin_scenario_channels_keyboard,
    admin_scenario_select_offer_keyboard,
    admin_scenario_settings_keyboard,
    admin_scenarios_keyboard,
)
from app.keyboards.user import (
    user_card_keyboard,
    user_consent_keyboard,
    user_material_keyboard,
    user_subscribe_keyboard,
)
from app.max_api import MaxApiClient, RateLimitError
from app.services.broadcast_runner import get_scheduler, launch_broadcast_now, schedule_broadcast_job
from app.services.export_service import ExportService
from app.services.replica_messages import (
    DEFAULT_REPLICA_AFTER_LINK,
    DEFAULT_REPLICA_STRANGER,
    send_replica_with_offers,
)
from app.services.replica_runner import schedule_after_link_replica
from app.services.link_builder import build_offer_link, offer_produces_valid_links
from app.services.user_flow import UserFlowService
from app.validators import validate_full_name, validate_phone

router = APIRouter(tags=["webhook"])
logger = logging.getLogger(__name__)

# Дедупликация callback_id: защита от повторных webhook от MAX при задержках
_seen_callbacks: dict[str, float] = {}
_CALLBACK_TTL = 60.0  # секунд

def _is_duplicate_callback(callback_id: str) -> bool:
    """Вернуть True если этот callback_id уже обрабатывался недавно."""
    now = time.monotonic()
    # Чистим старые записи
    expired = [k for k, v in _seen_callbacks.items() if now - v > _CALLBACK_TTL]
    for k in expired:
        del _seen_callbacks[k]
    if callback_id in _seen_callbacks:
        return True
    _seen_callbacks[callback_id] = now
    return False


def _get_cached_settings() -> Settings:
    return get_settings()


def _extract_broadcast_image_ref(attachments: list | None) -> str | None:
    """URL или token картинки из вложений MAX (разные формы payload)."""
    for att in attachments or []:
        att_type = (att.get("type") or "").lower()
        pld = att.get("payload")
        if not isinstance(pld, dict):
            continue
        url_candidate = pld.get("url") or pld.get("photo_url") or pld.get("link")
        if url_candidate:
            return str(url_candidate).strip()
        nested = pld.get("photo")
        if isinstance(nested, dict):
            u = nested.get("url") or nested.get("token")
            if u:
                return str(u).strip()
        if att_type in ("image", "photo", "picture", "sticker") and pld.get("token"):
            return str(pld["token"]).strip()
    return None


def _normalize_broadcast_https_url(raw: str) -> str:
    """Для рассылки: если схемы нет — подставить https://."""
    t = raw.strip()
    if not t:
        return t
    tl = t.lower()
    if tl.startswith("http://") or tl.startswith("https://"):
        return t
    return f"https://{t}"


# Текст кнопки по умолчанию в рассылке (ТЗ и модель Broadcast)
_BROADCAST_DEFAULT_BUTTON_TEXT = "Перейти к акции"


def _format_broadcast_preview(data: dict) -> str:
    lines = ["📣 Превью рассылки", "", f"Заголовок: {data.get('title', '')}"]
    img = data.get("image_url")
    if not img:
        lines.append("Картинка: —")
    else:
        lines.append("Картинка: да")
    body = (data.get("text") or "").strip()
    if body:
        lines.extend(["", body, ""])
    else:
        lines.extend(["", f"Текст у получателя: заголовок «{data.get('title', '')}»", ""])
    btn = data.get("button_text") or _BROADCAST_DEFAULT_BUTTON_TEXT
    lines.append(f"Кнопка: «{btn}» → {data.get('button_url', '')}")
    return "\n".join(lines)


def _remove_broadcast_scheduler_job(broadcast_id: int) -> None:
    try:
        get_scheduler().remove_job(f"broadcast_{broadcast_id}")
    except Exception:
        pass


def _format_broadcast_detail(b) -> str:
    """Карточка рассылки для экрана управления."""
    status_map = {
        "scheduled": "ожидает отправки",
        "sending": "отправляется",
        "sent": "отправлена",
        "failed": "ошибка",
        "cancelled": "отменена",
    }
    msk = ZoneInfo("Europe/Moscow")
    head = [f"📬 Рассылка #{b.id}", f"Статус: {status_map.get(b.status, b.status)}"]
    if b.status == "scheduled" and b.send_at:
        dt = b.send_at.replace(tzinfo=tz_utc.utc)
        head.append(f"Отправка: {dt.astimezone(msk).strftime('%d.%m.%Y %H:%M')} МСК")
    elif b.status == "sent" and b.sent_at:
        dt = b.sent_at.replace(tzinfo=tz_utc.utc)
        head.append(f"Отправлена: {dt.astimezone(msk).strftime('%d.%m.%Y %H:%M')} МСК")
    head.append("")
    data = {
        "title": b.title,
        "text": b.text or "",
        "button_url": b.button_url,
        "button_text": b.button_text,
        "image_url": b.image_url,
    }
    return "\n".join(head) + "\n" + _format_broadcast_preview(data)


def _parse_broadcast_schedule(text: str) -> datetime | None:
    t = text.strip()
    if not t:
        return None
    msk = ZoneInfo("Europe/Moscow")
    utc = ZoneInfo("UTC")
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
    except ValueError:
        dt = None
    else:
        if dt.tzinfo is not None:
            return dt.astimezone(utc).replace(tzinfo=None)
        return dt.replace(tzinfo=msk).astimezone(utc).replace(tzinfo=None)
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        try:
            parsed = datetime.strptime(t, fmt).replace(tzinfo=msk)
            return parsed.astimezone(utc).replace(tzinfo=None)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Парсинг входящего события
# ---------------------------------------------------------------------------

@dataclass
class Event:
    user_id: int = 0
    text: str = ""
    update_type: str = ""
    callback_id: str = ""
    message_id: str = ""
    max_name: str = ""
    max_username: str = ""
    attachments: list = field(default_factory=list)  # вложения сообщения (фото и др.)


def _extract_event(payload: dict) -> Event:
    update_type = payload.get("update_type", "")

    if update_type == "message_created":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        body = msg.get("body", {}) or {}
        attachments = body.get("attachments", []) or []
        return Event(
            user_id=int(sender.get("user_id") or 0),
            text=str(body.get("text", "")).strip(),
            update_type=update_type,
            message_id=str(body.get("mid", "") or ""),
            max_name=str(sender.get("name", "") or ""),
            max_username=str(sender.get("username", "") or ""),
            attachments=attachments,
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


def _parse_start_scenario_code(ev: Event) -> tuple[str, bool]:
    """
    (код сценария, is_stranger). «Незнакомец» — только bot_started или /start без аргумента,
    либо ссылка max.ru с пустым start=.
    """
    if ev.update_type == "bot_started":
        code = (ev.text or "").strip()
        return code, not bool(code)

    if ev.update_type == "message_created":
        t = (ev.text or "").strip()
        if t.startswith("/start"):
            parts = t.split(maxsplit=1)
            arg = (parts[1] if len(parts) > 1 else "").strip()
            return arg, not bool(arg)
        if "start=" in t and "max.ru" in t:
            try:
                code = t.split("start=", 1)[1].split("&")[0].strip()
            except Exception:
                code = ""
            return code, not bool(code)

    return "", False


def _short_replica_preview(stored: str, default: str) -> str:
    t = (stored or "").strip() or default
    return t if len(t) <= 400 else t[:397] + "…"


# ---------------------------------------------------------------------------
# FSM: подписчик — ФИО и телефон после «Далее» и проверки подписки (ТЗ)
# ---------------------------------------------------------------------------

async def _begin_user_fio_flow(api: MaxApiClient, user_id: int, scenario_code: str) -> None:
    fsm.set_state(user_id, "user_fio", {"scenario_code": scenario_code})
    await api.send_message(
        user_id,
        "Введите ФИО, на кого будет оформлена карта (фамилия, имя и отчество).",
    )


async def _user_proceed_to_fio_after_checks(
    api: MaxApiClient, repo: Repo, user_id: int, scenario_code: str
) -> None:
    scenario = repo.get_scenario_by_code(scenario_code)
    if not scenario:
        await api.send_message(user_id, "Сценарий не найден.")
        return
    if not offer_produces_valid_links(scenario.offer):
        await api.send_message(
            user_id,
            "Ошибка: для оффера не задана основная ссылка. Обратитесь к администратору.",
        )
        return
    await _begin_user_fio_flow(api, user_id, scenario_code)


async def _handle_user_fsm_text(
    api: MaxApiClient,
    repo: Repo,
    user_id: int,
    text: str,
    settings: Settings,
    max_name: str = "",
    max_username: str = "",
) -> bool:
    st = fsm.get_state(user_id)
    if not st:
        return False

    if st.state == "user_fio":
        if not validate_full_name(text):
            await api.send_message(
                user_id,
                "Укажите корректные ФИО: не менее двух слов, в каждом — больше одной буквы.",
            )
            return True
        fsm.set_state(user_id, "user_phone", st.data | {"full_name": text.strip()})
        await api.send_message(
            user_id,
            "Введите номер мобильного телефона, на кого будет оформлена карта "
            "(формат +7 или 8 не важен).",
        )
        return True

    if st.state == "user_phone":
        if not validate_phone(text):
            await api.send_message(
                user_id,
                "Укажите корректный номер телефона (не менее 10 цифр).",
            )
            return True
        scenario_code = st.data["scenario_code"]
        phone = text.strip()
        fsm.set_state(
            user_id,
            "user_await_consent",
            st.data | {"phone": phone, "max_name": max_name, "max_username": max_username},
        )
        await api.send_message_with_keyboard(
            user_id,
            "Ознакомьтесь с правилами сбора и хранения персональных данных и подтвердите согласие.",
            user_consent_keyboard(scenario_code, settings.personal_data_policy_url),
        )
        return True

    return False


async def _handle_user_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str,
    message_id: str, max_name: str, max_username: str, settings: Settings,
) -> None:
    await api.answer_callback(callback_id)

    if cb_payload == "user:noop":
        return

    if cb_payload.startswith("user:next:"):
        scenario_code = cb_payload[len("user:next:"):]
        scenario = repo.get_scenario_by_code(scenario_code)
        if not scenario:
            await api.send_message(user_id, "Сценарий не найден.")
            return
        if scenario.check_subscription:
            channels = repo.list_scenario_channels(scenario.id)
            if channels:
                not_subscribed = []
                for ch in channels:
                    if not await api.is_user_member_of_channel(ch.chat_id, user_id):
                        not_subscribed.append(ch)
                if not_subscribed:
                    await api.send_message_with_keyboard(
                        user_id,
                        "Для продолжения подпишитесь на каналы:",
                        user_subscribe_keyboard(not_subscribed, scenario_code),
                    )
                    return
        await _user_proceed_to_fio_after_checks(api, repo, user_id, scenario_code)
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
            if not await api.is_user_member_of_channel(ch.chat_id, user_id):
                not_subscribed.append(ch)

        if not_subscribed:
            await api.send_message_with_keyboard(
                user_id,
                "Вы ещё не подписаны на все каналы:",
                user_subscribe_keyboard(not_subscribed, scenario_code),
            )
            return

        await _user_proceed_to_fio_after_checks(api, repo, user_id, scenario_code)
        return

    if cb_payload.startswith("user:consent:"):
        scenario_code = cb_payload[len("user:consent:"):]
        st = fsm.get_state(user_id)
        if (
            not st
            or st.state != "user_await_consent"
            or str(st.data.get("scenario_code")) != str(scenario_code)
        ):
            await api.send_message(user_id, "Сначала введите ФИО и номер телефона.")
            return
        full_name = st.data.get("full_name", "")
        phone = st.data.get("phone", "")
        mn = st.data.get("max_name") or max_name
        mu = st.data.get("max_username") or max_username
        flow = UserFlowService(repo, settings)
        try:
            link = flow.issue_personal_link(
                user_id,
                scenario_code,
                full_name,
                phone,
                max_name=str(mn).strip() if mn else None,
                max_username=str(mu).strip() if mu else None,
            )
        except ValueError as e:
            await api.send_message(user_id, str(e))
            return
        fsm.clear_state(user_id)
        await api.send_message_with_keyboard(
            user_id,
            "Для оформления карты на сайте банка перейдите по ссылке ниже.",
            user_card_keyboard(link),
        )
        schedule_after_link_replica(user_id)
        return


# ---------------------------------------------------------------------------
# FSM: админ — текстовый ввод
# ---------------------------------------------------------------------------

async def _handle_admin_fsm_text(
    api: MaxApiClient, repo: Repo, user_id: int, text: str, attachments: list | None = None
) -> bool:
    st = fsm.get_state(user_id)
    if not st:
        return False

    state = st.state
    msg_id: str = st.data.get("_msg_id", "")
    msg_text: str = st.data.get("_msg_text", "")

    async def _reply(reply_text: str, buttons: list | None = None) -> None:
        """Убрать клавиатуру у предыдущего сообщения (если известен его текст), отправить новое."""
        if msg_id and msg_text:
            await api.edit_message(msg_id, msg_text, buttons=None)
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
        if not text:
            await api.send_message(user_id, "Название не может быть пустым.")
            return True
        fsm.set_state(user_id, "channel_add_invite", st.data | {"title": text})
        await api.send_message(
            user_id,
            "Отправьте ссылку-приглашение в канал или публичную ссылку на канал в MAX.\n"
            "Число (chat_id) вводить не нужно — бот определит канал по ссылке. "
            "Бот должен быть администратором канала.",
        )
        return True

    if state == "channel_add_invite":
        if not text or not text.strip():
            await api.send_message(user_id, "Пришлите ссылку на канал.")
            return True
        link = text.strip()
        settings_ch = _get_cached_settings()
        api_ch = MaxApiClient(settings_ch.bot_token)
        try:
            ok, chat_id, title_or_err = await api_ch.resolve_chat_from_invite_url(link)
            if not ok or chat_id is None:
                await api.send_message(user_id, f"⚠️ {title_or_err}\n\nПопробуйте другую ссылку.")
                return True
            ok_adm, adm_detail, eff_chat_id = await api_ch.check_bot_is_channel_admin(chat_id)
            if not ok_adm:
                await api.send_message(user_id, f"⚠️ {adm_detail}")
                return True
            chat_id = eff_chat_id if eff_chat_id is not None else chat_id
        finally:
            await api_ch.close()
        data = st.data
        fsm.clear_state(user_id)
        try:
            repo.add_required_channel(
                title=data["title"],
                chat_id=chat_id,
                invite_link=link,
            )
            channels = repo.list_required_channels()
            await _reply(f"✅ Канал «{data['title']}» добавлен.", admin_channels_keyboard(channels))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка добавления канала: {e}")
        return True

    if state == "scenario_add_title":
        if not text:
            return True
        data = st.data
        fsm.clear_state(user_id)
        try:
            code = f"sc{_secrets.token_hex(4)}"
            scenario = repo.create_scenario(
                offer_id=data["offer_id"],
                code=code,
                title=text,
            )
            settings = _get_cached_settings()
            if settings.bot_username:
                deep_link = f"https://max.ru/{settings.bot_username}?start={scenario.code}"
            else:
                deep_link = f"https://max.ru/start?start={scenario.code}"
            repo.create_or_update_bot_link(scenario.id, deep_link)
            channels = repo.list_scenario_channels(scenario.id)
            back = f"admin:offer_view:{data['offer_id']}" if data.get("offer_id") else "admin:scenarios"
            await _reply(
                f"✅ Сценарий «{text}» создан. Настройте его:",
                admin_scenario_settings_keyboard(scenario, channels, back_payload=back),
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
            return True

        # Ищем фото во вложениях сообщения
        image_url: str | None = None
        if attachments:
            logger.debug("scenario_edit_image attachments: %s", attachments)
        for att in (attachments or []):
            att_type = att.get("type", "")
            pld = att.get("payload", {}) or {}
            # MAX API может слать тип "image", "photo" или другие
            url_candidate = pld.get("url") or pld.get("photo_url")
            if url_candidate:
                image_url = url_candidate
                break
            # Если тип явно графический — берём token как запасной вариант
            if att_type in ("image", "photo") and pld.get("token"):
                image_url = pld["token"]
                break

        if image_url is None:
            if attachments:
                await api.send_message(user_id, "Не удалось сохранить изображение. Попробуйте отправить другое.")
                return True
            if not text:
                return True
            await api.send_message(user_id, "Отправьте изображение вложением.")
            return True

        repo.update_scenario_field(scenario_id, image_url=image_url)
        fsm.clear_state(user_id)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _reply("✅ Картинка сохранена.", admin_scenario_settings_keyboard(scenario, channels))
        return True

    if state == "scenario_edit_text":
        scenario_id = int(st.data.get("scenario_id", 0))
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            fsm.clear_state(user_id)
            return True
        if not text:
            return True  # пустое — игнорируем
        repo.update_scenario_field(scenario_id, description=text)
        fsm.clear_state(user_id)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _reply("✅ Текст сохранён.", admin_scenario_settings_keyboard(scenario, channels))
        return True

    if state == "scenario_channel_add":
        scenario_id = int(st.data.get("scenario_id", 0))
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Сценарий не найден.")
            return True

        if not text or not text.strip():
            await api.send_message(user_id, "Пришлите ссылку-приглашение или публичную ссылку на канал.")
            return True
        link = text.strip()

        settings_ch = _get_cached_settings()
        api_ch = MaxApiClient(settings_ch.bot_token)
        try:
            ok, chat_id, title_or_err = await api_ch.resolve_chat_from_invite_url(link)
            if not ok or chat_id is None:
                await api.send_message(user_id, f"⚠️ {title_or_err}\n\nПопробуйте другую ссылку.")
                return True
            ok_adm, ch_title, eff_chat_id = await api_ch.check_bot_is_channel_admin(chat_id)
            if not ok_adm:
                await api.send_message(
                    user_id,
                    f"⚠️ {ch_title}\n\nПришлите другую ссылку или нажмите «Назад».",
                )
                return True
            chat_id = eff_chat_id if eff_chat_id is not None else chat_id
        finally:
            await api_ch.close()

        fsm.clear_state(user_id)
        repo.add_scenario_channel(
            scenario_id=scenario_id,
            chat_id=chat_id,
            title=ch_title,
            invite_link=link,
        )
        channels = repo.list_scenario_channels(scenario_id)
        await _reply(f"✅ Канал «{ch_title}» добавлен.", admin_scenario_channels_keyboard(scenario_id, channels))
        return True

    if state == "replica_edit_stranger":
        if not (text or "").strip():
            await api.send_message(user_id, "Текст не может быть пустым.")
            return True
        repo.update_replica_stranger_text(text.strip())
        fsm.clear_state(user_id)
        rs = repo.get_replica_settings()
        await _reply(
            f"✅ Сохранено.\n\nТекущий текст «Для незнакомцев»:\n{rs.stranger_text}",
            admin_replicas_menu_keyboard((_get_cached_settings().personal_data_policy_url or "").strip() or None),
        )
        return True

    if state == "replica_edit_after":
        if not (text or "").strip():
            await api.send_message(user_id, "Текст не может быть пустым.")
            return True
        repo.update_replica_after_link_text(text.strip())
        fsm.clear_state(user_id)
        rs = repo.get_replica_settings()
        await _reply(
            f"✅ Сохранено.\n\nТекущий текст «После акции»:\n{rs.after_link_text}",
            admin_replicas_menu_keyboard((_get_cached_settings().personal_data_policy_url or "").strip() or None),
        )
        return True

    # --- Рассылка (мастер) ---

    if state == "broadcast_w_title":
        if not text.strip():
            await api.send_message(user_id, "Заголовок не может быть пустым.")
            return True
        fsm.set_state(user_id, "broadcast_w_image", {"title": text.strip()})
        await api.send_message_with_keyboard(
            user_id,
            "Пришлите изображение или нажмите «Без картинки».",
            admin_broadcast_skip_image_keyboard(),
        )
        return True

    if state == "broadcast_w_image":
        image_ref = _extract_broadcast_image_ref(attachments)

        if image_ref is None:
            if not (text or "").strip():
                await api.send_message(
                    user_id,
                    "Пришлите изображение или нажмите «Без картинки».",
                )
                return True
            await api.send_message(
                user_id,
                "Нужна картинка файлом или «Без картинки».",
            )
            return True

        token_ready = await api.resolve_broadcast_image_token(image_ref)
        if not token_ready:
            await api.send_message(
                user_id,
                "Не удалось сохранить изображение. Отправьте файл ещё раз или «Без картинки».",
            )
            return True

        fsm.set_state(user_id, "broadcast_w_text", st.data | {"image_url": token_ready})
        await api.send_message_with_keyboard(
            user_id,
            "Введите текст уведомления или нажмите «Без текста».",
            admin_broadcast_skip_text_keyboard(),
        )
        return True

    if state == "broadcast_w_text":
        if not text.strip():
            await api.send_message_with_keyboard(
                user_id,
                "Введите текст или нажмите «Без текста».",
                admin_broadcast_skip_text_keyboard(),
            )
            return True
        fsm.set_state(user_id, "broadcast_w_button_text", st.data | {"text": text.strip()})
        await api.send_message_with_keyboard(
            user_id,
            f"Введите текст на кнопке.\n\n"
            f"По умолчанию: «{_BROADCAST_DEFAULT_BUTTON_TEXT}» — или нажмите кнопку с этой надписью ниже.",
            admin_broadcast_default_button_keyboard(_BROADCAST_DEFAULT_BUTTON_TEXT),
        )
        return True

    if state == "broadcast_w_button_text":
        if not text.strip():
            await api.send_message(
                user_id,
                f"Введите текст кнопки или нажмите «{_BROADCAST_DEFAULT_BUTTON_TEXT}» ниже.\n"
                f"Значение по умолчанию: «{_BROADCAST_DEFAULT_BUTTON_TEXT}».",
            )
            return True
        fsm.set_state(user_id, "broadcast_w_button_url", st.data | {"button_text": text.strip()})
        await api.send_message(user_id, "Введите адрес, куда будет вести кнопка:")
        return True

    if state == "broadcast_w_button_url":
        if not text.strip():
            await api.send_message(user_id, "Адрес не может быть пустым.")
            return True
        url = _normalize_broadcast_https_url(text)
        data = st.data | {"button_url": url}
        fsm.set_state(user_id, "broadcast_preview", data)
        preview = _format_broadcast_preview(data)
        await api.send_message_with_keyboard(user_id, preview, admin_broadcast_preview_keyboard())
        return True

    if state == "broadcast_w_schedule":
        when = _parse_broadcast_schedule(text)
        if not when:
            await api.send_message(
                user_id,
                "Не удалось разобрать дату. Пример (московское время): 18.04.2026 15:30",
            )
            return True
        if when <= datetime.utcnow():
            await api.send_message(user_id, "Укажите дату и время в будущем.")
            return True
        data = st.data
        fsm.clear_state(user_id)
        try:
            bc = repo.create_broadcast(
                title=data["title"],
                text=data["text"],
                button_url=data["button_url"],
                button_text=data.get("button_text", _BROADCAST_DEFAULT_BUTTON_TEXT),
                image_url=data.get("image_url"),
                send_at=when,
            )
            schedule_broadcast_job(bc.id, bc.send_at)
            msk = ZoneInfo("Europe/Moscow")
            local = when.replace(tzinfo=ZoneInfo("UTC")).astimezone(msk).strftime("%d.%m.%Y %H:%M")
            await api.send_message_with_keyboard(
                user_id,
                f"✅ Рассылка «{bc.title}» запланирована на {local} (МСК).",
                admin_main_keyboard(),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка: {e}")
        return True

    if state == "broadcast_reschedule_at":
        bid = int(st.data.get("broadcast_id", 0))
        when = _parse_broadcast_schedule(text)
        if not when:
            await api.send_message(
                user_id,
                "Не удалось разобрать дату. Пример (московское время): 18.04.2026 15:30",
            )
            return True
        if when <= datetime.utcnow():
            await api.send_message(user_id, "Укажите дату и время в будущем.")
            return True
        b = repo.get_broadcast(bid)
        if not b or b.status != "scheduled":
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Рассылка уже недоступна для переноса.")
            return True
        _remove_broadcast_scheduler_job(bid)
        if not repo.set_broadcast_send_at(bid, when):
            fsm.clear_state(user_id)
            await api.send_message(user_id, "Не удалось сохранить время.")
            return True
        schedule_broadcast_job(bid, when)
        fsm.clear_state(user_id)
        msk = ZoneInfo("Europe/Moscow")
        local = when.replace(tzinfo=ZoneInfo("UTC")).astimezone(msk).strftime("%d.%m.%Y %H:%M")
        total = repo.count_broadcasts()
        ps = BROADCAST_MANAGE_PAGE_SIZE
        items = repo.list_broadcasts_paged(0, ps)
        await api.send_message_with_keyboard(
            user_id,
            f"✅ Рассылка #{bid} перенесена на {local} (МСК).",
            admin_broadcast_manage_keyboard(0, total, items),
        )
        return True

    if state == "broadcast_preview":
        await api.send_message(
            user_id,
            "Используйте кнопки под превью: «Отправить сейчас», «Отправить позже» или «Отмена».",
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
        nonlocal _acked
        # Приоритет 1: POST /answers с message — атомарный ack+edit в одном запросе.
        # Не попадает под rate-limit PUT /messages.
        if callback_id:
            ok = await api.answer_callback_with_edit(callback_id, text, buttons)
            if ok:
                _acked = True  # callback уже подтверждён внутри answer_callback_with_edit
                return
        # Приоритет 2: PUT /messages (отдельный запрос)
        edited = False
        if message_id:
            edited = await api.edit_message(message_id, text, buttons)
        if not edited:
            logger.warning("edit failed mid=%r — new message fallback", message_id)
            await api.send_message_with_keyboard(user_id, text, buttons or [])
        await _ack()

    async def _edit_then_ask(
        text_edit: str, question: str, question_buttons: list | None = None
    ) -> None:
        """Убрать клавиатуру в текущем сообщении, задать вопрос новым. Сохраняет message_id в FSM."""
        nonlocal _acked
        if callback_id:
            ok = await api.answer_callback_with_edit(callback_id, text_edit, buttons=None)
            if ok:
                _acked = True
                fsm.update_data(user_id, _msg_id=message_id, _msg_text=text_edit)
                if question_buttons is not None:
                    await api.send_message_with_keyboard(user_id, question, question_buttons)
                else:
                    await api.send_message(user_id, question)
                return
        if message_id:
            await api.edit_message(message_id, text_edit, buttons=None)
            fsm.update_data(user_id, _msg_id=message_id, _msg_text=text_edit)
        await _ack()
        if question_buttons is not None:
            await api.send_message_with_keyboard(user_id, question, question_buttons)
        else:
            await api.send_message(user_id, question)

    if cb_payload == "admin:main":
        fsm.clear_state(user_id)
        await _edit("Админ-меню:", admin_main_keyboard())
        return

    if cb_payload == "admin:replicas":
        fsm.clear_state(user_id)
        rs = repo.get_replica_settings()
        s1 = _short_replica_preview(rs.stranger_text, DEFAULT_REPLICA_STRANGER)
        s2 = _short_replica_preview(rs.after_link_text, DEFAULT_REPLICA_AFTER_LINK)
        policy_url = (_get_cached_settings().personal_data_policy_url or "").strip()
        policy_block = (
            f"\n\n📄 Правила обработки данных (как при согласии, из .env):\n{policy_url}"
            if policy_url
            else "\n\n📄 Правила: не заданы (PERSONAL_DATA_POLICY_URL в .env)."
        )
        await _edit(
            "💬 Управление репликами\n\n"
            "К обоим сообщениям добавляются кнопки последних 10 офферов "
            "(по дате создания карточки; ссылка — на сценарий оффера).\n\n"
            f"👤 Для незнакомцев (вход без кода в ссылке):\n{s1}\n\n"
            f"⏱ После акции (через 5 мин после выдачи ссылки на карту):\n{s2}"
            f"{policy_block}",
            admin_replicas_menu_keyboard(policy_url if policy_url else None),
        )
        return

    if cb_payload.startswith("admin:replica_edit:"):
        kind = cb_payload.split(":")[-1]
        if kind not in ("stranger", "after"):
            await _ack()
            return
        rs = repo.get_replica_settings()
        current = rs.stranger_text if kind == "stranger" else rs.after_link_text
        title = (
            "Для незнакомцев"
            if kind == "stranger"
            else "После акции (через 5 мин после выдачи ссылки на карту)"
        )
        fsm.set_state(user_id, f"replica_edit_{kind}", {})
        await _edit_then_ask(
            f"Редактирование: {title}",
            f"Текущий текст:\n\n{current}\n\nОтправьте новый текст сообщения:",
            admin_replica_cancel_keyboard(),
        )
        return

    # --- Платформы ---
    if cb_payload == "admin:platforms" or (cb_payload.startswith("admin:platforms:") and cb_payload.split(":")[-1].lstrip("-").isdigit()):
        fsm.clear_state(user_id)
        parts = cb_payload.split(":")
        page = int(parts[2]) if len(parts) > 2 else 0
        platforms = repo.list_platforms()
        text = "Платформы:" if platforms else "Платформ пока нет."
        await _edit(text, admin_platforms_keyboard(platforms, page))
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
        parts = cb_payload.split(":")
        platform_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        offers = repo.list_offers_for_platform(platform_id)
        text = "Офферы платформы:" if offers else "Офферов пока нет."
        await _edit(text, admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}", platform_id=platform_id, page=page))
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
    if cb_payload == "admin:offers" or (cb_payload.startswith("admin:offers:") and cb_payload.split(":")[-1].lstrip("-").isdigit()):
        fsm.clear_state(user_id)
        parts = cb_payload.split(":")
        page = int(parts[2]) if len(parts) > 2 else 0
        offers = repo.list_offers()
        text = "Все офферы:" if offers else "Офферов пока нет."
        await _edit(text, admin_offers_keyboard(offers, page=page))
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
                admin_scenario_settings_keyboard(scenario, channels, back_payload=f"admin:offer_view:{offer_id}"),
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
        offer_id = scenario.offer_id
        await _edit(
            f"Сценарий оффера:",
            admin_scenario_settings_keyboard(scenario, channels, back_payload=f"admin:offer_view:{offer_id}"),
        )
        return

    if cb_payload.startswith("admin:scenario_set_image:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        msg_text = "📷 Отправьте изображение сообщением ниже."
        fsm.set_state(user_id, "scenario_edit_image", {
            "scenario_id": scenario_id,
            "_msg_id": message_id,
            "_msg_text": msg_text,
        })
        await _edit(msg_text, [[{"type": "callback", "text": "⏭ Пропустить", "payload": f"admin:scenario_skip_image:{scenario_id}"}]])
        return

    if cb_payload.startswith("admin:scenario_skip_image:"):
        scenario_id = int(cb_payload.split(":")[-1])
        fsm.clear_state(user_id)
        repo.update_scenario_field(scenario_id, image_url=None)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _edit("✅ Картинка убрана.", admin_scenario_settings_keyboard(scenario, channels))
        return

    if cb_payload.startswith("admin:scenario_set_text:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            return
        cur = f"\n\nТекущий текст:\n{scenario.description}" if scenario.description else ""
        msg_text = f"📝 Введите текст акции, который увидит подписчик.{cur}"
        fsm.set_state(user_id, "scenario_edit_text", {
            "scenario_id": scenario_id,
            "_msg_id": message_id,
            "_msg_text": msg_text,
        })
        await _edit(msg_text, [[{"type": "callback", "text": "⏭ Пропустить", "payload": f"admin:scenario_skip_text:{scenario_id}"}]])
        return

    if cb_payload.startswith("admin:scenario_skip_text:"):
        scenario_id = int(cb_payload.split(":")[-1])
        fsm.clear_state(user_id)
        repo.update_scenario_field(scenario_id, description=None)
        scenario = repo.db.get(Scenario, scenario_id)
        channels = repo.list_scenario_channels(scenario_id)
        await _edit("✅ Текст убран.", admin_scenario_settings_keyboard(scenario, channels))
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
            "Отправьте ссылку-приглашение в канал или публичную ссылку на канал в MAX.\n"
            "Число (chat_id) вводить не нужно. Бот должен быть администратором канала.",
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
    if cb_payload == "admin:scenarios" or (cb_payload.startswith("admin:scenarios:") and cb_payload.split(":")[-1].lstrip("-").isdigit()):
        fsm.clear_state(user_id)
        parts = cb_payload.split(":")
        page = int(parts[2]) if len(parts) > 2 else 0
        scenarios = repo.list_scenarios()
        text = "Сценарии:" if scenarios else "Сценариев пока нет."
        await _edit(text, admin_scenarios_keyboard(scenarios, page))
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
        channels = repo.list_scenario_channels(scenario_id)
        await _edit(
            f"Сценарий: {scenario.title}",
            admin_scenario_settings_keyboard(scenario, channels, back_payload="admin:scenarios"),
        )
        return

    if cb_payload.startswith("admin:scenario_delete:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        name = scenario.title if scenario else f"#{scenario_id}"
        # Определяем, откуда пришли (из списка или из оффера), чтобы вернуться туда после отмены
        cancel_back = f"admin:scenario_view:{scenario_id}"
        await _edit(
            f"Удалить сценарий «{name}»?",
            admin_confirm_delete_keyboard(f"admin:scenario_delete_yes:{scenario_id}", cancel_back),
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
    if cb_payload == "admin:channels" or (cb_payload.startswith("admin:channels:") and cb_payload.split(":")[-1].lstrip("-").isdigit()):
        fsm.clear_state(user_id)
        parts = cb_payload.split(":")
        page = int(parts[2]) if len(parts) > 2 else 0
        channels = repo.list_required_channels()
        text = "Каналы подписки:" if channels else "Каналов пока нет."
        await _edit(text, admin_channels_keyboard(channels, page))
        return

    if cb_payload == "admin:channel_add":
        fsm.set_state(user_id, "channel_add_title")
        await _edit_then_ask("Добавление канала:", "Введите название канала:")
        return

    if cb_payload == "admin:channel_link_skip":
        st = fsm.get_state(user_id)
        if st and st.state == "channel_add_invite":
            await _edit("Нужна ссылка на канал — без неё нельзя добавить канал. Пришлите ссылку сообщением.")
        return

    if cb_payload == "admin:scenario_ch_link_skip":
        st = fsm.get_state(user_id)
        if st and st.state == "scenario_channel_add":
            await _edit("Нужна ссылка на канал. Пришлите ссылку сообщением.")
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
        await _edit(
            "Рассылка всем пользователям из базы лидов:\n"
            "изображение (по желанию), текст (можно пропустить), кнопка.\n"
            "Можно отправить сразу или запланировать.",
            admin_broadcast_entry_keyboard(),
        )
        return

    if cb_payload.startswith("admin:broadcast_manage:"):
        fsm.clear_state(user_id)
        try:
            page = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            page = 0
        total = repo.count_broadcasts()
        ps = BROADCAST_MANAGE_PAGE_SIZE
        total_pages = max(1, (total + ps - 1) // ps) if total else 1
        page = max(0, min(page, total_pages - 1))
        items = repo.list_broadcasts_paged(page * ps, ps)
        if total:
            body = (
                "📬 Управление рассылками\n\n"
                f"Страница {page + 1} из {total_pages} (всего {total}).\n"
                "Выберите рассылку:"
            )
        else:
            body = "📬 Управление рассылками\n\nПока нет ни одной рассылки."
        await _edit(body, admin_broadcast_manage_keyboard(page, total, items))
        return

    if cb_payload.startswith("admin:broadcast_view:"):
        fsm.clear_state(user_id)
        try:
            bid = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            return
        b = repo.get_broadcast(bid)
        if not b:
            await _edit("Рассылка не найдена.", admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), []))
            return
        await _edit(_format_broadcast_detail(b), admin_broadcast_detail_keyboard(bid, b.status))
        return

    if cb_payload.startswith("admin:broadcast_now:"):
        try:
            bid = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            await _ack()
            return
        b = repo.get_broadcast(bid)
        if not b or b.status != "scheduled":
            await _edit(
                "Нельзя отправить: рассылка не в статусе ожидания.",
                admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), repo.list_broadcasts_paged(0, BROADCAST_MANAGE_PAGE_SIZE)),
            )
            return
        _remove_broadcast_scheduler_job(bid)
        repo.set_broadcast_send_at(bid, None)
        launch_broadcast_now(bid)
        total = repo.count_broadcasts()
        ps = BROADCAST_MANAGE_PAGE_SIZE
        await _edit(
            f"✅ Рассылка #{bid} поставлена в очередь на отправку.",
            admin_broadcast_manage_keyboard(0, total, repo.list_broadcasts_paged(0, ps)),
        )
        return

    if cb_payload.startswith("admin:broadcast_reschedule:"):
        try:
            bid = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            await _ack()
            return
        b = repo.get_broadcast(bid)
        if not b or b.status != "scheduled":
            await _edit("Перенос недоступен.", admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), repo.list_broadcasts_paged(0, BROADCAST_MANAGE_PAGE_SIZE)))
            return
        fsm.set_state(user_id, "broadcast_reschedule_at", {"broadcast_id": bid})
        await _edit(
            "Укажите дату и время отправки (московское время), например:\n18.04.2026 15:30",
            admin_broadcast_manage_cancel_keyboard(),
        )
        return

    if cb_payload.startswith("admin:broadcast_cancel_pending:"):
        try:
            bid = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            await _ack()
            return
        b = repo.get_broadcast(bid)
        if not b:
            await _edit("Не найдено.", admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), []))
            return
        if repo.cancel_pending_broadcast(bid):
            _remove_broadcast_scheduler_job(bid)
            await _edit(
                f"Рассылка #{bid} отменена.",
                admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), repo.list_broadcasts_paged(0, BROADCAST_MANAGE_PAGE_SIZE)),
            )
        else:
            await _edit(
                "Отмена недоступна (уже отправлена или не в ожидании).",
                admin_broadcast_detail_keyboard(bid, b.status),
            )
        return

    if cb_payload.startswith("admin:broadcast_repeat:"):
        try:
            oid = int(cb_payload.rsplit(":", 1)[-1])
        except ValueError:
            await _ack()
            return
        nb = repo.duplicate_broadcast(oid)
        if not nb:
            await _edit("Не удалось создать копию.", admin_broadcast_manage_keyboard(0, repo.count_broadcasts(), []))
            return
        fsm.clear_state(user_id)
        await _edit(
            _format_broadcast_detail(nb) + "\n\nСоздана копия — можно отправить или запланировать.",
            admin_broadcast_detail_keyboard(nb.id, nb.status),
        )
        return

    if cb_payload == "admin:broadcast_new":
        fsm.set_state(user_id, "broadcast_w_title", {})
        await _edit_then_ask(
            "Новая рассылка",
            "Введите короткий заголовок (для истории):",
        )
        return

    if cb_payload == "admin:broadcast_skip_image":
        st = fsm.get_state(user_id)
        if not st or st.state != "broadcast_w_image":
            await _ack()
            return
        fsm.set_state(user_id, "broadcast_w_text", st.data | {"image_url": None})
        await _edit_then_ask(
            "Без картинки",
            "Введите текст уведомления или нажмите «Без текста».",
            admin_broadcast_skip_text_keyboard(),
        )
        return

    if cb_payload == "admin:broadcast_skip_text":
        st = fsm.get_state(user_id)
        if not st or st.state != "broadcast_w_text":
            await _ack()
            return
        fsm.set_state(user_id, "broadcast_w_button_text", st.data | {"text": ""})
        await _edit_then_ask(
            "Без текста",
            f"Введите текст на кнопке.\n\n"
            f"По умолчанию: «{_BROADCAST_DEFAULT_BUTTON_TEXT}» — или нажмите кнопку с этой надписью ниже.",
            admin_broadcast_default_button_keyboard(_BROADCAST_DEFAULT_BUTTON_TEXT),
        )
        return

    if cb_payload == "admin:broadcast_default_btn":
        st = fsm.get_state(user_id)
        if not st or st.state != "broadcast_w_button_text":
            await _ack()
            return
        fsm.set_state(
            user_id, "broadcast_w_button_url", st.data | {"button_text": _BROADCAST_DEFAULT_BUTTON_TEXT}
        )
        await _edit_then_ask(
            f"Текст кнопки: «{_BROADCAST_DEFAULT_BUTTON_TEXT}»",
            "Введите адрес, куда будет вести кнопка:",
        )
        return

    if cb_payload == "admin:broadcast_send_now":
        st = fsm.get_state(user_id)
        if not st or st.state != "broadcast_preview":
            await _ack()
            return
        data = st.data
        fsm.clear_state(user_id)
        try:
            bc = repo.create_broadcast(
                title=data["title"],
                text=data["text"],
                button_url=data["button_url"],
                button_text=data.get("button_text", _BROADCAST_DEFAULT_BUTTON_TEXT),
                image_url=data.get("image_url"),
                send_at=None,
            )
            launch_broadcast_now(bc.id)
            await _edit(
                f"✅ Рассылка «{bc.title}» запущена.",
                admin_main_keyboard(),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка: {e}")
            await _ack()
        return

    if cb_payload == "admin:broadcast_send_later":
        st = fsm.get_state(user_id)
        if not st or st.state != "broadcast_preview":
            await _ack()
            return
        fsm.set_state(user_id, "broadcast_w_schedule", st.data)
        await _edit(
            "Запланировать отправку.\n"
            "Укажите дату и время в одном сообщении (московское время),\n"
            "например: 18.04.2026 15:30\n"
            "или дату в формате ISO с часовым поясом.",
            admin_broadcast_schedule_cancel_keyboard(),
        )
        return

    if cb_payload == "admin:broadcast_cancel":
        fsm.clear_state(user_id)
        await _edit("Админ-меню:", admin_main_keyboard())
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
            if ev.callback_id and _is_duplicate_callback(ev.callback_id):
                logger.info("Duplicate callback_id=%s — skip", ev.callback_id[:20])
                return Response(status_code=200)
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
            handled = await _handle_user_fsm_text(
                api, repo, ev.user_id, ev.text, settings, ev.max_name, ev.max_username
            )
            if handled:
                return Response(status_code=200)

        # FSM: admin-ввод
        if is_admin and ev.update_type == "message_created":
            handled = await _handle_admin_fsm_text(api, repo, ev.user_id, ev.text, ev.attachments)
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
        # message_created: /start <code> как fallback, а также полный URL deep link
        scenario_code, is_stranger_start = _parse_start_scenario_code(ev)

        if scenario_code:
            scenario = repo.get_scenario_by_code(scenario_code)
            if not scenario:
                await api.send_message(ev.user_id, "Сценарий не найден. Используйте корректную ссылку.")
                return Response(status_code=200)

            fsm.clear_state(ev.user_id)

            desc = (scenario.description or "").strip()
            title = (scenario.title or "Акция").strip()
            kb = user_material_keyboard(scenario_code, None)

            if not offer_produces_valid_links(scenario.offer):
                await api.send_message(
                    ev.user_id,
                    "Ошибка: для оффера не задана основная ссылка. Обратитесь к администратору.",
                )
                return Response(status_code=200)

            if scenario.image_url:
                token = await api.resolve_broadcast_image_token(scenario.image_url)
                if token:
                    body = desc if desc else title
                    await api.send_message_with_image_and_keyboard(ev.user_id, body, token, kb)
                else:
                    body = "\n\n".join(x for x in (title, desc) if x).strip() or title
                    await api.send_message_with_keyboard(ev.user_id, body, kb)
            else:
                body = desc if desc else title
                await api.send_message_with_keyboard(ev.user_id, body, kb)

            return Response(status_code=200)

        if is_stranger_start:
            fsm.clear_state(ev.user_id)
            rs = repo.get_replica_settings()
            text = (rs.stranger_text or "").strip() or DEFAULT_REPLICA_STRANGER
            await send_replica_with_offers(api, repo, settings, ev.user_id, body_text=text)
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
