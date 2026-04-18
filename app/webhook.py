import logging
import secrets as _secrets

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
    admin_export_offers_keyboard,
    admin_export_platforms_keyboard,
    admin_main_keyboard,
    admin_offer_select_platform_keyboard,
    admin_offer_view_keyboard,
    admin_offers_keyboard,
    admin_platform_view_keyboard,
    admin_platforms_keyboard,
    admin_scenario_select_offer_keyboard,
    admin_scenario_view_keyboard,
    admin_scenarios_keyboard,
)
from app.keyboards.user import (
    user_card_keyboard,
    user_channels_keyboard,
    user_consent_keyboard,
    user_start_keyboard,
)
from app.max_api import MaxApiClient, RateLimitError
from app.services.export_service import ExportService
from app.services.link_builder import build_offer_link
from app.validators import validate_full_name, validate_phone

router = APIRouter(tags=["webhook"])
logger = logging.getLogger(__name__)


def _get_cached_settings() -> Settings:
    return get_settings()


# ---------------------------------------------------------------------------
# Парсинг входящего события
# ---------------------------------------------------------------------------

def _extract_event(payload: dict) -> tuple[int, str, str, str, str, str]:
    """Возвращает (user_id, text, update_type, callback_id, max_name, max_username)."""
    update_type = payload.get("update_type", "")

    if update_type == "message_created":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        body = msg.get("body", {}) or {}
        return (
            int(sender.get("user_id") or 0),
            str(body.get("text", "")).strip(),
            update_type,
            "",
            str(sender.get("name", "") or ""),
            str(sender.get("username", "") or ""),
        )

    if update_type == "message_callback":
        cb = payload.get("callback", {}) or {}
        user = cb.get("user", {}) or {}
        return (
            int(user.get("user_id") or 0),
            str(cb.get("payload", "")).strip(),
            update_type,
            str(cb.get("callback_id", "")),
            str(user.get("name", "") or ""),
            str(user.get("username", "") or ""),
        )

    if update_type == "bot_started":
        # MAX передаёт значение ?start= в поле payload верхнего уровня.
        # user может быть как верхнеуровневым полем, так и внутри message.sender.
        user = payload.get("user", {}) or {}
        if not user:
            msg = payload.get("message", {}) or {}
            user = msg.get("sender", {}) or {}
        start_payload = str(payload.get("payload", "") or "").strip()
        return (
            int(user.get("user_id") or 0),
            start_payload,   # код сценария напрямую, без /start
            update_type,
            "",
            str(user.get("name", "") or ""),
            str(user.get("username", "") or ""),
        )

    return int(payload.get("user_id", 0)), str(payload.get("text", "")).strip(), update_type, "", "", ""


# ---------------------------------------------------------------------------
# FSM: подписчик
# ---------------------------------------------------------------------------

async def _handle_user_fsm_text(
    api: MaxApiClient, repo: Repo, user_id: int, text: str, settings: Settings
) -> bool:
    st = fsm.get_state(user_id)
    if not st or not st.state.startswith("user:"):
        return False

    state = st.state

    if state == "user:enter_fio":
        if not validate_full_name(text):
            await api.send_message(user_id, "Введите ФИО полностью (минимум имя и фамилия, например: Иванов Иван Иванович):")
            return True
        fsm.set_state(user_id, "user:enter_phone", st.data | {"full_name": text})
        await api.send_message(user_id, "Введите номер мобильного телефона:")
        return True

    if state == "user:enter_phone":
        if not validate_phone(text):
            await api.send_message(user_id, "Введите корректный номер телефона (например: +79001234567):")
            return True
        data = st.data | {"phone": text}
        fsm.set_state(user_id, "user:await_consent", data)
        await api.send_message_with_keyboard(
            user_id,
            "Для получения ссылки необходимо согласиться с правилами сбора и хранения персональных данных.",
            user_consent_keyboard(data["scenario_code"], settings.personal_data_policy_url),
        )
        return True

    return False


async def _issue_link(api: MaxApiClient, repo: Repo, user_id: int, data: dict) -> None:
    scenario = repo.get_scenario_by_code(data["scenario_code"])
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
        full_name=data["full_name"],
        phone=data["phone"],
        subid_value=subid,
        consent_accepted=True,
        max_name=data.get("max_name") or None,
        max_username=data.get("max_username") or None,
    )
    await api.send_message_with_keyboard(
        user_id,
        "Для оформления карты на сайте банка перейдите по ссылке ниже:",
        user_card_keyboard(final_link),
    )


async def _handle_user_callback(
    api: MaxApiClient, repo: Repo, user_id: int, cb_payload: str, callback_id: str,
    max_name: str, max_username: str, settings: Settings,
) -> None:
    await api.answer_callback(callback_id)

    if cb_payload.startswith("user:next:"):
        scenario_code = cb_payload[len("user:next:"):]
        scenario = repo.get_scenario_by_code(scenario_code)
        if not scenario:
            await api.send_message(user_id, "Сценарий не найден.")
            return

        channels = repo.list_required_channels()
        if channels:
            not_subscribed = []
            for ch in channels:
                member = await api.get_chat_member(ch.chat_id, user_id)
                if not member:
                    not_subscribed.append(ch)
            if not_subscribed:
                await api.send_message_with_keyboard(
                    user_id,
                    "Для продолжения необходимо подписаться на каналы:",
                    user_channels_keyboard(not_subscribed, scenario_code),
                )
                return

        fsm.set_state(user_id, "user:enter_fio", {
            "scenario_code": scenario_code,
            "max_name": max_name,
            "max_username": max_username,
        })
        await api.send_message(user_id, "Введите ФИО на кого будет оформлена карта:")
        return

    if cb_payload.startswith("user:check_sub:"):
        scenario_code = cb_payload[len("user:check_sub:"):]
        channels = repo.list_required_channels()
        not_subscribed = []
        for ch in channels:
            member = await api.get_chat_member(ch.chat_id, user_id)
            if not member:
                not_subscribed.append(ch)
        if not_subscribed:
            await api.send_message_with_keyboard(
                user_id,
                "Вы ещё не подписаны на все каналы:",
                user_channels_keyboard(not_subscribed, scenario_code),
            )
            return
        fsm.set_state(user_id, "user:enter_fio", {
            "scenario_code": scenario_code,
            "max_name": max_name,
            "max_username": max_username,
        })
        await api.send_message(user_id, "Введите ФИО на кого будет оформлена карта:")
        return

    if cb_payload.startswith("user:consent:"):
        scenario_code = cb_payload[len("user:consent:"):]
        st = fsm.get_state(user_id)
        if not st or st.state != "user:await_consent":
            await api.send_message(user_id, "Сначала введите ФИО и телефон. Напишите /start для начала.")
            return
        data = st.data
        fsm.clear_state(user_id)
        await _issue_link(api, repo, user_id, data)
        return


# ---------------------------------------------------------------------------
# FSM: админ — текстовый ввод
# ---------------------------------------------------------------------------

async def _handle_admin_fsm_text(api: MaxApiClient, repo: Repo, user_id: int, text: str) -> bool:
    st = fsm.get_state(user_id)
    if not st:
        return False

    state = st.state

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
            await api.send_message_with_keyboard(
                user_id,
                f"✅ Оффер «{data['name']}» добавлен.\n\nПример ссылки:\n{example_link}{sep}{subid_param}=0001",
                admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}"),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка создания оффера: {e}")
        return True

    if state == "channel_add_title":
        fsm.set_state(user_id, "channel_add_id", {"title": text})
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
            await api.send_message_with_keyboard(
                user_id, f"✅ Канал «{data['title']}» добавлен.", admin_channels_keyboard(channels)
            )
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
                description=data["description"],
                image_url=image_url,
            )
            settings = _get_cached_settings()
            if settings.bot_username:
                deep_link = f"https://max.ru/join/{settings.bot_username}?start={scenario.code}"
            else:
                deep_link = f"https://max.ru/start?start={scenario.code}"  # fallback без bot_username
            repo.create_or_update_bot_link(scenario.id, deep_link)
            scenarios = repo.list_scenarios()
            await api.send_message_with_keyboard(
                user_id,
                f"✅ Сценарий «{data['title']}» создан.\n\nКод: {scenario.code}\nСсылка: {deep_link}",
                admin_scenarios_keyboard(scenarios),
            )
        except Exception as e:
            await api.send_message(user_id, f"Ошибка создания сценария: {e}")
        return True

    return False


# ---------------------------------------------------------------------------
# Callback-обработчики: admin
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

    if cb_payload.startswith("admin:platform_view:"):
        platform_id = int(cb_payload.split(":")[-1])
        from app.db.models import Platform as _Platform
        platform = repo.db.get(_Platform, platform_id)
        if not platform:
            await api.send_message(user_id, "Платформа не найдена.")
            return
        offers = repo.list_offers_for_platform(platform_id)
        offers_text = f"\nОфферов: {len(offers)}" if offers else "\nОфферов пока нет."
        await api.send_message_with_keyboard(
            user_id, f"Платформа: {platform.name}{offers_text}", admin_platform_view_keyboard(platform_id)
        )
        return

    if cb_payload.startswith("admin:platform_offers:"):
        platform_id = int(cb_payload.split(":")[-1])
        offers = repo.list_offers_for_platform(platform_id)
        text = "Офферы платформы:" if offers else "Офферов пока нет."
        await api.send_message_with_keyboard(
            user_id, text, admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}")
        )
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
        text = "Все офферы:" if offers else "Офферов пока нет."
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

    if cb_payload.startswith("admin:offer_view:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            await api.send_message(user_id, "Оффер не найден.")
            return
        sep = "&" if "?" in offer.base_url else "?"
        example = f"{offer.base_url}{sep}{offer.subid_param}=0001"
        await api.send_message_with_keyboard(
            user_id,
            f"Оффер: {offer.name}\nПример ссылки:\n{example}",
            admin_offer_view_keyboard(offer_id, offer.platform_id),
        )
        return

    if cb_payload.startswith("admin:offer_delete:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        platform_id = offer.platform_id if offer else None
        try:
            repo.delete_offer(offer_id)
            if platform_id:
                offers = repo.list_offers_for_platform(platform_id)
                await api.send_message_with_keyboard(
                    user_id, "Оффер удалён.",
                    admin_offers_keyboard(offers, back_payload=f"admin:platform_view:{platform_id}")
                )
            else:
                await api.send_message(user_id, "Оффер удалён.")
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

    if cb_payload == "admin:scenario_add":
        offers = repo.list_offers()
        if not offers:
            await api.send_message(user_id, "Сначала добавьте хотя бы один оффер.")
            return
        await api.send_message_with_keyboard(
            user_id, "Выберите оффер для нового сценария:", admin_scenario_select_offer_keyboard(offers)
        )
        return

    if cb_payload.startswith("admin:scenario_select_offer:"):
        offer_id = int(cb_payload.split(":")[-1])
        fsm.set_state(user_id, "scenario_add_title", {"offer_id": offer_id})
        await api.send_message(user_id, "Введите название сценария (заголовок акции):")
        return

    if cb_payload.startswith("admin:scenario_view:"):
        scenario_id = int(cb_payload.split(":")[-1])
        scenario = repo.db.get(Scenario, scenario_id)
        if not scenario:
            await api.send_message(user_id, "Сценарий не найден.")
            return
        bot_link = getattr(scenario, "bot_link", None)
        link_text = f"\nСсылка: {bot_link.deep_link}" if bot_link else ""
        await api.send_message_with_keyboard(
            user_id,
            f"Сценарий: {scenario.title}\nКод: {scenario.code}\nОписание: {scenario.description}{link_text}",
            admin_scenario_view_keyboard(scenario_id),
        )
        return

    if cb_payload.startswith("admin:scenario_delete:"):
        scenario_id = int(cb_payload.split(":")[-1])
        try:
            scenario = repo.db.get(Scenario, scenario_id)
            if scenario:
                repo.db.delete(scenario)
                repo.db.commit()
            scenarios = repo.list_scenarios()
            await api.send_message_with_keyboard(user_id, "Сценарий удалён.", admin_scenarios_keyboard(scenarios))
        except Exception as e:
            await api.send_message(user_id, f"Ошибка удаления: {e}")
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
        offers = repo.list_offers_for_platform(platform_id)
        if not offers:
            await api.send_message(user_id, "У этой платформы нет офферов с данными.")
            return
        await api.send_message_with_keyboard(
            user_id, "Выберите оффер для экспорта:", admin_export_offers_keyboard(offers, platform_id)
        )
        return

    if cb_payload.startswith("admin:export_offer:"):
        offer_id = int(cb_payload.split(":")[-1])
        offer = repo.db.get(Offer, offer_id)
        if not offer:
            await api.send_message(user_id, "Оффер не найден.")
            return
        await api.send_message(user_id, "⏳ Генерирую файл...")
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
        await api.send_message(user_id, "Рассылка — функция в разработке.")
        return

    logger.warning("Неизвестный admin callback: %r", cb_payload)


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

    user_id, text, update_type, callback_id, max_name, max_username = _extract_event(payload)
    logger.info("Webhook update_type=%r user_id=%r text=%r", update_type, user_id, text)

    api = MaxApiClient(settings.bot_token)
    try:
        if not user_id:
            return Response(status_code=200)

        repo = Repo(db)
        is_admin = user_id in settings.admin_user_ids

        # --- Callbacks ---
        if update_type == "message_callback":
            if text.startswith("user:"):
                await _handle_user_callback(api, repo, user_id, text, callback_id, max_name, max_username, settings)
            elif is_admin and text.startswith("admin:"):
                await _handle_admin_callback(api, repo, user_id, text, callback_id)
            else:
                if callback_id:
                    await api.answer_callback(callback_id)
            return Response(status_code=200)

        # --- Текстовые сообщения ---
        if update_type not in ("message_created", "bot_started", ""):
            return Response(status_code=200)

        # FSM: подписчик
        if update_type == "message_created":
            handled = await _handle_user_fsm_text(api, repo, user_id, text, settings)
            if handled:
                return Response(status_code=200)

        # FSM: admin-ввод
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

        # bot_started: text = значение ?start= из deep link
        # message_created: поддерживаем /start <code> как fallback для тестов
        scenario_code = ""
        if update_type == "bot_started":
            scenario_code = text  # payload от ?start=
        elif text.startswith("/start"):
            parts = text.split(maxsplit=1)
            scenario_code = parts[1] if len(parts) > 1 else ""

        if scenario_code:
            scenario = repo.get_scenario_by_code(scenario_code)
            if not scenario:
                await api.send_message(user_id, "Сценарий не найден. Используйте корректную ссылку.")
                return Response(status_code=200)

            fsm.set_state(user_id, "user:scenario_started", {
                "scenario_code": scenario_code,
                "max_name": max_name,
                "max_username": max_username,
            })
            msg = scenario.description or scenario.title
            if scenario.image_url:
                msg = f"{scenario.image_url}\n\n{msg}"
            await api.send_message_with_keyboard(user_id, msg, user_start_keyboard(scenario_code))
            return Response(status_code=200)

        return Response(status_code=200)

    except RateLimitError as exc:
        logger.error("Rate limit exhausted: %s", exc)
        try:
            await api.client.post(
                "/messages",
                params={"user_id": user_id} if user_id > 0 else {"chat_id": user_id},
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
