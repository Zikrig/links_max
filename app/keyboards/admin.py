PAGE_SIZE = 6


def _btn(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def _btn_link(text: str, url: str) -> dict:
    return {"type": "link", "text": text, "url": url}


def admin_input_nav_keyboard(back_payload: str, menu_payload: str = "admin:main") -> list:
    """«Назад» на предыдущий шаг/экран и «Главное меню» (если отличается)."""
    if back_payload == menu_payload:
        return [[_btn("🔙 Назад", back_payload)]]
    return [
        [_btn("🔙 Назад", back_payload)],
        [_btn("🏠 Главное меню", menu_payload)],
    ]


def _nav_row(page: int, total: int, prev_payload: str, next_payload: str) -> list | None:
    """Ряд кнопок «◀ Назад» / «▶ Далее» для пагинации, или None если не нужен."""
    nav = []
    if page > 0:
        nav.append(_btn(f"◀ {page}", prev_payload))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(_btn(f"{page + 2} ▶", next_payload))
    return nav if nav else None


def admin_main_keyboard(*, include_moderators: bool = False) -> list:
    rows = [
        [_btn("📋 Платформы", "admin:platforms")],
        [_btn("🎯 Офферы", "admin:offers")],
        [_btn("📢 Каналы подписки", "admin:channels")],
        [_btn("📊 Экспорт", "admin:export")],
        [_btn("📣 Рассылка", "admin:broadcast")],
        [_btn("💬 Управление репликами", "admin:replicas")],
    ]
    if include_moderators:
        rows.append([_btn("👥 Модераторы", "admin:moderators")])
    return rows


def admin_moderators_keyboard(moderator_ids: list[int]) -> list:
    """Список модераторов с удалением; добавление и выход."""
    rows: list = []
    for uid in moderator_ids:
        rows.append(
            [
                _btn(f"🗑 {uid}", f"admin:moderator_remove:{uid}"),
            ]
        )
    rows.append([_btn("➕ Добавить модератора", "admin:moderator_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_replicas_menu_keyboard() -> list:
    return [
        [_btn("👤 Для незнакомцев", "admin:replica_edit:stranger")],
        [_btn("⏱ После акции (через 5 мин)", "admin:replica_edit:after")],
        [_btn("📄 Ссылка на правила (согласие)", "admin:replica_edit:policy")],
        [_btn("🔙 Назад", "admin:main")],
    ]


def admin_replica_input_keyboard() -> list:
    """Ввод текста/URL в репликах — назад к списку реплик или в главное меню."""
    return admin_input_nav_keyboard("admin:replicas", "admin:main")


def admin_platforms_keyboard(platforms: list, page: int = 0) -> list:
    total = len(platforms)
    items = platforms[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = [[_btn(p.name, f"admin:platform_view:{p.id}")] for p in items]
    nav = _nav_row(page, total, f"admin:platforms:{page - 1}", f"admin:platforms:{page + 1}")
    if nav:
        rows.append(nav)
    rows.append([_btn("➕ Добавить платформу", "admin:platform_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_platform_view_keyboard(platform_id: int) -> list:
    return [
        [_btn("📋 Офферы платформы", f"admin:platform_offers:{platform_id}")],
        [_btn("🗑 Удалить платформу", f"admin:platform_delete:{platform_id}")],
        [_btn("🔙 Назад", "admin:platforms")],
    ]


def admin_offers_root_keyboard(platforms: list, page: int = 0) -> list:
    """Первый экран «Офферы» — выбор платформы."""
    total = len(platforms)
    items = platforms[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = [[_btn(p.name, f"admin:offers_by_platform:{p.id}")] for p in items]
    nav = _nav_row(page, total, f"admin:offers_root:{page - 1}", f"admin:offers_root:{page + 1}")
    if nav:
        rows.append(nav)
    rows.append([_btn("➕ Добавить оффер", "admin:offer_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_offers_keyboard(
    offers: list,
    back_payload: str = "admin:main",
    platform_id: int | None = None,
    page: int = 0,
    *,
    from_offers_menu: bool = False,
) -> list:
    total = len(offers)
    items = offers[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = []
    for o in items:
        ov = f"admin:offer_view:{o.id}:from_offers" if from_offers_menu else f"admin:offer_view:{o.id}"
        rows.append([_btn(o.name, ov)])
    if platform_id:
        if from_offers_menu:
            prev_p = f"admin:offers_by_platform:{platform_id}:{page - 1}"
            next_p = f"admin:offers_by_platform:{platform_id}:{page + 1}"
        else:
            prev_p = f"admin:platform_offers:{platform_id}:{page - 1}"
            next_p = f"admin:platform_offers:{platform_id}:{page + 1}"
    else:
        prev_p = f"admin:offers:{page - 1}"
        next_p = f"admin:offers:{page + 1}"
    nav = _nav_row(page, total, prev_p, next_p)
    if nav:
        rows.append(nav)
    if platform_id:
        add_payload = (
            f"admin:offer_add:{platform_id}:1"
            if from_offers_menu
            else f"admin:offer_add:{platform_id}:0"
        )
    else:
        add_payload = "admin:offer_add"
    rows.append([_btn("➕ Добавить оффер", add_payload)])
    rows.append([_btn("🔙 Назад", back_payload)])
    return rows


def _ind(has_value: bool, optional: bool = True) -> str:
    if has_value:
        return "🟢"
    return "🟡" if optional else "🔴"


def admin_offer_view_keyboard(
    offer,
    scenario,
    has_bot_link: bool = False,
    *,
    offer_list_back_payload: str | None = None,
    created_date_label: str | None = None,
    from_offers_menu: bool = False,
) -> list:
    link_icon = _ind(bool(offer.base_url and offer.subid_param), optional=False)
    sc_icon = _ind(scenario is not None, optional=False)
    bl_icon = _ind(has_bot_link, optional=False)
    back = offer_list_back_payload or f"admin:platform_offers:{offer.platform_id}"
    suf = ":from_offers" if from_offers_menu else ""
    rows = [
        [_btn(f"{link_icon} Ссылка", f"admin:offer_link:{offer.id}{suf}")],
        [_btn(f"{sc_icon} Сценарий", f"admin:offer_scenario:{offer.id}{suf}")],
        [_btn(f"{bl_icon} Ссылка на бот", f"admin:offer_botlink:{offer.id}{suf}")],
        [_btn("🗑 Удалить оффер", f"admin:offer_delete:{offer.id}{suf}")],
    ]
    if created_date_label:
        rows.append([_btn(f"📅 Заведён {created_date_label}", "admin:noop")])
    rows.append([_btn("🔙 Назад", back)])
    return rows


def admin_scenario_settings_keyboard(
    scenario, sub_channel_count: int = 0, back_payload: str | None = None
) -> list:
    img_icon = _ind(bool(scenario.image_url))
    txt_icon = _ind(bool(scenario.description))

    ch_count = sub_channel_count
    sub_on = getattr(scenario, "check_subscription", False)
    sub_icon = "🟢" if sub_on else "🔴"
    sub_label = "ВКЛ" if sub_on else "ВЫКЛ"

    if sub_on:
        ch_icon = "🟢" if ch_count > 0 else "🔴"
    else:
        ch_icon = "🟡"

    back = back_payload or f"admin:offer_view:{scenario.offer_id}"
    back_label = "🔙 Назад к сценариям" if back_payload == "admin:scenarios" else "🔙 Назад к офферу"

    return [
        [_btn(f"{img_icon} Картинка", f"admin:scenario_image_menu:{scenario.id}")],
        [_btn(f"{txt_icon} Текст для подписчика", f"admin:scenario_replace_text:{scenario.id}")],
        [_btn(f"{sub_icon} Проверка подписки: {sub_label}", f"admin:scenario_toggle_sub:{scenario.id}")],
        [_btn(f"{ch_icon} Каналы ({ch_count})", f"admin:scenario_channels:{scenario.id}")],
        [_btn("🗑 Удалить сценарий", f"admin:scenario_delete:{scenario.id}")],
        [_btn(back_label, back)],
    ]


def admin_scenario_image_menu_keyboard(scenario_id: int, has_image: bool) -> list:
    rows = [
        [_btn("🔄 Заменить картинку", f"admin:scenario_replace_image:{scenario_id}")],
    ]
    if has_image:
        rows.append([_btn("🗑 Убрать картинку", f"admin:scenario_skip_image:{scenario_id}")])
    rows.append([_btn("🔙 Назад", f"admin:offer_scenario_view:{scenario_id}")])
    return rows


def admin_scenario_text_menu_keyboard(scenario_id: int) -> list:
    return [
        [_btn("🔄 Заменить текст", f"admin:scenario_replace_text:{scenario_id}")],
        [_btn("🔙 Назад", f"admin:offer_scenario_view:{scenario_id}")],
    ]


def admin_scenario_subscription_keyboard(
    scenario_id: int, global_channels: list, enabled_ids: set
) -> list:
    """Включение глобальных каналов для сценария (заведение каналов — в главном меню)."""
    rows = []
    if not global_channels:
        rows.append([_btn("📢 Завести каналы (главное меню → Каналы подписки)", "admin:channels")])
    for ch in global_channels:
        on = ch.id in enabled_ids
        icon = "✅" if on else "⬜"
        label = ch.title if len(ch.title) <= 34 else ch.title[:31] + "…"
        rows.append([_btn(f"{icon} {label}", f"admin:scenario_sub_ch_toggle:{scenario_id}:{ch.id}")])
    rows.append([_btn("🔙 Назад к сценарию", f"admin:offer_scenario_view:{scenario_id}")])
    return rows


def admin_scenarios_keyboard(scenarios: list, page: int = 0) -> list:
    total = len(scenarios)
    items = scenarios[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = [[_btn(f"{s.code}: {s.title}", f"admin:scenario_view:{s.id}")] for s in items]
    nav = _nav_row(page, total, f"admin:scenarios:{page - 1}", f"admin:scenarios:{page + 1}")
    if nav:
        rows.append(nav)
    rows.append([_btn("➕ Добавить сценарий", "admin:scenario_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_scenario_select_offer_keyboard(offers: list) -> list:
    rows = [[_btn(o.name, f"admin:scenario_select_offer:{o.id}")] for o in offers]
    rows.extend(admin_input_nav_keyboard("admin:scenarios", "admin:main"))
    return rows


def admin_scenario_view_keyboard(scenario_id: int) -> list:
    return [
        [_btn("🗑 Удалить сценарий", f"admin:scenario_delete:{scenario_id}")],
        [_btn("🔙 Назад", "admin:scenarios")],
    ]


def admin_bot_links_keyboard() -> list:
    return [
        [_btn("➕ Добавить ссылку", "admin:bot_link_add")],
        [_btn("📋 Список ссылок", "admin:bot_link_list")],
        [_btn("🔙 Назад", "admin:main")],
    ]


def admin_channels_keyboard(channels: list, page: int = 0) -> list:
    total = len(channels)
    items = channels[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = [[_btn(f"❌ {c.title}", f"admin:channel_delete:{c.id}")] for c in items]
    nav = _nav_row(page, total, f"admin:channels:{page - 1}", f"admin:channels:{page + 1}")
    if nav:
        rows.append(nav)
    rows.append([_btn("➕ Добавить канал", "admin:channel_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_offer_select_platform_keyboard(platforms: list) -> list:
    rows = [[_btn(p.name, f"admin:offer_select_platform:{p.id}")] for p in platforms]
    rows.extend(admin_input_nav_keyboard("admin:offers", "admin:main"))
    return rows


def admin_export_platforms_keyboard(platforms: list) -> list:
    rows = [[_btn(p.name, f"admin:export_platform:{p.id}")] for p in platforms]
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_export_offers_keyboard(offers: list, platform_id: int) -> list:
    rows = [[_btn(o.name, f"admin:export_offer:{o.id}")] for o in offers]
    rows.append([_btn("🔙 Назад", "admin:export")])
    return rows


def admin_confirm_delete_keyboard(confirm_payload: str, cancel_payload: str) -> list:
    return [
        [_btn("✅ Да", confirm_payload)],
        [_btn("🔙 Отмена", cancel_payload)],
    ]


def build_keyboard_attachment(buttons: list) -> dict:
    return {
        "type": "inline_keyboard",
        "payload": {"buttons": buttons},
    }


BROADCAST_MANAGE_PAGE_SIZE = 5


def admin_broadcast_manage_keyboard(page: int, total_count: int, items: list) -> list:
    """Список рассылок с пагинацией. Неотправленные — отмена в строке; отправленные — быстрый повтор."""
    rows: list = []
    for b in items:
        title = (getattr(b, "title", None) or "без названия").replace("\n", " ").strip()
        if len(title) > 30:
            title = title[:27] + "…"
        main = _btn(f"#{b.id} «{title}»", f"admin:broadcast_view:{b.id}")
        st = getattr(b, "status", "") or ""
        if st == "scheduled":
            rows.append([main, _btn("🚫", f"admin:broadcast_cancel_pending:{b.id}")])
        elif st in ("sent", "failed"):
            rows.append([main, _btn("📋", f"admin:broadcast_repeat:{b.id}")])
        else:
            rows.append([main])
    if total_count == 0:
        rows.append([_btn("🔙 Назад", "admin:broadcast")])
        return rows
    total_pages = max(1, (total_count + BROADCAST_MANAGE_PAGE_SIZE - 1) // BROADCAST_MANAGE_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    nav = []
    if page > 0:
        nav.append(_btn("◀", f"admin:broadcast_manage:{page - 1}"))
    if (page + 1) * BROADCAST_MANAGE_PAGE_SIZE < total_count:
        nav.append(_btn("▶", f"admin:broadcast_manage:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([_btn("🔙 Назад", "admin:broadcast")])
    return rows


def admin_broadcast_manage_cancel_keyboard() -> list:
    """Ввод времени переноса — назад к карточке рассылки или в главное меню."""
    return admin_input_nav_keyboard("admin:wizard_back", "admin:main")


def admin_broadcast_detail_keyboard(broadcast_id: int, status: str) -> list:
    """Карточка рассылки: для scheduled доступны отправка, перенос и отмена."""
    rows: list = []
    if status == "scheduled":
        rows.append([_btn("▶ Отправить сейчас", f"admin:broadcast_now:{broadcast_id}")])
        rows.append([_btn("🖼 Изменить картинку", f"admin:broadcast_edit_image:{broadcast_id}")])
        rows.append([_btn("📝 Изменить текст", f"admin:broadcast_edit_text:{broadcast_id}")])
        rows.append([_btn("🔗 Изменить кнопку/ссылку", f"admin:broadcast_edit_button:{broadcast_id}")])
        rows.append([_btn("📅 Изменить время", f"admin:broadcast_reschedule:{broadcast_id}")])
        rows.append([_btn("🚫 Отменить", f"admin:broadcast_cancel_pending:{broadcast_id}")])
    elif status in ("sent", "failed"):
        rows.append([_btn("📋 Повторить (копия)", f"admin:broadcast_repeat:{broadcast_id}")])
    rows.append([_btn("🔙 К списку", "admin:broadcast_manage:0")])
    return rows


def admin_broadcast_entry_keyboard() -> list:
    return [
        [_btn("✉️ Создать рассылку", "admin:broadcast_new")],
        [_btn("📬 Управление рассылками", "admin:broadcast_manage:0")],
        [_btn("🔙 Назад", "admin:main")],
    ]


def admin_broadcast_skip_image_keyboard() -> list:
    rows = [[_btn("⏭ Без картинки", "admin:broadcast_skip_image")]]
    rows.extend(admin_input_nav_keyboard("admin:wizard_back", "admin:main"))
    return rows


def admin_broadcast_skip_text_keyboard() -> list:
    rows = [[_btn("⏭ Без текста", "admin:broadcast_skip_text")]]
    rows.extend(admin_input_nav_keyboard("admin:wizard_back", "admin:main"))
    return rows


def admin_broadcast_default_button_keyboard(default_label: str = "Перейти к акции") -> list:
    """Кнопка показывает текст по умолчанию, чтобы админ видел, что будет подставлено."""
    rows = [[_btn(f"📌 {default_label}", "admin:broadcast_default_btn")]]
    rows.extend(admin_input_nav_keyboard("admin:wizard_back", "admin:main"))
    return rows


def admin_broadcast_preview_keyboard() -> list:
    rows = [
        [_btn("▶ Отправить сейчас", "admin:broadcast_send_now")],
        [_btn("📅 Отправить позже", "admin:broadcast_send_later")],
    ]
    rows.extend(admin_input_nav_keyboard("admin:wizard_back", "admin:main"))
    return rows


def admin_broadcast_schedule_cancel_keyboard() -> list:
    return admin_input_nav_keyboard("admin:wizard_back", "admin:main")
