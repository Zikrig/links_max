PAGE_SIZE = 6


def _btn(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def _nav_row(page: int, total: int, prev_payload: str, next_payload: str) -> list | None:
    """Ряд кнопок «◀ Назад» / «▶ Далее» для пагинации, или None если не нужен."""
    nav = []
    if page > 0:
        nav.append(_btn(f"◀ {page}", prev_payload))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(_btn(f"{page + 2} ▶", next_payload))
    return nav if nav else None


def admin_main_keyboard() -> list:
    return [
        [_btn("📋 Платформы", "admin:platforms")],
        [_btn("🎯 Офферы", "admin:offers")],
        [_btn("📝 Сценарии", "admin:scenarios")],
        [_btn("🔗 Ссылки на бот", "admin:bot_links")],
        # [_btn("📢 Каналы подписки", "admin:channels")],
        [_btn("📊 Экспорт", "admin:export")],
        [_btn("📣 Рассылка", "admin:broadcast")],
    ]


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


def admin_offers_keyboard(
    offers: list,
    back_payload: str = "admin:main",
    platform_id: int | None = None,
    page: int = 0,
) -> list:
    total = len(offers)
    items = offers[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    rows = [[_btn(o.name, f"admin:offer_view:{o.id}")] for o in items]
    if platform_id:
        prev_p = f"admin:platform_offers:{platform_id}:{page - 1}"
        next_p = f"admin:platform_offers:{platform_id}:{page + 1}"
    else:
        prev_p = f"admin:offers:{page - 1}"
        next_p = f"admin:offers:{page + 1}"
    nav = _nav_row(page, total, prev_p, next_p)
    if nav:
        rows.append(nav)
    add_payload = f"admin:offer_add:{platform_id}" if platform_id else "admin:offer_add"
    rows.append([_btn("➕ Добавить оффер", add_payload)])
    rows.append([_btn("🔙 Назад", back_payload)])
    return rows


def _ind(has_value: bool, optional: bool = True) -> str:
    if has_value:
        return "🟢"
    return "🟡" if optional else "🔴"


def admin_offer_view_keyboard(offer, scenario, has_bot_link: bool = False) -> list:
    link_icon = _ind(bool(offer.base_url and offer.subid_param), optional=False)
    sc_icon = _ind(scenario is not None, optional=False)
    bl_icon = _ind(has_bot_link, optional=False)
    return [
        [_btn(f"{link_icon} Ссылка", f"admin:offer_link:{offer.id}")],
        [_btn(f"{sc_icon} Сценарий", f"admin:offer_scenario:{offer.id}")],
        [_btn(f"{bl_icon} Ссылка на бот", f"admin:offer_botlink:{offer.id}")],
        [_btn("🗑 Удалить оффер", f"admin:offer_delete:{offer.id}")],
        [_btn("🔙 Назад", f"admin:platform_offers:{offer.platform_id}")],
    ]


def admin_scenario_settings_keyboard(
    scenario, channels: list | None = None, back_payload: str | None = None
) -> list:
    img_icon = _ind(bool(scenario.image_url))
    txt_icon = _ind(bool(scenario.description))

    channels = channels or []
    ch_count = len(channels)
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
        [_btn(f"{img_icon} Картинка", f"admin:scenario_set_image:{scenario.id}")],
        [_btn(f"{txt_icon} Текст для подписчика", f"admin:scenario_set_text:{scenario.id}")],
        [_btn(f"{sub_icon} Проверка подписки: {sub_label}", f"admin:scenario_toggle_sub:{scenario.id}")],
        [_btn(f"{ch_icon} Каналы ({ch_count})", f"admin:scenario_channels:{scenario.id}")],
        [_btn("🗑 Удалить сценарий", f"admin:scenario_delete:{scenario.id}")],
        [_btn(back_label, back)],
    ]


def admin_scenario_channels_keyboard(scenario_id: int, channels: list) -> list:
    rows = []
    for ch in channels:
        rows.append([_btn(f"❌ {ch.title}", f"admin:scenario_ch_del:{ch.id}")])
    rows.append([_btn("➕ Добавить канал", f"admin:scenario_ch_add:{scenario_id}")])
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
    rows.append([_btn("🔙 Назад", "admin:scenarios")])
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
    rows.append([_btn("🔙 Назад", "admin:offers")])
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


def admin_broadcast_entry_keyboard() -> list:
    return [
        [_btn("✉️ Создать рассылку", "admin:broadcast_new")],
        [_btn("📋 История рассылок", "admin:broadcast_history")],
        [_btn("🔙 Назад", "admin:main")],
    ]


def admin_broadcast_skip_image_keyboard() -> list:
    return [
        [_btn("⏭ Без картинки", "admin:broadcast_skip_image")],
        [_btn("🔙 Отмена", "admin:broadcast_cancel")],
    ]


def admin_broadcast_default_button_keyboard() -> list:
    return [
        [_btn("📌 Текст по умолчанию", "admin:broadcast_default_btn")],
        [_btn("🔙 Отмена", "admin:broadcast_cancel")],
    ]


def admin_broadcast_preview_keyboard() -> list:
    return [
        [_btn("▶ Отправить сейчас", "admin:broadcast_send_now")],
        [_btn("📅 Отправить позже", "admin:broadcast_send_later")],
        [_btn("🔙 Отмена", "admin:broadcast_cancel")],
    ]


def admin_broadcast_schedule_cancel_keyboard() -> list:
    return [[_btn("🔙 Отмена", "admin:broadcast_cancel")]]
