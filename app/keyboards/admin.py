def _btn(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def admin_main_keyboard() -> list:
    return [
        [_btn("📋 Платформы", "admin:platforms")],
        [_btn("🎯 Офферы", "admin:offers")],
        [_btn("📝 Сценарии", "admin:scenarios")],
        [_btn("🔗 Ссылки на бот", "admin:bot_links")],
        [_btn("📢 Каналы подписки", "admin:channels")],
        [_btn("📊 Экспорт", "admin:export")],
        [_btn("📣 Рассылка", "admin:broadcast")],
    ]


def admin_platforms_keyboard(platforms: list) -> list:
    rows = [[_btn(f"❌ {p.name}", f"admin:platform_delete:{p.id}")] for p in platforms]
    rows.append([_btn("➕ Добавить платформу", "admin:platform_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_offers_keyboard(offers: list) -> list:
    rows = [[_btn(f"❌ {o.name}", f"admin:offer_delete:{o.id}")] for o in offers]
    rows.append([_btn("➕ Добавить оффер", "admin:offer_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_scenarios_keyboard(scenarios: list) -> list:
    rows = [[_btn(f"{s.code}: {s.title}", f"admin:scenario_view:{s.id}")] for s in scenarios]
    rows.append([_btn("➕ Добавить сценарий", "admin:scenario_add")])
    rows.append([_btn("🔙 Назад", "admin:main")])
    return rows


def admin_bot_links_keyboard() -> list:
    return [
        [_btn("➕ Добавить ссылку", "admin:bot_link_add")],
        [_btn("📋 Список ссылок", "admin:bot_link_list")],
        [_btn("🔙 Назад", "admin:main")],
    ]


def admin_channels_keyboard(channels: list) -> list:
    rows = [[_btn(f"❌ {c.title}", f"admin:channel_delete:{c.id}")] for c in channels]
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


def build_keyboard_attachment(buttons: list) -> dict:
    return {
        "type": "inline_keyboard",
        "payload": {"buttons": buttons},
    }
