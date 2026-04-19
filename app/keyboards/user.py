def _btn_callback(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload, "intent": "default"}


def _btn_link(text: str, url: str) -> dict:
    return {"type": "link", "text": text, "url": url}


def user_material_keyboard(scenario_code: str, ref_url: str | None) -> list:
    """Под описанием акции — только «Далее»; реф. ссылка выдаётся после ФИО, телефона и согласия (ТЗ)."""
    if ref_url:
        return [[_btn_link("Оформить карту", ref_url)]]
    return [[_btn_callback("Далее ➡️", f"user:next:{scenario_code}")]]


def user_subscribe_keyboard(channels: list, scenario_code: str) -> list:
    """Каналы для подписки + кнопка «Я подписался»."""
    rows = []
    for ch in channels:
        if ch.invite_link:
            rows.append([_btn_link(f"📢 {ch.title}", ch.invite_link)])
        else:
            rows.append([_btn_callback(f"📢 {ch.title}", "user:noop")])
    rows.append([_btn_callback("✅ Я подписался", f"user:check_sub:{scenario_code}")])
    return rows


def user_channels_keyboard(channels: list, scenario_code: str) -> list:
    """Алиас для обратной совместимости."""
    return user_subscribe_keyboard(channels, scenario_code)


def user_consent_keyboard(scenario_code: str, policy_url: str) -> list:
    return [
        [_btn_link("📄 Ознакомиться с правилами", policy_url)],
        [_btn_callback("✅ Соглашаюсь", f"user:consent:{scenario_code}")],
    ]


def user_card_keyboard(ref_url: str) -> list:
    return [[_btn_link("Оформить карту", ref_url)]]


def user_wizard_nav_keyboard(scenario_code: str) -> list:
    """Назад на предыдущий шаг ввода (к материалу акции с первого шага)."""
    return [[_btn_callback("🔙 Назад", f"user:wizard_back:{scenario_code}")]]
