def _btn_callback(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def _btn_link(text: str, url: str) -> dict:
    return {"type": "link", "text": text, "url": url}


def user_start_keyboard(scenario_code: str) -> list:
    return [
        [_btn_callback("Далее ➡️", f"user:next:{scenario_code}")],
    ]


def user_channels_keyboard(channels: list, scenario_code: str) -> list:
    rows = [[_btn_link(f"📢 {c.title}", c.invite_link or "#")] for c in channels if c.invite_link]
    rows.append([_btn_callback("✅ Проверить подписку", f"user:check_sub:{scenario_code}")])
    return rows


def user_consent_keyboard(scenario_code: str, policy_url: str) -> list:
    return [
        [_btn_link("📄 Ознакомиться с правилами", policy_url)],
        [_btn_callback("✅ Соглашаюсь", f"user:consent:{scenario_code}")],
    ]


def user_card_keyboard(ref_url: str) -> list:
    return [
        [_btn_link("🏦 Оформить карту", ref_url)],
    ]
