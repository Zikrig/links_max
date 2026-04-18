from app.db.models import Offer


def build_offer_link(offer: Offer, subid_value: str) -> str:
    base = offer.base_url.strip().rstrip("?&")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{offer.subid_param}={subid_value}"


def is_valid_offer_link(url: str) -> bool:
    """MAX API отклоняет кнопку link без абсолютного URL (например «?sub=1» при пустом base_url)."""
    u = (url or "").strip()
    return len(u) >= 10 and (u.startswith("https://") or u.startswith("http://"))


def offer_produces_valid_links(offer: Offer) -> bool:
    """Проверка до расхода SUBID: ссылка с тестовым subid должна быть абсолютной."""
    return is_valid_offer_link(build_offer_link(offer, "0001"))
