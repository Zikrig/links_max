from app.db.models import Offer


def build_offer_link(offer: Offer, subid_value: str) -> str:
    base = offer.base_url.strip().rstrip("?&")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{offer.subid_param}={subid_value}"
