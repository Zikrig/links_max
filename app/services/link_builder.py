from app.db.models import Offer


def build_offer_link(offer: Offer, subid_value: str) -> str:
    return f"{offer.link_prefix}{offer.subid_static_part}{subid_value}{offer.link_suffix}"
