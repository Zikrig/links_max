from dataclasses import dataclass


@dataclass
class IncomingMessage:
    user_id: int
    text: str


def extract_start_scenario(text: str) -> str | None:
    text = text.strip()
    if not text.startswith("/start"):
        return None
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip() or None
