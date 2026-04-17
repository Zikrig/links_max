import re


def validate_phone(value: str) -> bool:
    normalized = re.sub(r"\D", "", value)
    return len(normalized) >= 10


def validate_full_name(value: str) -> bool:
    parts = [p for p in value.strip().split(" ") if p]
    return len(parts) >= 2 and all(len(p) > 1 for p in parts)
