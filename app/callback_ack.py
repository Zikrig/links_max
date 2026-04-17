def build_safe_callback_ack() -> dict:
    # MAX-specific safe callback ack:
    # message=None and non-empty notification avoids stale attachment rollback.
    return {"message": None, "notification": " "}
