import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.callback_ack import build_safe_callback_ack
from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.repo import Repo

router = APIRouter(tags=["webhook"])
logger = logging.getLogger(__name__)


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/max/webhook/{suffix:path}")
def webhook_info():
    return {
        "ok": True,
        "webhook": True,
        "detail": "MAX отправляет события POST-запросом на этот endpoint.",
    }


def _extract_sender_and_text(payload: dict) -> tuple[int, str]:
    update_type = payload.get("update_type")
    if update_type == "message_created":
        msg = payload.get("message", {}) or {}
        sender = msg.get("sender", {}) or {}
        body = msg.get("body", {}) or {}
        return int(sender.get("user_id") or 0), str(body.get("text", "")).strip()
    if update_type == "message_callback":
        callback = payload.get("callback", {}) or {}
        user = callback.get("user", {}) or {}
        cb_payload = callback.get("payload", "")
        return int(user.get("user_id") or 0), str(cb_payload).strip()
    # fallback for simplified mock payload
    return int(payload.get("user_id", 0)), str(payload.get("text", "")).strip()


@router.post("/max/webhook/{suffix:path}")
async def handle_max_webhook(
    request: Request,
    x_max_bot_api_secret: str | None = Header(default=None),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    if settings.webhook_secret and x_max_bot_api_secret != settings.webhook_secret:
        logger.warning("Webhook secret mismatch")
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("Bad webhook JSON: %s", exc)
        return Response(status_code=400)
    if not isinstance(payload, dict):
        return Response(status_code=400)

    update_type = payload.get("update_type")
    logger.info("Webhook POST update_type=%r", update_type)
    user_id, text = _extract_sender_and_text(payload)
    callback = update_type == "message_callback" or bool(payload.get("callback"))

    if callback:
        # Безопасный ответ на callback для maxapi, чтобы не откатывались attachments.
        return {"type": "callback_ack", "ack": build_safe_callback_ack()}

    if text == "admin" and user_id in settings.admin_user_ids:
        return {"route": "admin", "command": "admin", "user_id": user_id}

    # Deeplink: /start scenario7
    if text.startswith("/start"):
        parts = text.split(maxsplit=1)
        scenario_code = parts[1] if len(parts) > 1 else ""
        return {"route": "user", "command": "start", "user_id": user_id, "payload": {"scenario_code": scenario_code}}

    # Fallback for user conversation steps.
    repo = Repo(db)
    if repo.list_required_channels():
        return {"route": "user", "command": "next", "user_id": user_id}
    return {"route": "user", "command": "unknown", "user_id": user_id}
