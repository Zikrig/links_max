from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.repo import Repo
from app.services.user_flow import UserFlowService

router = APIRouter(prefix="/user", tags=["user"])


class UserAction(BaseModel):
    user_id: int
    command: str
    payload: dict = {}


def _check_required_subscription(repo: Repo, user_id: int) -> tuple[bool, list[dict]]:
    # Заглушка проверки членства: сюда нужно подключить maxapi get_chat_member/get_chat_members.
    channels = repo.list_required_channels()
    if not channels:
        return True, []
    not_joined: list[dict] = []
    for channel in channels:
        # В этой реализации считаем, что нужно отдельное API-подтверждение.
        not_joined.append({"title": channel.title, "invite_link": channel.invite_link})
    return False, not_joined


@router.post("/command")
def handle_user_command(
    action: UserAction,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    repo = Repo(db)
    flow = UserFlowService(repo=repo, settings=settings)

    if action.command == "start":
        scenario_code = action.payload.get("scenario_code")
        if not scenario_code:
            raise HTTPException(status_code=400, detail="Нужен scenario_code")
        scenario = flow.start_scenario(scenario_code=scenario_code)
        return {
            "text": scenario.description,
            "image_url": scenario.image_url,
            "next_button": "Далее",
            "scenario_code": scenario.code,
        }

    if action.command == "next":
        ok, channels = _check_required_subscription(repo=repo, user_id=action.user_id)
        if not ok:
            return {
                "text": "Подпишитесь на каналы перед продолжением",
                "channels": channels,
            }
        return {
            "text": "Введите ФИО, затем телефон и подтвердите согласие",
            "policy_url": flow.policy_url(),
        }

    if action.command == "submit_profile":
        scenario_code = action.payload["scenario_code"]
        full_name = action.payload["full_name"]
        phone = action.payload["phone"]
        link = flow.issue_personal_link(
            user_id=action.user_id,
            scenario_code=scenario_code,
            full_name=full_name,
            phone=phone,
        )
        return {
            "text": "Для оформления карты на сайте банка перейдите по ссылке ниже.",
            "button": {"text": "Оформить карту", "url": link},
        }

    raise HTTPException(status_code=400, detail=f"Неизвестная user-команда: {action.command}")
