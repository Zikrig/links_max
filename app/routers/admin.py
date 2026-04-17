from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.database import get_db
from app.db.repo import Repo
from app.keyboards.admin import admin_main_keyboard
from app.services.admin_service import AdminService
from app.services.export_service import ExportService

router = APIRouter(prefix="/admin", tags=["admin"])


class AdminAction(BaseModel):
    user_id: int
    command: str
    payload: dict = {}


def _require_admin(user_id: int, settings: Settings) -> None:
    if user_id not in settings.admin_user_ids:
        raise HTTPException(status_code=403, detail="Недостаточно прав")


@router.post("/command")
def handle_admin_command(
    action: AdminAction,
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    _require_admin(action.user_id, settings)
    repo = Repo(db)
    service = AdminService(repo)

    if action.command == "admin":
        return {"text": "Админ-меню", "keyboard": admin_main_keyboard()}
    if action.command == "platform_add":
        platform = service.add_platform(action.payload["name"])
        return {"text": f"Платформа добавлена: {platform.name}"}
    if action.command == "platform_delete":
        service.remove_platform(int(action.payload["platform_id"]))
        return {"text": "Платформа удалена"}
    if action.command == "offer_add":
        offer = service.add_offer(
            platform_id=int(action.payload["platform_id"]),
            name=action.payload["name"],
            prefix=action.payload["prefix"],
            static_subid=action.payload["static_subid"],
            suffix=action.payload["suffix"],
        )
        return {"text": f"Оффер добавлен: {offer.name}"}
    if action.command == "offer_delete":
        service.remove_offer(int(action.payload["offer_id"]))
        return {"text": "Оффер удален"}
    if action.command == "scenario_add":
        scenario = service.add_scenario(
            offer_id=int(action.payload["offer_id"]),
            code=action.payload["code"],
            title=action.payload["title"],
            description=action.payload["description"],
            image_url=action.payload.get("image_url"),
        )
        return {"text": f"Сценарий создан: {scenario.code}"}
    if action.command == "scenario_list":
        items = repo.list_scenarios()
        return {"items": [{"id": s.id, "code": s.code, "title": s.title} for s in items]}
    if action.command == "bot_link_add":
        item = repo.create_or_update_bot_link(
            scenario_id=int(action.payload["scenario_id"]),
            deep_link=action.payload["deep_link"],
        )
        return {"text": f"Ссылка сохранена: {item.deep_link}"}
    if action.command == "bot_link_list":
        links = repo.list_bot_links()
        return {"items": [{"id": x.id, "deep_link": x.deep_link, "scenario_id": x.scenario_id} for x in links]}
    if action.command == "bot_link_delete":
        repo.delete_bot_link(int(action.payload["link_id"]))
        return {"text": "Ссылка удалена"}
    if action.command == "required_channel_add":
        item = service.add_required_channel(
            title=action.payload["title"],
            chat_id=int(action.payload["chat_id"]),
            invite_link=action.payload.get("invite_link"),
        )
        return {"text": f"Канал добавлен: {item.title}"}
    if action.command == "required_channel_delete":
        repo.delete_required_channel(int(action.payload["channel_id"]))
        return {"text": "Канал удален"}
    if action.command == "export":
        exporter = ExportService(db)
        path = exporter.export_leads_xlsx(
            platform_id=int(action.payload["platform_id"]),
            offer_id=int(action.payload["offer_id"]),
            output_dir=action.payload.get("output_dir", "/tmp"),
        )
        return {"text": "Экспорт готов", "file_path": str(path)}

    raise HTTPException(status_code=400, detail=f"Неизвестная admin-команда: {action.command}")
