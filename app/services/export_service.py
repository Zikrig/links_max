from pathlib import Path
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from sqlalchemy.orm import Session

from app.db.repo import Repo


class ExportService:
    def __init__(self, db: Session):
        self.repo = Repo(db)

    def export_leads_xlsx(
        self,
        platform_id: int,
        offer_id: int,
        output_dir: str = "/tmp",
        *,
        timezone_name: str = "Europe/Moscow",
    ) -> Path:
        leads = self.repo.list_leads_for_export(platform_id=platform_id, offer_id=offer_id)
        tz = ZoneInfo(timezone_name)
        tz_label = timezone_name.split("/")[-1]
        wb = Workbook()
        ws = wb.active
        ws.title = "Leads"
        ws.append(
            [
                "Партнерская платформа",
                "Название карты (оффера)",
                f"Дата заведения оффера ({tz_label})",
                "SUBID",
                f"Дата получения ссылки ({tz_label})",
                "ФИО (введено)",
                "Телефон",
                "Имя в MAX",
                "Никнейм в MAX",
                "MAX user_id",
            ]
        )
        for lead in leads:
            od = lead.offer.created_date
            offer_date_str = od.strftime("%d.%m.%Y") if od else ""
            issued = lead.issued_at
            if issued is not None:
                if issued.tzinfo is None:
                    issued = issued.replace(tzinfo=ZoneInfo("UTC"))
                issued_str = issued.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
            else:
                issued_str = ""
            ws.append(
                [
                    lead.offer.platform.name,
                    lead.offer.name,
                    offer_date_str,
                    lead.subid_value,
                    issued_str,
                    lead.full_name,
                    lead.phone,
                    lead.max_name or "",
                    f"@{lead.max_username}" if lead.max_username else "",
                    lead.user_id,
                ]
            )

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        export_path = Path(output_dir) / f"export_platform_{platform_id}_offer_{offer_id}.xlsx"
        wb.save(export_path)
        return export_path
