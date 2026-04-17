from pathlib import Path

from openpyxl import Workbook
from sqlalchemy.orm import Session

from app.db.repo import Repo


class ExportService:
    def __init__(self, db: Session):
        self.repo = Repo(db)

    def export_leads_xlsx(self, platform_id: int, offer_id: int, output_dir: str = "/tmp") -> Path:
        leads = self.repo.list_leads_for_export(platform_id=platform_id, offer_id=offer_id)
        wb = Workbook()
        ws = wb.active
        ws.title = "Leads"
        ws.append(
            [
                "Партнерская платформа",
                "Название карты (оффера)",
                "Дата заведения оффера",
                "SUBID",
                "Дата получения ссылки",
                "ФИО",
                "Телефон",
            ]
        )
        for lead in leads:
            ws.append(
                [
                    lead.offer.platform.name,
                    lead.offer.name,
                    str(lead.offer.created_date),
                    lead.subid_value,
                    lead.issued_at.isoformat(sep=" ", timespec="seconds"),
                    lead.full_name,
                    lead.phone,
                ]
            )

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        export_path = Path(output_dir) / f"export_platform_{platform_id}_offer_{offer_id}.xlsx"
        wb.save(export_path)
        return export_path
