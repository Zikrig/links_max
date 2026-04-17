from datetime import datetime
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session

from app.db.repo import Repo


class BroadcastService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def schedule(
        self,
        db: Session,
        title: str,
        text: str,
        button_url: str,
        sender: Callable[[str, str, str], None],
        send_at: datetime | None = None,
    ) -> int:
        repo = Repo(db)
        broadcast = repo.create_broadcast(
            title=title,
            text=text,
            button_url=button_url,
            send_at=send_at,
        )
        if send_at is None or send_at <= datetime.utcnow():
            sender(title, text, button_url)
            repo.mark_broadcast_sent(broadcast.id)
        else:
            self.scheduler.add_job(
                sender,
                "date",
                run_date=send_at,
                args=[title, text, button_url],
                id=f"broadcast_{broadcast.id}",
            )
        return broadcast.id
