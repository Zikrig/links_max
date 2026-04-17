from datetime import datetime

from app.db.repo import Repo


class AdminService:
    def __init__(self, repo: Repo):
        self.repo = repo

    def is_admin(self, user_id: int, admin_ids: set[int]) -> bool:
        return user_id in admin_ids

    def add_platform(self, name: str):
        return self.repo.create_platform(name=name)

    def remove_platform(self, platform_id: int):
        self.repo.delete_platform(platform_id=platform_id)

    def add_offer(self, platform_id: int, name: str, prefix: str, static_subid: str, suffix: str):
        return self.repo.create_offer(
            platform_id=platform_id,
            name=name,
            link_prefix=prefix,
            subid_static_part=static_subid,
            link_suffix=suffix,
        )

    def remove_offer(self, offer_id: int):
        self.repo.delete_offer(offer_id=offer_id)

    def add_scenario(self, offer_id: int, code: str, title: str, description: str, image_url: str | None):
        return self.repo.create_scenario(
            offer_id=offer_id,
            code=code,
            title=title,
            description=description,
            image_url=image_url,
        )

    def add_required_channel(self, title: str, chat_id: int, invite_link: str | None):
        return self.repo.add_required_channel(title=title, chat_id=chat_id, invite_link=invite_link)

    def create_broadcast(
        self,
        title: str,
        text: str,
        button_url: str,
        button_text: str = "Перейти к акции",
        image_url: str | None = None,
        send_at: datetime | None = None,
    ):
        return self.repo.create_broadcast(
            title=title,
            text=text,
            button_text=button_text,
            button_url=button_url,
            image_url=image_url,
            send_at=send_at,
        )
