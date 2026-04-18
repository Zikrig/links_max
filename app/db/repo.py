from datetime import datetime

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from app.db import models


class Repo:
    def __init__(self, db: Session):
        self.db = db

    def create_platform(self, name: str) -> models.Platform:
        entity = models.Platform(name=name)
        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)
        return entity

    def delete_platform(self, platform_id: int) -> None:
        entity = self.db.get(models.Platform, platform_id)
        if entity:
            self.db.delete(entity)
            self.db.commit()

    def list_platforms(self) -> list[models.Platform]:
        return list(self.db.scalars(select(models.Platform).order_by(models.Platform.name)))

    def create_offer(
        self,
        platform_id: int,
        name: str,
        base_url: str,
        subid_param: str,
    ) -> models.Offer:
        offer = models.Offer(
            platform_id=platform_id,
            name=name,
            base_url=base_url,
            subid_param=subid_param,
        )
        self.db.add(offer)
        self.db.commit()
        self.db.refresh(offer)
        return offer

    def delete_offer(self, offer_id: int) -> None:
        entity = self.db.get(models.Offer, offer_id)
        if entity:
            self.db.delete(entity)
            self.db.commit()

    def list_offers(self, platform_id: int | None = None) -> list[models.Offer]:
        stmt: Select[tuple[models.Offer]] = select(models.Offer).order_by(models.Offer.created_date.desc())
        if platform_id is not None:
            stmt = stmt.where(models.Offer.platform_id == platform_id)
        return list(self.db.scalars(stmt))

    def get_scenario_for_offer(self, offer_id: int) -> models.Scenario | None:
        return self.db.scalar(select(models.Scenario).where(models.Scenario.offer_id == offer_id))

    def update_scenario_field(self, scenario_id: int, **fields) -> models.Scenario | None:
        scenario = self.db.get(models.Scenario, scenario_id)
        if not scenario:
            return None
        for k, v in fields.items():
            setattr(scenario, k, v)
        self.db.commit()
        self.db.refresh(scenario)
        return scenario

    def create_scenario(self, offer_id: int, code: str, title: str, description: str | None = None, image_url: str | None = None) -> models.Scenario:
        scenario = models.Scenario(
            offer_id=offer_id,
            code=code,
            title=title,
            description=description,
            image_url=image_url,
        )
        self.db.add(scenario)
        self.db.commit()
        self.db.refresh(scenario)
        return scenario

    def add_scenario_channel(
        self, scenario_id: int, chat_id: int, title: str, invite_link: str | None = None
    ) -> models.ScenarioChannel:
        ch = models.ScenarioChannel(
            scenario_id=scenario_id, chat_id=chat_id, title=title, invite_link=invite_link
        )
        self.db.add(ch)
        self.db.commit()
        self.db.refresh(ch)
        return ch

    def list_scenario_channels(self, scenario_id: int) -> list[models.ScenarioChannel]:
        return list(self.db.scalars(
            select(models.ScenarioChannel).where(models.ScenarioChannel.scenario_id == scenario_id)
        ))

    def delete_scenario_channel(self, channel_id: int) -> None:
        entity = self.db.get(models.ScenarioChannel, channel_id)
        if entity:
            self.db.delete(entity)
            self.db.commit()

    def list_scenarios(self) -> list[models.Scenario]:
        return list(self.db.scalars(select(models.Scenario).order_by(models.Scenario.created_at.desc())))

    def get_scenario_by_code(self, code: str) -> models.Scenario | None:
        return self.db.scalar(select(models.Scenario).where(models.Scenario.code == code))

    def create_or_update_bot_link(self, scenario_id: int, deep_link: str) -> models.BotLink:
        entity = self.db.scalar(select(models.BotLink).where(models.BotLink.scenario_id == scenario_id))
        if entity:
            entity.deep_link = deep_link
        else:
            entity = models.BotLink(scenario_id=scenario_id, deep_link=deep_link)
            self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)
        return entity

    def get_bot_link_for_scenario(self, scenario_id: int) -> models.BotLink | None:
        return self.db.scalar(select(models.BotLink).where(models.BotLink.scenario_id == scenario_id))

    def list_bot_links(self) -> list[models.BotLink]:
        return list(self.db.scalars(select(models.BotLink).order_by(models.BotLink.created_at.desc())))

    def delete_bot_link(self, link_id: int) -> None:
        entity = self.db.get(models.BotLink, link_id)
        if entity:
            self.db.delete(entity)
            self.db.commit()

    def next_subid(self, offer_id: int) -> str:
        offer = self.db.get(models.Offer, offer_id)
        if not offer:
            raise ValueError("Offer not found")
        if offer.next_subid > 9999:
            raise ValueError("SUBID limit reached for this offer")
        current = offer.next_subid
        offer.next_subid += 1
        self.db.commit()
        return f"{current:04d}"

    def list_offers_for_platform(self, platform_id: int) -> list[models.Offer]:
        return list(self.db.scalars(
            select(models.Offer)
            .where(models.Offer.platform_id == platform_id)
            .order_by(models.Offer.created_date.desc())
        ))

    def create_lead(
        self,
        user_id: int,
        scenario_id: int,
        offer_id: int,
        subid_value: str,
        full_name: str = "",
        phone: str = "",
        consent_accepted: bool = True,
        max_name: str | None = None,
        max_username: str | None = None,
    ) -> models.Lead:
        lead = models.Lead(
            user_id=user_id,
            scenario_id=scenario_id,
            offer_id=offer_id,
            full_name=full_name,
            phone=phone,
            subid_value=subid_value,
            consent_accepted=consent_accepted,
            max_name=max_name,
            max_username=max_username,
        )
        self.db.add(lead)
        self.db.commit()
        self.db.refresh(lead)
        return lead

    def list_leads_for_export(self, platform_id: int, offer_id: int) -> list[models.Lead]:
        stmt = (
            select(models.Lead)
            .join(models.Offer, models.Lead.offer_id == models.Offer.id)
            .where(models.Offer.platform_id == platform_id, models.Offer.id == offer_id)
            .order_by(models.Lead.issued_at.desc())
        )
        return list(self.db.scalars(stmt))

    def list_distinct_lead_user_ids(self) -> list[int]:
        """Уникальные user_id из лидов — аудитория рассылки."""
        stmt = select(models.Lead.user_id).distinct()
        return list(self.db.scalars(stmt))

    def get_broadcast(self, broadcast_id: int) -> models.Broadcast | None:
        return self.db.get(models.Broadcast, broadcast_id)

    def list_broadcasts_recent(self, limit: int = 20) -> list[models.Broadcast]:
        stmt = (
            select(models.Broadcast)
            .order_by(models.Broadcast.id.desc())
            .limit(limit)
        )
        return list(self.db.scalars(stmt))

    def count_broadcasts(self) -> int:
        n = self.db.scalar(select(func.count()).select_from(models.Broadcast))
        return int(n or 0)

    def list_broadcasts_paged(self, offset: int, limit: int) -> list[models.Broadcast]:
        stmt = (
            select(models.Broadcast)
            .order_by(models.Broadcast.id.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(self.db.scalars(stmt))

    def duplicate_broadcast(self, source_id: int) -> models.Broadcast | None:
        src = self.get_broadcast(source_id)
        if not src:
            return None
        return self.create_broadcast(
            title=src.title,
            text=src.text,
            button_url=src.button_url,
            button_text=src.button_text or "Перейти к акции",
            image_url=src.image_url,
            send_at=None,
            status="scheduled",
        )

    def set_broadcast_send_at(self, broadcast_id: int, send_at: datetime | None) -> bool:
        b = self.db.get(models.Broadcast, broadcast_id)
        if not b or b.status != "scheduled":
            return False
        b.send_at = send_at
        self.db.commit()
        return True

    def cancel_pending_broadcast(self, broadcast_id: int) -> bool:
        b = self.db.get(models.Broadcast, broadcast_id)
        if not b or b.status != "scheduled":
            return False
        b.status = "cancelled"
        self.db.commit()
        return True

    def list_scheduled_broadcasts_with_send_at(self) -> list[models.Broadcast]:
        """Все отложенные по send_at (для восстановления планировщика после рестарта)."""
        stmt = (
            select(models.Broadcast)
            .where(
                models.Broadcast.status == "scheduled",
                models.Broadcast.send_at.isnot(None),
            )
            .order_by(models.Broadcast.send_at.asc())
        )
        return list(self.db.scalars(stmt))

    def try_claim_broadcast_for_sending(self, broadcast_id: int) -> models.Broadcast | None:
        """Атомарно перевести scheduled → sending; вернуть запись или None если не получилось."""
        from sqlalchemy import update

        res = self.db.execute(
            update(models.Broadcast)
            .where(
                models.Broadcast.id == broadcast_id,
                models.Broadcast.status == "scheduled",
            )
            .values(status="sending")
        )
        self.db.commit()
        if res.rowcount == 0:
            return None
        return self.get_broadcast(broadcast_id)

    def add_required_channel(self, title: str, chat_id: int, invite_link: str | None) -> models.RequiredChannel:
        channel = models.RequiredChannel(title=title, chat_id=chat_id, invite_link=invite_link)
        self.db.add(channel)
        self.db.commit()
        self.db.refresh(channel)
        return channel

    def list_required_channels(self) -> list[models.RequiredChannel]:
        return list(self.db.scalars(select(models.RequiredChannel).order_by(models.RequiredChannel.id.desc())))

    def delete_required_channel(self, channel_id: int) -> None:
        entity = self.db.get(models.RequiredChannel, channel_id)
        if entity:
            self.db.delete(entity)
            self.db.commit()

    def create_broadcast(
        self,
        title: str,
        text: str,
        button_url: str,
        button_text: str = "Перейти к акции",
        image_url: str | None = None,
        send_at: datetime | None = None,
        status: str = "scheduled",
    ) -> models.Broadcast:
        item = models.Broadcast(
            title=title,
            text=text,
            button_text=button_text,
            button_url=button_url,
            image_url=image_url,
            send_at=send_at,
            status=status,
        )
        self.db.add(item)
        self.db.commit()
        self.db.refresh(item)
        return item

    def update_broadcast_status(self, broadcast_id: int, status: str) -> None:
        item = self.db.get(models.Broadcast, broadcast_id)
        if item:
            item.status = status
            self.db.commit()

    def mark_broadcast_sent(self, broadcast_id: int) -> None:
        item = self.db.get(models.Broadcast, broadcast_id)
        if item:
            item.status = "sent"
            item.sent_at = datetime.utcnow()
            self.db.commit()
