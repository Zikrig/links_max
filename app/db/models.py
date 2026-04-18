from datetime import datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


class Platform(Base):
    __tablename__ = "platforms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    offers: Mapped[list["Offer"]] = relationship(back_populates="platform")


class Offer(Base):
    __tablename__ = "offers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    created_date: Mapped[Date] = mapped_column(Date, default=datetime.utcnow().date)
    base_url: Mapped[str] = mapped_column(Text, default="")
    subid_param: Mapped[str] = mapped_column(String(80), default="")
    next_subid: Mapped[int] = mapped_column(Integer, default=1)

    platform: Mapped["Platform"] = relationship(back_populates="offers")
    scenarios: Mapped[list["Scenario"]] = relationship(back_populates="offer")
    leads: Mapped[list["Lead"]] = relationship(back_populates="offer")


class Scenario(Base):
    __tablename__ = "scenarios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    offer_id: Mapped[int] = mapped_column(ForeignKey("offers.id"), index=True)
    code: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    channel_chat_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    channel_title: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    offer: Mapped["Offer"] = relationship(back_populates="scenarios")
    bot_link: Mapped["BotLink"] = relationship(back_populates="scenario", uselist=False)


class BotLink(Base):
    __tablename__ = "bot_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scenario_id: Mapped[int] = mapped_column(ForeignKey("scenarios.id"), unique=True)
    deep_link: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    scenario: Mapped["Scenario"] = relationship(back_populates="bot_link")


class RequiredChannel(Base):
    __tablename__ = "required_channels"
    __table_args__ = (UniqueConstraint("chat_id", name="uq_required_channel_chat_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    chat_id: Mapped[int] = mapped_column(Integer, index=True)
    invite_link: Mapped[str | None] = mapped_column(String(255), nullable=True)


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    max_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_username: Mapped[str | None] = mapped_column(String(120), nullable=True)
    full_name: Mapped[str] = mapped_column(String(255))
    phone: Mapped[str] = mapped_column(String(30))
    consent_accepted: Mapped[bool] = mapped_column(Boolean, default=False)
    subid_value: Mapped[str] = mapped_column(String(4), index=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    offer_id: Mapped[int] = mapped_column(ForeignKey("offers.id"), index=True)
    scenario_id: Mapped[int] = mapped_column(ForeignKey("scenarios.id"), index=True)

    offer: Mapped["Offer"] = relationship(back_populates="leads")


class Broadcast(Base):
    __tablename__ = "broadcasts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    text: Mapped[str] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    button_text: Mapped[str] = mapped_column(String(80), default="Перейти к акции")
    button_url: Mapped[str] = mapped_column(String(255))
    send_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="scheduled")
