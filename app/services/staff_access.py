"""Доступ к админ-боту: администраторы из .env и модераторы из БД."""

from app.config import Settings
from app.db.repo import Repo


def is_env_admin(user_id: int, settings: Settings) -> bool:
    return user_id in settings.admin_user_ids


def can_manage_moderators(user_id: int, settings: Settings) -> bool:
    """Только пользователи из ADMIN_USER_IDS."""
    return is_env_admin(user_id, settings)


def can_use_admin_bot(user_id: int, settings: Settings, repo: Repo) -> bool:
    if is_env_admin(user_id, settings):
        return True
    return repo.is_moderator(user_id)
