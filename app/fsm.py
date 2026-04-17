"""
Простой in-memory FSM для диалогов. Хранит состояние и накопленные данные по user_id.
При перезапуске контейнера состояния сбрасываются — для админки это приемлемо.
"""
from dataclasses import dataclass, field


@dataclass
class UserState:
    state: str
    data: dict = field(default_factory=dict)


_store: dict[int, UserState] = {}


def get_state(user_id: int) -> UserState | None:
    return _store.get(user_id)


def set_state(user_id: int, state: str, data: dict | None = None) -> None:
    _store[user_id] = UserState(state=state, data=data or {})


def update_data(user_id: int, **kwargs) -> None:
    if user_id in _store:
        _store[user_id].data.update(kwargs)


def clear_state(user_id: int) -> None:
    _store.pop(user_id, None)
