# dialog_manager.py
from enum import Enum, auto
from datetime import datetime
from typing import Dict, Any, Optional

class DialogState(Enum):
    NONE = auto()
    RESCHEDULE_DATE = auto()
    RESCHEDULE_TIME = auto()

class DialogManager:
    def __init__(self):
        self._data: Dict[int, Dict[str, Any]] = {}

    def get_state(self, chat_id: int) -> DialogState:
        return self._data.get(chat_id, {}).get('state', DialogState.NONE)

    def set_state(self, chat_id: int, state: DialogState):
        if chat_id not in self._data:
            self._data[chat_id] = {}
        self._data[chat_id]['state'] = state

    def update(self, chat_id: int, **kwargs):
        if chat_id not in self._data:
            self._data[chat_id] = {'state': DialogState.NONE}
        self._data[chat_id].update(kwargs)

    def get(self, chat_id: int, key: str, default=None):
        return self._data.get(chat_id, {}).get(key, default)

    def get_all(self, chat_id: int) -> Dict[str, Any]:
        return self._data.get(chat_id, {})

    def clear(self, chat_id: int):
        if chat_id in self._data:
            del self._data[chat_id]

    def reset(self, chat_id: int):
        self._data[chat_id] = {'state': DialogState.NONE}