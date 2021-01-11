from typing import Dict, Optional, Any

from pydantic import BaseModel


class StateEntry(BaseModel):
    key: str
    value: Any
    kind: str


class State(BaseModel):
    bookmarks: Dict[str, StateEntry] = {}

    def set_stream_state(self, stream: str, key: str, value: Any):
        self.bookmarks[stream] = StateEntry(key=key, value=value, kind=str(type(value)))

    def get_stream_state(self, stream: str) -> Optional[StateEntry]:
        return self.bookmarks.get(stream)