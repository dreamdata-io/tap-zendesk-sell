import sys
import json
import base64
from datetime import datetime
from typing import TextIO, Dict, Optional, Any

from state import State, StateEntry


class _DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        DATETIME_ERR = "Object of type datetime is not JSON serializable"
        BYTES_ERR = "Object of type bytes is not JSON serializable"
        DECIMAL_ERR = "Object of type Decimal is not JSON serializable"
        try:
            super().default(obj)
        except TypeError as err:
            err_str = str(err)

            if err_str == DATETIME_ERR:
                return obj.isoformat() + "Z"
            elif err_str == BYTES_ERR:
                try:
                    return obj.decode("utf-8")
                except UnicodeDecodeError:
                    pass

                # failing to utf-8 encode,
                # fallback to base64 and nest within
                # base64 object
                return {"base64": base64.b64encode(obj)}
            elif err_str == DECIMAL_ERR:
                return str(obj)
            else:
                raise


class Stream:
    state: State

    def __init__(
        self, state: Optional[State] = None, state_filename: Optional[str] = None
    ):
        if state and state_filename:
            raise ValueError(
                "'state' and 'state_filename' parameters cannot both be specified"
            )

        if state:
            self.state = state
            return

        if state_filename:
            self.state = State.parse_file(state_filename)
            return

        self.state = State()

    def set_stream_state(self, stream_id, key: str, value: Any):
        self.state.set_stream_state(stream_id, key, value)

    def get_stream_state(self, stream_id) -> Optional[StateEntry]:
        return self.state.get_stream_state(stream_id)

    def write_state(self, file: TextIO = sys.stdout):
        state_message = dict(type="STATE", value=self.state.dict())
        Stream.write_message(state_message, file=file)

    @staticmethod
    def write_record(record: Dict, stream_id: str, file: TextIO = sys.stdout):
        Stream.write_message(
            dict(
                type="RECORD",
                stream=stream_id,
                time_extracted=datetime.utcnow().isoformat() + "Z",
                record=record,
            ),
            file=file,
        )

    @staticmethod
    def write_message(message: Dict, file: TextIO = sys.stdout):
        line = json.dumps(message, cls=_DatetimeEncoder)
        file.write(line + "\n")
        file.flush()
