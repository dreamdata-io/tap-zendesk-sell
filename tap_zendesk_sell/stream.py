import sys
import json
import base64
from datetime import datetime
from typing import TextIO, Dict, Optional, Any, Iterable

from tap_zendesk_sell.state import State, StateEntry


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


def encode_dt(val: datetime) -> str:
    assert isinstance(val, datetime)
    return val.isoformat(timespec="milliseconds")


def decode_dt(val: str) -> datetime:
    if val.endswith("Z"):
        val = val[:-1]
    return datetime.fromisoformat(val)


def skip_descending(gen: Iterable[Dict], state: datetime, key: str) -> Iterable[Dict]:
    """inverses the ordering from descending to ascending; stopping when we hit an item we've seen before.
    This importantly also stopped consuming from the generator, which will stop the client paginating until the end."""
    descending_list = []
    for record in gen:
        replication_value = decode_dt(record[key])
        # items can have the same datetime in the records
        if replication_value >= state:
            descending_list.append(record)
        else:
            break

    yield from reversed(descending_list)


def skip_unordered(gen: Iterable[Dict], state: datetime, key: str) -> Iterable[Dict]:
    """runs through all the items (it has to, because they have no ordering)
    and sorts and filters them based on the previous state"""
    if not state:
        yield from sorted(gen, key=lambda record: decode_dt(record[key]), reverse=False)
    random_list = []
    for record in gen:
        replication_value = decode_dt(record[key])
        if replication_value >= state:
            random_list.append(record)

    yield from sorted(
        random_list, key=lambda record: decode_dt(record[key]), reverse=False
    )


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

    def set_stream_state(self, stream_id: str, key: str, value: Any):
        self.state.set_stream_state(stream_id, key, value)

    def get_stream_state(self, stream_id: Optional[str]) -> Optional[StateEntry]:
        if stream_id is None:
            return None

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
