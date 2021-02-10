import argparse
import logging
import sys
import time
from typing import Any, Dict, Iterable, Optional

from pydantic import BaseModel

from tap_zendesk_sell.stream import (
    Stream,
    decode_dt,
    encode_dt,
    skip_descending,
    skip_unordered,
)
from tap_zendesk_sell.zendesk_client import ZendeskSell

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


class Config(BaseModel):
    client_id: str
    client_secret: str
    user_agent: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token: Optional[str] = None


def main():
    parser = argparse.ArgumentParser(description="Ingest data from Zendesk Sell")
    parser.add_argument("--config", "-c", type=str)
    parser.add_argument("--state", "-s", type=str, default=None)

    args = parser.parse_args()
    config_filename = args.config
    state_filename = args.state

    tap(config_filename, state_filename)


def tap(config_filename: str, state_filename: Optional[str]):
    """
    if we are performing an incremental replication
    we will attempt to read backwards (since there is no way
    to say: "after this datetime").
    we reverse the order prior to sending it to the generic output
    function for simplicity - we don't worry about it, because it will most likely
    not be a lot of data in a daily increment
    """

    stream = Stream(state_filename=state_filename)

    stream.write_state()

    config = Config.parse_file(config_filename)

    client = ZendeskSell(
        config.client_id,
        config.client_secret,
        refresh_token=config.refresh_token,
        access_token=config.access_token,
    )

    try:
        # process_dependent_streams(stream, client)
        process_ordered_streams(stream, client)
        process_unordered_streams(stream, client)
    except Exception:
        logging.exception("encountered error - aborting")
        sys.exit(1)
    finally:
        stream.write_state()


def process_dependent_streams(stream: Stream, client: ZendeskSell):
    """only associated_contacts requires a qualifying 'deal_id'.
    We only processes the deal_ids that got updated since last time,
    but this is still probably the most expensive section of the code since
    we have to call it for Every-Single-Id (tm).
    """
    order_by = "updated_at"
    replication_key = "updated_at"
    per_page = 100
    endpoint = "deals"

    state_entry = stream.get_stream_state(endpoint)
    if state_entry:
        state = decode_dt(state_entry.value)
        order_dir = "desc"
    else:
        state = None
        order_dir = "asc"

    record_gen = client.get(
        endpoint, order_by=order_by, order_dir=order_dir, per_page=per_page
    )

    if order_dir == "desc":
        assert state is not None
        record_gen = skip_descending(record_gen, state, replication_key)

    deal_ids = process_stream(
        stream,
        state,
        endpoint,
        record_gen,
        replication_key=replication_key,
        yield_field="id",  # yield the deal id
    )
    assert isinstance(deal_ids, list)

    logging.info(f"number of deal ids: {len(deal_ids)}")

    stream_id = "associated_contacts"

    for deal_id in deal_ids:
        logging.info(f"processing associated_deals for {deal_id}..")
        associated_contacts_gen = client.get(f"deals/{deal_id}/{stream_id}")
        process_stream(stream, None, stream_id, associated_contacts_gen)
    logging.info(f"{stream_id}: done processing all associated_contacts")


def process_unordered_streams(stream: Stream, client: ZendeskSell):
    order_by = None
    order_dir = None
    partition_key = "updated_at"
    per_page = 100
    stream_ids = [
        "calls",
        "call_outcomes",
        "visits",
        "visit_outcomes",
        "lead_unqualified_reasons",
        "pipelines",
        "stages",
    ]

    for endpoint in stream_ids:
        state_entry = stream.get_stream_state(endpoint)
        if state_entry:
            state = decode_dt(state_entry.value)
        else:
            state = None

        record_gen = client.get(
            endpoint, order_by=order_by, order_dir=order_dir, per_page=per_page
        )

        record_gen = skip_unordered(record_gen, state, partition_key)

        process_stream(
            stream,
            state,
            endpoint,
            record_gen,
            replication_key=partition_key,
        )


def process_ordered_streams(stream: Stream, client: ZendeskSell):
    """this function processes all ordered streams, ie. endpoints that enable
    us to call them with an 'order_by' and a direction [asc|desc]"""
    order_by = "updated_at"
    replication_key = "updated_at"
    per_page = 100

    for endpoint in [
        "deals",
        "contacts",
        "collaborations",
        "deal_sources",
        "deal_unqualified_reasons",
        "lead_sources",
        "leads",
        "loss_reasons",
        "users",
        "tags",
        "tasks",
    ]:
        state_entry = stream.get_stream_state(endpoint)
        if state_entry:
            state = decode_dt(state_entry.value)
            order_dir = "desc"
        else:
            state = None
            order_dir = "asc"

        record_gen = client.get(
            endpoint, order_by=order_by, order_dir=order_dir, per_page=per_page
        )

        if order_dir == "desc":
            assert state is not None
            record_gen = skip_descending(record_gen, state, replication_key)

        process_stream(
            stream,
            state,
            endpoint,
            record_gen,
            replication_key=replication_key,
        )


def process_stream(
    stream: Stream,
    state: Optional[Any],
    stream_id: str,
    record_gen: Iterable[Dict],
    replication_key: Optional[str] = None,
    max: Optional[int] = None,
    yield_field: Optional[str] = None,
) -> Optional[Iterable]:
    # assertion #1: record_gen is in ascending order
    # asserting #2: record_gen starts at beginning or at or after latest state
    logging.info(f"{stream_id}: processing stream records..")
    i = 0
    yield_values = [] if yield_field else None
    start_time = time.time()
    start_state = state
    try:
        for record in record_gen:
            if replication_key:

                replication_value = decode_dt(record[replication_key])

                if start_state and replication_value < start_state:
                    continue
                
                stream.set_stream_state(
                    stream_id, replication_key, encode_dt(replication_value)
                )
                if (not state) or (replication_value > state):
                    state = replication_value

            stream.write_record(record, stream_id)

            if yield_field:
                # this logic exists purely to get associated_contacts
                # since it requires a deal_id
                yield_values.append(record[yield_field])

            i += 1

            if i % 1000 == 0:
                logging.info(
                    f"{stream_id}: processed {i} records in {time.time() - start_time:.2f}"
                )

            if max and i >= max:
                break

        logging.info(
            f"{stream_id}: completed. Processed {i} records in {time.time() - start_time:.2f}s"
        )

        return yield_values

    except Exception:
        logging.error(f"failed while processing {stream_id}")
        raise


if __name__ == "__main__":
    tap("config.json", None)
