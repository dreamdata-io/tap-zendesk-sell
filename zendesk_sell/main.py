import argparse
from typing import Optional
from pydantic import BaseModel

from stream import Stream
from zendesk_client import ZendeskSell


class Config(BaseModel):
    user_agent: Optional[str]
    client_id: str
    client_secret: str
    refresh_token: str


def main():
    parser = argparse.ArgumentParser(description="Ingest data from Zendesk Sell")
    parser.add_argument("--config", "-c", type=str)
    parser.add_argument("--state", "-s", type=str, default=None)

    args = parser.parse_args()
    config_filename = args.config
    state_filename = args.state

    tap(config_filename, state_filename)


def tap(config_filename: str, state_filename: Optional[str]):

    stream = Stream(state_filename=state_filename)

    stream.write_state()

    config = Config.parse_file(config_filename)

    client = ZendeskSell(config.client_id, config.client_secret, config.refresh_token)

    print(client.get_contacts())


if __name__ == "__main__":
    tap("config.json", None)