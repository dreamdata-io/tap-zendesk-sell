from typing import Optional, Dict

import requests
from requests import exceptions


class ZendeskSell:
    client_id: str
    client_secret: str
    refresh_token: str
    access_token: Optional[str] = None

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

    def get_access_token(self) -> Dict:
        resp = requests.post(
            "https://api.getbase.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            auth=(self.client_id, self.client_secret),
        )
        resp.raise_for_status()

        return resp.json()
