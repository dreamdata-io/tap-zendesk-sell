from typing import Optional, Dict
import time
import json

import requests


class ZendeskSell:
    _client_id: str
    _client_secret: str
    _refresh_token: str
    _access_token: Optional[str] = None
    _access_token_ttl: Optional[float] = None
    _session = None

    class Decorators:
        @staticmethod
        def refresh_token(decorated):
            # the function that is used to check
            # the JWT and refresh if necessary
            def wrapper(api, *args, **kwargs):
                if api._access_token_ttl is None or time.time() > api._access_token_ttl:
                    api._refresh_access_token()

                return decorated(api, *args, **kwargs)

            return wrapper

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._session = requests.Session()

    @Decorators.refresh_token
    def call(self, method: str, path: str, *args, **kwargs) -> Dict:
        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"
        kwargs["headers"] = headers

        try:
            resp = self._session.request(
                method, "https://api.getbase.com" + path, *args, **kwargs
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            print(err)
            raise

    def _refresh_access_token(self):
        try:
            req = requests.Request(
                "POST",
                "https://api.getbase.com/oauth2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                },
                auth=(self._client_id, self._client_secret),
            )
            prep = req.prepare()
            resp = self._session.send(prep)
            resp.raise_for_status()
        except requests.RequestException as err:
            print(err)
            raise
        else:
            data = resp.json()
            self._access_token = data["access_token"]
            self._access_token_ttl = float(data["expires_in"] - 100)
            self._refresh_token = data["refresh_token"]

        # TODO: remove once we figure out how
        with open("config.json", "w") as fp:
            json.dump(
                dict(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    refresh_token=self._refresh_token,
                ),
                fp,
                indent="  ",
                sort_keys=True,
            )
