from typing import Iterable, Optional, Dict
import time
import json
import logging

import requests

logger = logging.getLogger(__name__)


class ZendeskSell:
    # static values
    _client_id: str
    _client_secret: str

    # these will change as access_token expires
    _refresh_token: str
    _access_token: Optional[str] = None
    _access_token_ttl: Optional[float] = None
    _session: Optional[requests.Session] = None

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token

    def get_contacts(self, per_page: int = 100):
        return self.__paginate(
            "GET",
            "/v2/contacts",
            sort_by="updated_at:desc",
            per_page=per_page,
        )

    def __paginate(
        self,
        method: str,
        path: str,
        sort_by: str,
        per_page: int = 100,
        page: int = 1,
        **kwargs,
    ) -> Iterable[Dict]:
        params = kwargs.pop("params", {})
        params.update({"sort_by": sort_by, "per_page": per_page})

        while True:
            params.update({"page": page})
            response_data = self.__do(method, path, params=params, **kwargs)

            items = response_data["items"]

            for entry in items:
                data = entry["data"]
                yield data

            next_page = response_data["meta"].get("links", {}).get("next_page")
            if next_page is None:
                return

            page += 1

    def __session(self) -> requests.Session:
        if self._access_token_ttl is not None and time.time() < self._access_token_ttl:
            assert self._session is not None
            return self._session

        token = self._refresh_access_token()

        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        return session

    def __do(self, method: str, path: str, **kwargs) -> Dict:
        session = self.__session()
        try:
            resp = session.request(method, "https://api.getbase.com" + path, **kwargs)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            logger.exception("failed to perform request")
            raise

    def _refresh_access_token(self) -> str:
        try:
            resp = requests.post(
                "https://api.getbase.com/oauth2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                },
                auth=(self._client_id, self._client_secret),
            )
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
            assert self._access_token is not None
            return self._access_token
