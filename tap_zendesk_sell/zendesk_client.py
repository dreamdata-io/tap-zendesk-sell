from typing import Iterable, Optional, Dict
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ZendeskSell:
    _client_id: Optional[str] = None
    _client_secret: Optional[str] = None
    _refresh_token: Optional[str]
    _access_token: Optional[str]
    _access_token_ttl: Optional[float] = None
    _session: Optional[requests.Session] = None

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: Optional[str] = None,
        access_token: Optional[str] = None,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token = access_token

        if access_token is None and refresh_token is None:
            raise ValueError("'access_token' and 'refresh_token' cannot both be None")

    def get(self, path: str, **kwargs) -> Iterable[Dict]:
        return self.__paginate("GET", f"/v2/{path}", **kwargs)

    def __paginate(
        self,
        method: str,
        path: str,
        order_by: Optional[str] = None,
        order_dir: Optional[str] = None,
        per_page: int = 100,
        page: int = 1,
        **kwargs,
    ) -> Iterable[Dict]:
        params = kwargs.pop("params", {})

        params.update({"per_page": per_page})
        if order_by:
            sort_by = f"{order_by}:{order_dir if order_dir else 'asc'}"
            params.update({"sort_by": sort_by})

        while True:
            params.update({"page": page})
            response_data = self.__do(method, path, params=params, **kwargs)

            items = response_data.get("items", [])
            if not items:
                return

            for entry in items:
                data = entry["data"]
                yield data

            # TODO: re-introduce if the above does 'if not items' turns out to not work
            # next_page = response_data["meta"].get("links", {}).get("next_page")
            # if next_page is None:
            #     return

            page += 1

    def __do(self, method: str, path: str, **kwargs) -> Dict:
        session = self.__session()
        with session.request(
            method, "https://api.getbase.com" + path, **kwargs
        ) as resp:
            resp.raise_for_status()
            return resp.json()

    def __session(self) -> requests.Session:
        if self._access_token_ttl is not None and time.time() < self._access_token_ttl:
            assert self._session is not None
            return self._session

        if self._refresh_token:
            token = self.__refresh_access_token()
        elif self._access_token is not None:
            token = self._access_token
            self._access_token_ttl = float("inf")
        else:
            raise ValueError("missing required 'access_token' or 'refresh_token'")

        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {token}"})

        # 409 is unique to zendesk sell and is raised when we are throttled
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504, 409],
            method_whitelist=["GET", "OPTIONS", "HEAD"],
        )

        self._session.mount("https://", HTTPAdapter(max_retries=retries))

        return self._session

    def __refresh_access_token(self) -> str:
        assert self._client_id is not None
        assert self._client_secret is not None
        resp = requests.post(
            "https://api.getbase.com/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            },
            auth=(self._client_id, self._client_secret),
        )
        resp.raise_for_status()

        data = resp.json()
        self._access_token = data["access_token"]
        self._access_token_ttl = time.time() + float(data["expires_in"] - 100)

        # # TODO: required for oauth2 code flow
        # self._refresh_token = data["refresh_token"]
        # with open("config.json", "w") as fp:
        #     json.dump(
        #         dict(
        #             client_id=self._client_id,
        #             client_secret=self._client_secret,
        #             refresh_token=self._refresh_token,
        #         ),
        #         fp,
        #         indent="  ",
        #         sort_keys=True,
        #     )

        assert self._access_token is not None
        return self._access_token
