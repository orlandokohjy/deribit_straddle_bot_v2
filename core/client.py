from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import structlog

from config.settings import Settings

log = structlog.get_logger("deribit.client")


class DeribitClientError(Exception):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        self.code = code
        self.api_message = message
        self.data = data
        super().__init__(f"Deribit error {code}: {message}")


def _fire_request(base_url: str, method: str, params: dict[str, Any], token: str | None) -> Any:
    url = f"{base_url}/{method}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with httpx.Client(timeout=15.0) as session:
        resp = session.get(url, params=params, headers=headers)
    body = resp.json()
    if "error" in body:
        err = body["error"]
        raise DeribitClientError(err["code"], err["message"], err.get("data"))
    resp.raise_for_status()
    return body.get("result")


class DeribitClient:
    """Synchronous REST client for Deribit API v2.

    Used for all order execution. Supports parallel() for concurrent requests.
    """

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.rest_url
        self._session = httpx.Client(
            timeout=15.0,
            limits=httpx.Limits(max_keepalive_connections=0),
        )
        self._token: str | None = None
        self._authenticate(settings.client_id, settings.client_secret)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "DeribitClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}/{method}"
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        resp = self._session.get(url, params=params or {}, headers=headers)
        body = resp.json()
        if "error" in body:
            err = body["error"]
            raise DeribitClientError(err["code"], err["message"], err.get("data"))
        resp.raise_for_status()
        return body.get("result")

    def public(self, method: str, params: dict[str, Any] | None = None) -> Any:
        return self._request(f"public/{method}", params)

    def private(self, method: str, params: dict[str, Any] | None = None) -> Any:
        return self._request(f"private/{method}", params)

    def parallel(self, *calls: tuple[str, dict[str, Any] | None]) -> list[Any]:
        results: list[Any] = [None] * len(calls)
        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            futures = {
                pool.submit(_fire_request, self._base_url, method, params or {}, self._token): i
                for i, (method, params) in enumerate(calls)
            }
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()
        return results

    def _authenticate(self, client_id: str, client_secret: str) -> None:
        result = self.public("auth", {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        })
        self._token = result["access_token"]
        log.info("authenticated", env=self._base_url.split("//")[1].split("/")[0])

    @property
    def token(self) -> str | None:
        return self._token
