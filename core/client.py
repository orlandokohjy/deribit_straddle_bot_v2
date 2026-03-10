from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import structlog

from config.settings import Settings

log = structlog.get_logger("deribit.client")

_TOKEN_REFRESH_BUFFER_S = 60


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
    Auto-refreshes the access token before it expires.
    """

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.rest_url
        self._client_id = settings.client_id
        self._client_secret = settings.client_secret
        self._session = httpx.Client(
            timeout=15.0,
            limits=httpx.Limits(max_keepalive_connections=0),
        )
        self._token: str | None = None
        self._refresh_token: str | None = None
        self._token_expires_at: float = 0.0
        self._authenticate()

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "DeribitClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _ensure_token(self) -> None:
        """Refresh the access token if it's about to expire."""
        if time.monotonic() < self._token_expires_at - _TOKEN_REFRESH_BUFFER_S:
            return
        if self._refresh_token:
            try:
                self._do_refresh()
                return
            except DeribitClientError:
                log.warning("refresh_token_failed_falling_back_to_reauth")
        self._authenticate()

    def _request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        if not method.startswith("public/auth"):
            self._ensure_token()
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
        self._ensure_token()
        results: list[Any] = [None] * len(calls)
        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            futures = {
                pool.submit(_fire_request, self._base_url, method, params or {}, self._token): i
                for i, (method, params) in enumerate(calls)
            }
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()
        return results

    def _store_auth(self, result: dict[str, Any]) -> None:
        self._token = result["access_token"]
        self._refresh_token = result.get("refresh_token")
        expires_in = int(result.get("expires_in", 900))
        self._token_expires_at = time.monotonic() + expires_in
        log.info("authenticated",
                 env=self._base_url.split("//")[1].split("/")[0],
                 expires_in_s=expires_in)

    def _authenticate(self) -> None:
        result = self.public("auth", {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "client_credentials",
        })
        self._store_auth(result)

    def _do_refresh(self) -> None:
        result = self.public("auth", {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        })
        self._store_auth(result)
        log.info("token_refreshed")

    @property
    def token(self) -> str | None:
        return self._token
