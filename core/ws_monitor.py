from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import structlog
import websockets

log = structlog.get_logger("ws_monitor")

_HEARTBEAT_INTERVAL = 15


class PriceMonitor:
    """WebSocket monitor that subscribes to mark prices for two instruments
    and fires a callback when the combined premium hits the TP threshold.

    Runs in an asyncio event loop. Designed to be started from a sync context
    via run() or from an existing loop via start().
    """

    def __init__(
        self,
        ws_url: str,
        client_id: str,
        client_secret: str,
        call_instrument: str,
        put_instrument: str,
        entry_call_avg: float,
        entry_put_avg: float,
        take_profit_pct: float,
        on_tp_hit: Callable[[], None],
    ) -> None:
        self._ws_url = ws_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._call_instrument = call_instrument
        self._put_instrument = put_instrument
        self._entry_premium = entry_call_avg + entry_put_avg
        self._tp_threshold = self._entry_premium * (1 + take_profit_pct)
        self._on_tp_hit = on_tp_hit

        self._call_mark: float = 0.0
        self._put_mark: float = 0.0
        self._tp_triggered = False
        self._stop_event: asyncio.Event | None = None
        self._ws: Any = None
        self._msg_id = 0

    @property
    def call_mark(self) -> float:
        return self._call_mark

    @property
    def put_mark(self) -> float:
        return self._put_mark

    @property
    def combined_mark(self) -> float:
        return self._call_mark + self._put_mark

    @property
    def tp_triggered(self) -> bool:
        return self._tp_triggered

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def _send(self, method: str, params: dict[str, Any]) -> None:
        msg = json.dumps({"jsonrpc": "2.0", "id": self._next_id(), "method": method, "params": params})
        await self._ws.send(msg)

    async def _authenticate(self) -> None:
        await self._send("public/auth", {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        })
        resp = json.loads(await self._ws.recv())
        if "error" in resp:
            raise RuntimeError(f"WS auth failed: {resp['error']}")
        log.info("ws_authenticated")

    async def _subscribe(self) -> None:
        channels = [
            f"ticker.{self._call_instrument}.raw",
            f"ticker.{self._put_instrument}.raw",
        ]
        await self._send("public/subscribe", {"channels": channels})
        resp = json.loads(await self._ws.recv())
        if "error" in resp:
            raise RuntimeError(f"WS subscribe failed: {resp['error']}")
        log.info("ws_subscribed", channels=channels)

    async def _enable_heartbeat(self) -> None:
        await self._send("public/set_heartbeat", {"interval": _HEARTBEAT_INTERVAL})
        resp = json.loads(await self._ws.recv())
        if "error" in resp:
            log.warning("heartbeat_setup_failed", error=resp["error"])

    def _handle_ticker(self, data: dict[str, Any]) -> None:
        instrument = data.get("instrument_name", "")
        mark = float(data.get("mark_price", 0))

        if instrument == self._call_instrument:
            self._call_mark = mark
        elif instrument == self._put_instrument:
            self._put_mark = mark
        else:
            return

        if self._call_mark <= 0 or self._put_mark <= 0:
            return

        combined = self._call_mark + self._put_mark

        if not self._tp_triggered and combined >= self._tp_threshold:
            self._tp_triggered = True
            log.info(
                "tp_hit",
                call_mark=self._call_mark,
                put_mark=self._put_mark,
                combined=round(combined, 6),
                threshold=round(self._tp_threshold, 6),
                entry_premium=round(self._entry_premium, 6),
                pnl_pct=round((combined / self._entry_premium - 1) * 100, 2),
            )
            self._on_tp_hit()

    async def _listen(self) -> None:
        while not self._stop_event.is_set():
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=_HEARTBEAT_INTERVAL + 5)
            except asyncio.TimeoutError:
                log.warning("ws_recv_timeout")
                break
            except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK):
                log.warning("ws_connection_closed")
                break

            msg = json.loads(raw)

            if msg.get("method") == "heartbeat":
                if msg.get("params", {}).get("type") == "test_request":
                    await self._send("public/test", {})
                continue

            if msg.get("method") == "subscription":
                data = msg.get("params", {}).get("data", {})
                self._handle_ticker(data)

    async def start(self, stop_event: asyncio.Event) -> None:
        """Connect, authenticate, subscribe, and listen until stop_event is set.

        Automatically reconnects on disconnect.
        """
        self._stop_event = stop_event
        backoff = 1.0

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self._ws = ws
                    backoff = 1.0
                    await self._authenticate()
                    await self._enable_heartbeat()
                    await self._subscribe()
                    log.info("ws_monitor_running",
                             call=self._call_instrument, put=self._put_instrument,
                             tp_threshold=round(self._tp_threshold, 6))
                    await self._listen()
            except Exception as exc:
                if stop_event.is_set():
                    break
                log.warning("ws_reconnecting", error=str(exc), backoff_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

        log.info("ws_monitor_stopped")
