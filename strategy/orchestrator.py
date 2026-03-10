from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone

import structlog

from config.settings import Settings
from core.client import DeribitClient
from core.ws_monitor import PriceMonitor
from strategy.entry import StraddleEntry, enter_straddle
from strategy.exit import close_all, close_tier1
from strategy.instrument_selector import StraddleInstruments, select_straddle
from strategy.position_sizer import SizingResult, compute_size
from utils.helpers import utcnow

log = structlog.get_logger("orchestrator")


class StraddleOrchestrator:
    """Full lifecycle: enter -> monitor combined premium -> tiered exit."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: DeribitClient | None = None
        self._instruments: StraddleInstruments | None = None
        self._sizing: SizingResult | None = None
        self._entry: StraddleEntry | None = None
        self._tier1_closed = False
        self._monitor: PriceMonitor | None = None
        self._ws_stop: asyncio.Event | None = None

    def _exit_time_utc(self) -> datetime:
        now = utcnow()
        exit_dt = now.replace(
            hour=self._settings.exit_hour_utc,
            minute=self._settings.exit_minute_utc,
            second=0, microsecond=0,
        )
        if exit_dt <= now:
            exit_dt = exit_dt.replace(day=exit_dt.day + 1)
        return exit_dt

    def _on_tp_hit(self) -> None:
        """Called by WS monitor (from async context) when combined TP is hit.

        Fires Tier 1 close on a background thread to avoid blocking the event loop.
        """
        if self._tier1_closed:
            return
        self._tier1_closed = True

        def _do_close():
            try:
                close_tier1(
                    self._client,
                    self._entry.call_leg.instrument_name,
                    self._entry.put_leg.instrument_name,
                    float(self._sizing.tier1_contracts),
                )
                log.info("tier1_tp_exit_complete",
                         tier1_qty=self._sizing.tier1_contracts,
                         remaining=self._sizing.tier2_contracts)
            except Exception:
                log.exception("tier1_close_failed")

        threading.Thread(target=_do_close, daemon=True).start()

    def run(self) -> None:
        """Execute the full lifecycle synchronously (blocks until exit time)."""
        s = self._settings

        self._client = DeribitClient(s)
        try:
            self._instruments = select_straddle(self._client, s)
            self._sizing = compute_size(self._client, self._instruments, s)

            log.info("tier_split",
                     total=self._sizing.contracts,
                     tier1=self._sizing.tier1_contracts,
                     tier2=self._sizing.tier2_contracts,
                     tp_pct=s.take_profit_pct)

            self._entry = enter_straddle(
                self._client, self._instruments, self._sizing.contracts, s,
                cached_call_ask=self._sizing.call_ask,
                cached_put_ask=self._sizing.put_ask,
            )

            entry_premium = self._entry.per_contract_premium
            tp_threshold = entry_premium * (1 + s.take_profit_pct)
            exit_time = self._exit_time_utc()

            log.info("monitoring_started",
                     entry_premium=round(entry_premium, 6),
                     tp_threshold=round(tp_threshold, 6),
                     exit_time_utc=exit_time.isoformat(),
                     tier1_qty=self._sizing.tier1_contracts,
                     tier2_qty=self._sizing.tier2_contracts)

            self._monitor = PriceMonitor(
                ws_url=s.ws_url,
                client_id=s.client_id,
                client_secret=s.client_secret,
                call_instrument=self._instruments.call_name,
                put_instrument=self._instruments.put_name,
                entry_call_avg=self._entry.call_leg.average_price,
                entry_put_avg=self._entry.put_leg.average_price,
                take_profit_pct=s.take_profit_pct,
                on_tp_hit=self._on_tp_hit,
            )

            asyncio.run(self._run_monitor_until_exit(exit_time))

            log.info("exit_time_reached", time=utcnow().isoformat())
            close_all(self._client, s)
            log.info("lifecycle_complete",
                     tier1_tp_hit=self._tier1_closed,
                     final_time=utcnow().isoformat())

        finally:
            self._client.close()

    async def _run_monitor_until_exit(self, exit_time: datetime) -> None:
        """Run the WS monitor until exit_time is reached."""
        self._ws_stop = asyncio.Event()

        monitor_task = asyncio.create_task(self._monitor.start(self._ws_stop))

        while True:
            now = datetime.now(timezone.utc)
            remaining_s = (exit_time - now).total_seconds()
            if remaining_s <= 0:
                break

            wait_s = min(remaining_s, 5.0)
            await asyncio.sleep(wait_s)

            if self._monitor.call_mark > 0 and self._monitor.put_mark > 0:
                combined = self._monitor.combined_mark
                entry_p = self._entry.per_contract_premium
                tp_thresh = entry_p * (1 + self._settings.take_profit_pct)
                pnl_pct = (combined / entry_p - 1) * 100 if entry_p > 0 else 0
                remaining_min = remaining_s / 60
                log.info("price_check",
                         call_mark=round(self._monitor.call_mark, 6),
                         put_mark=round(self._monitor.put_mark, 6),
                         combined=round(combined, 6),
                         tp_threshold=round(tp_thresh, 6),
                         pnl_pct=round(pnl_pct, 2),
                         tier1_closed=self._tier1_closed,
                         exit_in_min=round(remaining_min, 1))

        self._ws_stop.set()
        try:
            await asyncio.wait_for(monitor_task, timeout=5.0)
        except asyncio.TimeoutError:
            monitor_task.cancel()

    def dry_run(self) -> None:
        """Select instruments and size without placing orders."""
        s = self._settings
        with DeribitClient(s) as client:
            instruments = select_straddle(client, s)
            sizing = compute_size(client, instruments, s)

            exit_time = self._exit_time_utc()
            entry_premium = sizing.call_ask + sizing.put_ask
            tp_threshold = entry_premium * (1 + s.take_profit_pct)

            log.info("dry_run_complete",
                     call=instruments.call_name, put=instruments.put_name,
                     strike=instruments.strike, spot=instruments.spot_price, dte=instruments.dte,
                     contracts=sizing.contracts,
                     tier1=sizing.tier1_contracts, tier2=sizing.tier2_contracts,
                     est_premium=round(entry_premium, 6),
                     tp_threshold=round(tp_threshold, 6),
                     exit_time_utc=exit_time.isoformat())
