from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from config.settings import Settings
from core.client import DeribitClient
from utils.helpers import ms_to_utc, utcnow

log = structlog.get_logger("strategy.instrument_selector")


@dataclass(frozen=True)
class StraddleInstruments:
    call_name: str
    put_name: str
    strike: float
    expiry_ts_ms: int
    spot_price: float
    dte: float

    @property
    def expiry_utc(self):
        return ms_to_utc(self.expiry_ts_ms)


def select_straddle(client: DeribitClient, settings: Settings) -> StraddleInstruments:
    """Select call + put for a long straddle (call slightly ITM, same strike)."""
    spot_result, instruments = client.parallel(
        ("public/get_index_price", {"index_name": "btc_usd"}),
        ("public/get_instruments", {"currency": "BTC", "kind": "option", "expired": "false"}),
    )
    spot = float(spot_result["index_price"])
    now = utcnow()

    min_dte = max(0.01, settings.target_dte - settings.dte_tolerance)
    max_dte = settings.target_dte + settings.dte_tolerance

    expiry_map: dict[int, list[dict[str, Any]]] = {}
    for inst in instruments:
        expiry_ms = inst["expiration_timestamp"]
        dte = (ms_to_utc(expiry_ms) - now).total_seconds() / 86400
        if min_dte <= dte <= max_dte:
            expiry_map.setdefault(expiry_ms, []).append(inst)

    if not expiry_map:
        raise RuntimeError(
            f"No BTC option expiries within DTE [{min_dte}, {max_dte}]. "
            f"target_dte={settings.target_dte}."
        )

    best_expiry_ms = min(
        expiry_map,
        key=lambda e: abs((ms_to_utc(e) - now).total_seconds() / 86400 - settings.target_dte),
    )
    candidates = expiry_map[best_expiry_ms]
    actual_dte = (ms_to_utc(best_expiry_ms) - now).total_seconds() / 86400

    calls_by_strike: dict[float, dict[str, Any]] = {}
    puts_by_strike: dict[float, dict[str, Any]] = {}
    for inst in candidates:
        strike = float(inst["strike"])
        if inst["option_type"] == "call":
            calls_by_strike[strike] = inst
        else:
            puts_by_strike[strike] = inst

    valid_strikes = sorted(
        [s for s in calls_by_strike if s in puts_by_strike and s <= spot],
        reverse=True,
    )

    if not valid_strikes:
        common_strikes = sorted(calls_by_strike.keys() & puts_by_strike.keys())
        if not common_strikes:
            raise RuntimeError("No strikes with both call and put for the selected expiry.")
        best_strike = min(common_strikes, key=lambda s: abs(s - spot))
        log.warning("no_itm_call_strike", spot=spot, fallback_strike=best_strike)
    else:
        best_strike = valid_strikes[0]

    result = StraddleInstruments(
        call_name=calls_by_strike[best_strike]["instrument_name"],
        put_name=puts_by_strike[best_strike]["instrument_name"],
        strike=best_strike,
        expiry_ts_ms=best_expiry_ms,
        spot_price=spot,
        dte=round(actual_dte, 2),
    )

    log.info(
        "straddle_selected",
        call=result.call_name, put=result.put_name,
        strike=result.strike, spot=result.spot_price,
        dte=result.dte, itm_depth=round(spot - best_strike, 2),
    )
    return result
