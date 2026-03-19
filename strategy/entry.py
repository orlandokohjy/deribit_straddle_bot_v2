from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from config.settings import Settings
from core.client import DeribitClient, DeribitClientError
from strategy.instrument_selector import StraddleInstruments
from utils.helpers import utcnow

log = structlog.get_logger("strategy.entry")

_OPTION_TICK_SIZE = 0.0005


class EntryCapBreached(Exception):
    """Raised when a leg's fill price exceeds the allowed cap above cached ask."""

    def __init__(
        self,
        leg: str,
        fill_price: float,
        cached_ask: float,
        cap_pct: float,
        call_leg: "FilledLeg | None" = None,
        put_leg: "FilledLeg | None" = None,
    ) -> None:
        self.leg = leg
        self.fill_price = fill_price
        self.cached_ask = cached_ask
        self.cap_pct = cap_pct
        self.call_leg = call_leg
        self.put_leg = put_leg
        super().__init__(
            f"{leg} fill {fill_price:.6f} > cap {cached_ask * (1 + cap_pct):.6f} "
            f"(cached_ask={cached_ask:.6f}, cap={cap_pct:.0%})"
        )


@dataclass
class FilledLeg:
    instrument_name: str
    order_id: str
    direction: str
    amount: float
    average_price: float
    filled_amount: float
    label: str


@dataclass
class StraddleEntry:
    call_leg: FilledLeg
    put_leg: FilledLeg
    entry_timestamp: str
    total_premium_btc: float
    per_contract_premium: float


def _round_to_tick(price: float) -> float:
    return round(round(price / _OPTION_TICK_SIZE) * _OPTION_TICK_SIZE, 4)


def _build_sweep_params(instrument_name: str, amount: float, cached_ask: float, label: str) -> dict[str, Any]:
    sweep_price = _round_to_tick(cached_ask + _OPTION_TICK_SIZE)
    return {
        "instrument_name": instrument_name,
        "amount": amount,
        "type": "limit",
        "price": sweep_price,
        "time_in_force": "immediate_or_cancel",
        "label": label,
    }


def _parse_fill(order: dict[str, Any]) -> tuple[float, float]:
    """Return (filled_amount, average_price) from an order result."""
    return float(order.get("filled_amount", 0)), float(order.get("average_price", 0))


def _fill_legs_parallel(
    client: DeribitClient,
    call_name: str,
    put_name: str,
    amount: float,
    call_label: str,
    put_label: str,
    settings: Settings,
    cached_call_ask: float,
    cached_put_ask: float,
) -> tuple[FilledLeg, FilledLeg]:
    """Fill both legs in parallel: IOC sweep, then parallel market fallback for remainders."""
    call_sweep_price = _round_to_tick(cached_call_ask + _OPTION_TICK_SIZE)
    put_sweep_price = _round_to_tick(cached_put_ask + _OPTION_TICK_SIZE)

    log.info("parallel_sweep",
             call=call_name, put=put_name, amount=amount,
             call_ask=cached_call_ask, call_sweep=call_sweep_price,
             put_ask=cached_put_ask, put_sweep=put_sweep_price)

    call_result, put_result = client.parallel(
        ("private/buy", _build_sweep_params(call_name, amount, cached_call_ask, call_label)),
        ("private/buy", _build_sweep_params(put_name, amount, cached_put_ask, put_label)),
    )

    call_filled, call_avg = _parse_fill(call_result["order"])
    put_filled, put_avg = _parse_fill(put_result["order"])

    log.info("sweep_results",
             call_filled=call_filled, call_avg=round(call_avg, 6),
             put_filled=put_filled, put_avg=round(put_avg, 6))

    call_remaining = amount - call_filled
    put_remaining = amount - put_filled

    if (call_remaining > 0 or put_remaining > 0) and settings.allow_market_fallback:
        if call_remaining > 0:
            try:
                mkt_call_result = client.private("buy", {
                    "instrument_name": call_name, "amount": call_remaining,
                    "type": "market", "label": call_label,
                })
                mkt_call_filled, mkt_call_avg = _parse_fill(mkt_call_result["order"])
                total_call = call_filled + mkt_call_filled
                call_avg = ((call_avg * call_filled) + (mkt_call_avg * mkt_call_filled)) / total_call if total_call > 0 else 0
                call_filled = total_call
            except DeribitClientError as exc:
                log.warning("call_market_fallback_failed", instrument=call_name, error=str(exc))

        if put_remaining > 0:
            try:
                mkt_put_result = client.private("buy", {
                    "instrument_name": put_name, "amount": put_remaining,
                    "type": "market", "label": put_label,
                })
                mkt_put_filled, mkt_put_avg = _parse_fill(mkt_put_result["order"])
                total_put = put_filled + mkt_put_filled
                put_avg = ((put_avg * put_filled) + (mkt_put_avg * mkt_put_filled)) / total_put if total_put > 0 else 0
                put_filled = total_put
            except DeribitClientError as exc:
                log.warning("put_market_fallback_failed", instrument=put_name, error=str(exc))

    log.info("parallel_fill_done",
             call_filled=call_filled, call_vwap=round(call_avg, 6),
             put_filled=put_filled, put_vwap=round(put_avg, 6))

    if call_filled < amount:
        log.warning("incomplete_fill", instrument=call_name, requested=amount, filled=call_filled)
        try:
            client.private("cancel_all_by_instrument", {"instrument_name": call_name, "type": "all"})
        except DeribitClientError:
            pass
    if put_filled < amount:
        log.warning("incomplete_fill", instrument=put_name, requested=amount, filled=put_filled)
        try:
            client.private("cancel_all_by_instrument", {"instrument_name": put_name, "type": "all"})
        except DeribitClientError:
            pass

    call_leg = FilledLeg(
        instrument_name=call_name, order_id=call_result["order"].get("order_id", ""),
        direction="buy", amount=call_filled, average_price=round(call_avg, 6),
        filled_amount=call_filled, label=call_label,
    )
    put_leg = FilledLeg(
        instrument_name=put_name, order_id=put_result["order"].get("order_id", ""),
        direction="buy", amount=put_filled, average_price=round(put_avg, 6),
        filled_amount=put_filled, label=put_label,
    )
    return call_leg, put_leg


def check_premium_cap(
    call_leg: FilledLeg,
    put_leg: FilledLeg,
    cached_call_ask: float,
    cached_put_ask: float,
    cap_pct: float,
) -> None:
    """Raise EntryCapBreached if either leg's fill price exceeds the cap."""
    call_cap = cached_call_ask * (1 + cap_pct)
    put_cap = cached_put_ask * (1 + cap_pct)

    if call_leg.average_price > call_cap:
        log.warning("entry_cap_breached",
                     leg="call", fill=call_leg.average_price,
                     cached=cached_call_ask, cap=round(call_cap, 6))
        raise EntryCapBreached("call", call_leg.average_price, cached_call_ask, cap_pct,
                                call_leg=call_leg, put_leg=put_leg)

    if put_leg.average_price > put_cap:
        log.warning("entry_cap_breached",
                     leg="put", fill=put_leg.average_price,
                     cached=cached_put_ask, cap=round(put_cap, 6))
        raise EntryCapBreached("put", put_leg.average_price, cached_put_ask, cap_pct,
                                call_leg=call_leg, put_leg=put_leg)

    log.info("premium_cap_ok",
             call_fill=call_leg.average_price, call_cap=round(call_cap, 6),
             put_fill=put_leg.average_price, put_cap=round(put_cap, 6))


def close_failed_entry(client: DeribitClient, call_leg: FilledLeg, put_leg: FilledLeg) -> None:
    """Market-sell both legs to unwind a failed entry attempt."""
    date_tag = utcnow().strftime("%Y%m%d")
    sells: list[tuple[str, dict[str, Any] | None]] = []

    if call_leg.filled_amount > 0:
        sells.append(("private/sell", {
            "instrument_name": call_leg.instrument_name,
            "amount": call_leg.filled_amount,
            "type": "market", "reduce_only": "true",
            "label": f"cap-unwind-call-{date_tag}",
        }))
    if put_leg.filled_amount > 0:
        sells.append(("private/sell", {
            "instrument_name": put_leg.instrument_name,
            "amount": put_leg.filled_amount,
            "type": "market", "reduce_only": "true",
            "label": f"cap-unwind-put-{date_tag}",
        }))

    if not sells:
        return

    results = client.parallel(*sells)
    for r in results:
        order = r["order"]
        log.info("cap_unwind_filled",
                 instrument=order["instrument_name"],
                 filled=float(order.get("filled_amount", 0)),
                 avg=round(float(order.get("average_price", 0)), 6))


def _trim_excess(client: DeribitClient, instrument_name: str, excess_qty: float, date_tag: str) -> None:
    try:
        result = client.private("sell", {
            "instrument_name": instrument_name,
            "amount": excess_qty,
            "type": "market",
            "reduce_only": "true",
            "label": f"straddle-trim-{date_tag}",
        })
        log.info("excess_trimmed", instrument=instrument_name, sold=float(result["order"].get("filled_amount", 0)))
    except DeribitClientError as exc:
        log.error("trim_failed", instrument=instrument_name, qty=excess_qty, error=str(exc))
        raise RuntimeError(f"Failed to trim {excess_qty} on {instrument_name}: {exc}")


def enter_straddle(
    client: DeribitClient,
    instruments: StraddleInstruments,
    contracts: int,
    settings: Settings,
    cached_call_ask: float,
    cached_put_ask: float,
) -> StraddleEntry:
    """Enter a long straddle with parallel execution, premium cap check, and leg balancing."""
    date_tag = utcnow().strftime("%Y%m%d")
    amount = float(contracts)

    call_leg, put_leg = _fill_legs_parallel(
        client,
        instruments.call_name, instruments.put_name,
        amount,
        f"straddle-call-{date_tag}", f"straddle-put-{date_tag}",
        settings,
        cached_call_ask, cached_put_ask,
    )

    check_premium_cap(call_leg, put_leg, cached_call_ask, cached_put_ask, settings.entry_cap_pct)

    balanced_qty = min(call_leg.amount, put_leg.amount)
    if balanced_qty <= 0:
        raise RuntimeError(f"Straddle entry failed: call={call_leg.amount}, put={put_leg.amount}")

    if call_leg.amount != put_leg.amount:
        excess_leg = call_leg if call_leg.amount > balanced_qty else put_leg
        excess_qty = excess_leg.amount - balanced_qty
        log.warning("balancing_legs", excess_instrument=excess_leg.instrument_name, excess_qty=excess_qty, balanced_qty=balanced_qty)
        _trim_excess(client, excess_leg.instrument_name, excess_qty, date_tag)
        excess_leg.amount = balanced_qty
        excess_leg.filled_amount = balanced_qty

    per_contract = call_leg.average_price + put_leg.average_price
    total_premium = per_contract * balanced_qty

    entry = StraddleEntry(
        call_leg=call_leg,
        put_leg=put_leg,
        entry_timestamp=utcnow().isoformat(),
        total_premium_btc=round(total_premium, 8),
        per_contract_premium=round(per_contract, 8),
    )

    log.info("straddle_entered", call=call_leg.instrument_name, put=put_leg.instrument_name,
             contracts=balanced_qty, per_contract_premium=entry.per_contract_premium,
             total_premium_btc=entry.total_premium_btc)
    return entry
