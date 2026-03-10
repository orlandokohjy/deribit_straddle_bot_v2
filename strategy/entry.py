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


def _fill_leg(
    client: DeribitClient,
    instrument_name: str,
    amount: float,
    label: str,
    settings: Settings,
    cached_ask: float | None = None,
) -> FilledLeg:
    """Aggressive IOC limit sweep at best_ask + 1 tick, then market fallback."""
    if cached_ask is not None and cached_ask > 0:
        best_ask = cached_ask
    else:
        book = client.public("get_order_book", {"instrument_name": instrument_name, "depth": "1"})
        best_ask = float(book.get("best_ask_price", 0))
        if best_ask <= 0:
            raise RuntimeError(f"No valid ask for {instrument_name}")

    sweep_price = _round_to_tick(best_ask + _OPTION_TICK_SIZE)
    log.info("sweep_limit", instrument=instrument_name, amount=amount, best_ask=best_ask, sweep_price=sweep_price)

    try:
        result = client.private("buy", {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": "limit",
            "price": sweep_price,
            "time_in_force": "immediate_or_cancel",
            "label": label,
        })
    except DeribitClientError as exc:
        log.warning("sweep_rejected", instrument=instrument_name, error=str(exc))
        if not settings.allow_market_fallback:
            raise
        result = {"order": {"filled_amount": 0, "order_id": "none", "average_price": 0}}

    order = result["order"]
    filled = float(order.get("filled_amount", 0))

    if filled >= amount:
        avg = float(order.get("average_price", sweep_price))
        log.info("leg_filled_sweep", instrument=instrument_name, filled=filled, avg_price=avg)
        return FilledLeg(instrument_name=instrument_name, order_id=order["order_id"],
                         direction="buy", amount=amount, average_price=avg,
                         filled_amount=filled, label=label)

    remaining = amount - filled
    log.info("sweep_partial", instrument=instrument_name, filled=filled, remaining=remaining)

    if not settings.allow_market_fallback:
        raise RuntimeError(f"Partial fill {instrument_name}: filled={filled}, remaining={remaining}")

    mkt_result = client.private("buy", {
        "instrument_name": instrument_name,
        "amount": remaining,
        "type": "market",
        "label": label,
    })
    mkt_order = mkt_result["order"]
    mkt_filled = float(mkt_order.get("filled_amount", 0))
    mkt_avg = float(mkt_order.get("average_price", 0))

    sweep_avg = float(order.get("average_price", 0))
    total_filled = filled + mkt_filled
    vwap = ((sweep_avg * filled) + (mkt_avg * mkt_filled)) / total_filled if total_filled > 0 else 0

    log.info("leg_filled_market", instrument=instrument_name, sweep_filled=filled, mkt_filled=mkt_filled, vwap=round(vwap, 6))

    if total_filled < amount:
        log.warning("incomplete_fill", instrument=instrument_name, requested=amount, filled=total_filled)
        try:
            client.private("cancel_all_by_instrument", {"instrument_name": instrument_name, "type": "all"})
        except DeribitClientError:
            pass

    return FilledLeg(instrument_name=instrument_name, order_id=mkt_order["order_id"],
                     direction="buy", amount=total_filled, average_price=round(vwap, 6),
                     filled_amount=total_filled, label=label)


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
    cached_call_ask: float | None = None,
    cached_put_ask: float | None = None,
) -> StraddleEntry:
    """Enter a long straddle: buy call + buy put at the same strike."""
    date_tag = utcnow().strftime("%Y%m%d")
    amount = float(contracts)

    call_leg = _fill_leg(client, instruments.call_name, amount, f"straddle-call-{date_tag}", settings, cached_ask=cached_call_ask)
    log.info("call_leg_done", order_id=call_leg.order_id, avg_price=call_leg.average_price)

    try:
        put_leg = _fill_leg(client, instruments.put_name, amount, f"straddle-put-{date_tag}", settings, cached_ask=cached_put_ask)
    except Exception:
        log.error("put_leg_failed_after_call_filled", call_order_id=call_leg.order_id)
        raise

    log.info("put_leg_done", order_id=put_leg.order_id, avg_price=put_leg.average_price)

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
