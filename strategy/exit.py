from __future__ import annotations

import time
from typing import Any

import structlog

from config.settings import Settings
from core.client import DeribitClient, DeribitClientError
from utils.helpers import utcnow

log = structlog.get_logger("strategy.exit")


def close_tier1(
    client: DeribitClient,
    call_instrument: str,
    put_instrument: str,
    tier1_qty: float,
) -> None:
    """Market-sell Tier 1 contracts on both legs simultaneously."""
    date_tag = utcnow().strftime("%Y%m%d")

    log.info("closing_tier1", call=call_instrument, put=put_instrument, qty=tier1_qty)

    call_result, put_result = client.parallel(
        ("private/sell", {
            "instrument_name": call_instrument,
            "amount": tier1_qty,
            "type": "market",
            "reduce_only": "true",
            "label": f"tier1-exit-call-{date_tag}",
        }),
        ("private/sell", {
            "instrument_name": put_instrument,
            "amount": tier1_qty,
            "type": "market",
            "reduce_only": "true",
            "label": f"tier1-exit-put-{date_tag}",
        }),
    )

    call_filled = float(call_result["order"].get("filled_amount", 0))
    put_filled = float(put_result["order"].get("filled_amount", 0))
    call_avg = float(call_result["order"].get("average_price", 0))
    put_avg = float(put_result["order"].get("average_price", 0))

    log.info(
        "tier1_closed",
        call_filled=call_filled, call_avg=round(call_avg, 6),
        put_filled=put_filled, put_avg=round(put_avg, 6),
        exit_premium=round(call_avg + put_avg, 6),
    )


def _cancel_all_for_instrument(client: DeribitClient, instrument_name: str) -> int:
    try:
        result = client.private("cancel_all_by_instrument", {
            "instrument_name": instrument_name,
            "type": "all",
        })
        count = result if isinstance(result, int) else 0
        log.info("orders_cancelled", instrument=instrument_name, count=count)
        return count
    except DeribitClientError as exc:
        log.warning("cancel_orders_failed", instrument=instrument_name, error=str(exc))
        return 0


def _round_amount(amount: float) -> float:
    """Round to 1 decimal place (Deribit BTC option minimum increment is 0.1)."""
    return round(amount, 1)


def _close_leg(
    client: DeribitClient,
    instrument_name: str,
    amount: float,
    settings: Settings,
    label: str,
) -> None:
    """Close a position leg with aggressive limit sell + market fallback."""
    remaining = _round_amount(amount)

    for attempt in range(1, settings.max_order_retries + 1):
        book = client.public("get_order_book", {"instrument_name": instrument_name, "depth": "1"})
        best_bid = book.get("best_bid_price")
        if best_bid is None or best_bid <= 0:
            log.warning("no_bid_for_close", instrument=instrument_name, attempt=attempt)
            if attempt < settings.max_order_retries:
                time.sleep(2.0)
                continue
            break

        try:
            result = client.private("sell", {
                "instrument_name": instrument_name,
                "amount": remaining,
                "type": "limit",
                "price": best_bid,
                "time_in_force": "immediate_or_cancel",
                "reduce_only": "true",
                "label": label,
            })
        except DeribitClientError as exc:
            log.warning("sell_rejected", instrument=instrument_name, error=str(exc))
            if attempt < settings.max_order_retries:
                time.sleep(2.0)
                continue
            raise

        filled = float(result["order"].get("filled_amount", 0))
        if filled >= remaining:
            log.info("leg_closed", instrument=instrument_name)
            return
        remaining = _round_amount(remaining - filled)
        log.info("partial_close_fill", instrument=instrument_name, filled=filled, remaining=remaining)
        if attempt < settings.max_order_retries:
            time.sleep(2.0)

    if settings.allow_market_fallback:
        log.warning("market_close_fallback", instrument=instrument_name, remaining=remaining)
        client.private("sell", {
            "instrument_name": instrument_name,
            "amount": remaining,
            "type": "market",
            "reduce_only": "true",
            "label": label,
        })
        log.info("leg_closed_market", instrument=instrument_name)
        return

    raise RuntimeError(f"Failed to close {instrument_name}, remaining={remaining}")


def close_all(client: DeribitClient, settings: Settings) -> None:
    """Cancel all open orders and close every open BTC option position."""
    positions = client.private("get_positions", {"currency": "BTC", "kind": "option"})
    open_positions = [p for p in positions if float(p.get("size", 0)) != 0]

    if not open_positions:
        log.info("no_open_positions")
        return

    date_tag = utcnow().strftime("%Y%m%d")

    for pos in open_positions:
        _cancel_all_for_instrument(client, pos["instrument_name"])

    for pos in open_positions:
        inst = pos["instrument_name"]
        size = abs(float(pos["size"]))
        pnl = float(pos.get("floating_profit_loss", 0))
        log.info("closing_position", instrument=inst, size=size, pnl=round(pnl, 8))
        _close_leg(client, inst, size, settings, f"tier2-exit-{date_tag}")

    log.info("all_positions_closed")


def get_status(client: DeribitClient) -> dict[str, Any]:
    positions = client.private("get_positions", {"currency": "BTC", "kind": "option"})
    open_positions = [p for p in positions if float(p.get("size", 0)) != 0]
    open_orders = client.private("get_open_orders", {"currency": "BTC", "kind": "option"})
    return {
        "positions": open_positions,
        "open_orders": open_orders if isinstance(open_orders, list) else [],
    }
