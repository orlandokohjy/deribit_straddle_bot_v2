from __future__ import annotations

import math
from dataclasses import dataclass

import structlog

from config.settings import Settings
from core.client import DeribitClient
from strategy.instrument_selector import StraddleInstruments

log = structlog.get_logger("strategy.position_sizer")


@dataclass
class SizingResult:
    contracts: int
    tier1_contracts: int
    tier2_contracts: int
    equity_btc: float
    call_ask: float
    put_ask: float


def compute_size(
    client: DeribitClient,
    instruments: StraddleInstruments,
    settings: Settings,
) -> SizingResult:
    """Compute total contracts and tier split. Fetches equity + books in parallel."""
    equity_result, call_book, put_book = client.parallel(
        ("private/get_account_summary", {"currency": "BTC", "extended": "false"}),
        ("public/get_order_book", {"instrument_name": instruments.call_name, "depth": "1"}),
        ("public/get_order_book", {"instrument_name": instruments.put_name, "depth": "1"}),
    )

    equity_btc = float(equity_result["equity"])
    call_ask = float(call_book["best_ask_price"])
    put_ask = float(put_book["best_ask_price"])

    if call_ask <= 0 or put_ask <= 0:
        raise RuntimeError(f"Invalid ask prices: call={call_ask}, put={put_ask}")

    allocated_btc = equity_btc * settings.equity_pct
    total_premium = call_ask + put_ask
    contracts = max(1, math.floor(allocated_btc / total_premium))

    tier1 = max(1, math.floor(contracts * settings.tier1_fraction))
    tier2 = contracts - tier1

    log.info(
        "position_sized",
        equity_btc=round(equity_btc, 6),
        allocated_btc=round(allocated_btc, 6),
        call_ask=call_ask,
        put_ask=put_ask,
        total_premium=round(total_premium, 6),
        contracts=contracts,
        tier1=tier1,
        tier2=tier2,
    )

    return SizingResult(
        contracts=contracts,
        tier1_contracts=tier1,
        tier2_contracts=tier2,
        equity_btc=equity_btc,
        call_ask=call_ask,
        put_ask=put_ask,
    )
