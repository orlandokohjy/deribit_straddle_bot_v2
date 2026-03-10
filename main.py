from __future__ import annotations

import argparse
import sys

import structlog

from config.settings import load_settings
from core.client import DeribitClient
from strategy.exit import close_all, get_status
from strategy.orchestrator import StraddleOrchestrator
from utils.logger import setup_logging

log = structlog.get_logger("main")


def cmd_run() -> None:
    """Full lifecycle: enter -> monitor -> tiered exit."""
    settings = load_settings()
    orchestrator = StraddleOrchestrator(settings)
    orchestrator.run()


def cmd_dry_run() -> None:
    """Select instruments and size without placing orders."""
    settings = load_settings()
    orchestrator = StraddleOrchestrator(settings)
    orchestrator.dry_run()


def cmd_status() -> None:
    """Show current positions and open orders."""
    settings = load_settings()
    with DeribitClient(settings) as client:
        status = get_status(client)

    positions = status["positions"]
    open_orders = status["open_orders"]

    if not positions:
        log.info("status", positions=0, open_orders=len(open_orders))
        print("\nNo open positions.")
    else:
        log.info("status", positions=len(positions), open_orders=len(open_orders))
        print(f"\n{'Instrument':<30} {'Size':>8} {'Direction':<6} {'PnL (BTC)':>12}")
        print("-" * 60)
        for p in positions:
            print(
                f"{p['instrument_name']:<30} "
                f"{p.get('size', 0):>8.1f} "
                f"{p.get('direction', '?'):<6} "
                f"{p.get('floating_profit_loss', 0):>12.6f}"
            )

    if open_orders:
        print(f"\n{'Order ID':<20} {'Instrument':<30} {'Type':<8} {'Price':>10} {'Amount':>8} {'Label'}")
        print("-" * 95)
        for o in open_orders:
            print(
                f"{o.get('order_id', '?'):<20} "
                f"{o.get('instrument_name', '?'):<30} "
                f"{o.get('order_type', '?'):<8} "
                f"{o.get('price', 0):>10.4f} "
                f"{o.get('amount', 0):>8.1f} "
                f"{o.get('label', '')}"
            )
    else:
        print("No open orders.")


def cmd_close() -> None:
    """Emergency: cancel all orders and close all positions now."""
    settings = load_settings()
    with DeribitClient(settings) as client:
        close_all(client, settings)
    log.info("emergency_close_complete")


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="Deribit BTC Tiered Straddle Bot v2")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Full lifecycle: enter -> monitor -> tiered exit")
    sub.add_parser("dry-run", help="Select + size without placing orders")
    sub.add_parser("status", help="Show positions and open orders")
    sub.add_parser("close", help="Emergency: cancel all and close positions now")

    args = parser.parse_args()

    commands = {
        "run": cmd_run,
        "dry-run": cmd_dry_run,
        "status": cmd_status,
        "close": cmd_close,
    }

    try:
        commands[args.command]()
    except KeyboardInterrupt:
        log.info("interrupted")
        sys.exit(0)
    except Exception:
        log.exception("command_failed", command=args.command)
        sys.exit(1)


if __name__ == "__main__":
    main()
