from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_ENV_FILE)

_REST_URLS = {
    "test": "https://test.deribit.com/api/v2",
    "prod": "https://www.deribit.com/api/v2",
}

_WS_URLS = {
    "test": "wss://test.deribit.com/ws/api/v2",
    "prod": "wss://www.deribit.com/ws/api/v2",
}


def _bool_env(key: str, default: bool) -> bool:
    raw = os.getenv(key, str(default)).strip().lower()
    return raw in ("1", "true", "yes")


@dataclass(frozen=True)
class Settings:
    client_id: str = field(repr=False, default="")
    client_secret: str = field(repr=False, default="")
    env: str = "test"

    target_dte: int = 7
    dte_tolerance: int = 2

    equity_pct: float = 0.20
    tier1_fraction: float = 0.20
    take_profit_pct: float = 0.50

    exit_hour_utc: int = 18
    exit_minute_utc: int = 0

    entry_cap_pct: float = 0.50
    max_entry_attempts: int = 2

    max_order_retries: int = 3
    allow_market_fallback: bool = True

    @property
    def rest_url(self) -> str:
        return _REST_URLS[self.env]

    @property
    def ws_url(self) -> str:
        return _WS_URLS[self.env]

    def validate(self) -> None:
        if not self.client_id or not self.client_secret:
            raise ValueError("DERIBIT_CLIENT_ID and DERIBIT_CLIENT_SECRET must be set")
        if self.env not in _REST_URLS:
            raise ValueError(f"DERIBIT_ENV must be one of {list(_REST_URLS)}, got '{self.env}'")
        if not 0 < self.equity_pct <= 1:
            raise ValueError(f"EQUITY_PCT must be in (0, 1], got {self.equity_pct}")
        if not 0 < self.tier1_fraction < 1:
            raise ValueError(f"TIER1_FRACTION must be in (0, 1), got {self.tier1_fraction}")
        if self.take_profit_pct <= 0:
            raise ValueError(f"TAKE_PROFIT_PCT must be > 0, got {self.take_profit_pct}")
        if self.entry_cap_pct <= 0:
            raise ValueError(f"ENTRY_CAP_PCT must be > 0, got {self.entry_cap_pct}")
        if self.max_entry_attempts < 1:
            raise ValueError(f"MAX_ENTRY_ATTEMPTS must be >= 1, got {self.max_entry_attempts}")
        if not 0 <= self.exit_hour_utc <= 23:
            raise ValueError(f"EXIT_HOUR_UTC must be 0-23, got {self.exit_hour_utc}")


def load_settings() -> Settings:
    settings = Settings(
        client_id=os.getenv("DERIBIT_CLIENT_ID", ""),
        client_secret=os.getenv("DERIBIT_CLIENT_SECRET", ""),
        env=os.getenv("DERIBIT_ENV", "test").strip().lower(),
        target_dte=int(os.getenv("TARGET_DTE", "7")),
        dte_tolerance=int(os.getenv("DTE_TOLERANCE", "2")),
        equity_pct=float(os.getenv("EQUITY_PCT", "0.20")),
        tier1_fraction=float(os.getenv("TIER1_FRACTION", "0.20")),
        take_profit_pct=float(os.getenv("TAKE_PROFIT_PCT", "0.50")),
        exit_hour_utc=int(os.getenv("EXIT_HOUR_UTC", "18")),
        exit_minute_utc=int(os.getenv("EXIT_MINUTE_UTC", "0")),
        entry_cap_pct=float(os.getenv("ENTRY_CAP_PCT", "0.50")),
        max_entry_attempts=int(os.getenv("MAX_ENTRY_ATTEMPTS", "2")),
        max_order_retries=int(os.getenv("MAX_ORDER_RETRIES", "3")),
        allow_market_fallback=_bool_env("ALLOW_MARKET_FALLBACK", True),
    )
    settings.validate()
    return settings
