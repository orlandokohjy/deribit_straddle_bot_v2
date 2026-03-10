# Deribit BTC Long Straddle Bot v2 — Tiered Exit

Automated BTC long straddle with tiered exit strategy on Deribit.

## Strategy

1. **Entry** — At run time, allocate 20% of account equity to buy N contracts of
   a BTC call + put at the same strike (call slightly ITM).
2. **Tier 1 (1/5 of N)** — Take profit when the *combined* straddle premium
   (call mark + put mark) rises 50% above entry. Both legs are closed simultaneously.
3. **Tier 2 (4/5 of N)** — Close all remaining contracts at 18:00 UTC regardless of PnL.

## Architecture

- **REST** for all order execution (entry, exits) — proven fast (~2s entry).
- **WebSocket** for real-time mark-price monitoring of the combined TP condition.
- Single process: enters, monitors, exits, then shuts down.

## Usage

```bash
cd deribit_straddle_bot_v2
pip install -r requirements.txt
cp .env.example .env   # fill in credentials

python main.py run       # full lifecycle: enter → monitor → exit
python main.py dry-run   # select instruments + size, no orders
python main.py status    # show current positions and open orders
python main.py close     # emergency: cancel all orders + close all positions
```

## Configuration (.env)

| Variable | Default | Description |
|---|---|---|
| `DERIBIT_CLIENT_ID` | — | API key |
| `DERIBIT_CLIENT_SECRET` | — | API secret |
| `DERIBIT_ENV` | `test` | `test` or `prod` |
| `TARGET_DTE` | `0` | Target days to expiry (0 = nearest/0DTE) |
| `DTE_TOLERANCE` | `1` | DTE search window ± |
| `EQUITY_PCT` | `0.20` | Fraction of equity to allocate |
| `TIER1_FRACTION` | `0.20` | Fraction of contracts for Tier 1 (1/5) |
| `TAKE_PROFIT_PCT` | `0.50` | Combined premium TP threshold |
| `EXIT_HOUR_UTC` | `18` | Tier 2 close hour (UTC) |
| `EXIT_MINUTE_UTC` | `0` | Tier 2 close minute (UTC) |
| `MAX_ORDER_RETRIES` | `3` | Retries for partial fills |
| `ALLOW_MARKET_FALLBACK` | `true` | Fall back to market orders |
