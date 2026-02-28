# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**poly-autobetting** is a Python trading bot for Polymarket BTC 15-minute binary prediction markets. It places limit orders on both UP and DOWN outcomes, monitors positions with real-time WebSocket price feeds, and auto-redeems resolved positions.

Two main entry points:
- `scripts/place_45.py` — Simple bot placing 45c limit orders on both sides
- `src/bot/runner.py` — Complex bot with market scheduling, multi-level ladder strategies, position tracking, and risk management

## Architecture

### Layered Structure

**API Layer** (`src/api/`)
- `gamma.py` — Market discovery and metadata (Polymarket's Gamma API)
- `clob.py` — Order book queries and trade data
- `data_api.py` — Historical and real-time market data
- `polygonscan.py` — On-chain data via Polygon blockchain queries

**Bot Layer** (`src/bot/`)
- `runner.py` — Main orchestrator; event loop managing market discovery, session lifecycle, and redemption
- `session_loop.py` — Session primitives; market-by-market trading loop
- `bot_config.py` — Configuration loader from environment variables (BOT_* prefix)
- `types.py` — Shared data types (`MarketInfo`, `PositionState`, `LadderLevel`, `BotConfig`, `MarketPhase`, `EdgeTier`)
- `market_scheduler.py` — Market rotation and phase detection
- `order_engine.py` — Order placement and management (active/passive strategies)
- `fill_monitor.py` — Real-time fill detection (polls CLOB API)
- `position_tracker.py` — Cumulative position and P&L tracking
- `ws_book_feed.py` — WebSocket order book feed for real-time prices
- `client.py` — Wrapper around py-clob-client SDK
- `risk_engine.py` — Risk checks, circuit breakers, loss limits
- `state_manager.py` — Persistence of bot state to disk
- `math_engine.py` — Pricing utilities (VWAP, edge calculation, fair value)
- `rebalance.py` — Position rebalancing logic
- `alerts.py` — Alert/notification handling
- `backtest.py` — Backtesting framework

**Analysis Layer** (`src/analysis/`)
- `strategy.py` — Strategy definitions and evaluation
- `calculator.py` — Trade P&L and performance metrics
- `activity_watch.py` — Trade monitoring and activity tracking

**Monitor Module** (`src/monitor/`)
- `orderbook.py` — Order book monitoring
- `spreads.py` — Bid-ask spread analysis

### Core Concepts

**Market Phase Lifecycle**
Markets transition through phases: `WAITING` → `DISCOVERED` → `PRE_ENTRY` → `ACTIVE` → `WINDING_DOWN` → `CLOSED` → `RESOLVED` → `SETTLED`

**Position Tracking**
`PositionState` tracks cumulative positions:
- `up_shares` / `down_shares` — Share counts
- `up_cost` / `down_cost` — USDC spent
- `up_vwap` / `down_vwap` — Volume-weighted average price per side
- `combined_vwap` — Sum of both sides (fair value entry cost)
- `hedged_profit` — Profit from matched pairs: `hedged_shares × (1 - combined_vwap)`
- `excess_shares` — Unhedged imbalance

**Order Ladder**
`LadderLevel` represents a price tier in the ladder strategy:
- `price_cents` — Price level (e.g., 45 = $0.45)
- `target_shares` — Goal shares at this level
- `filled_shares` / `replenish_count` — Tracking fills and replenishments

**Edge Classification**
Orders are evaluated by edge tier:
- `WIDE` — >5¢ edge (strong opportunity)
- `MEDIUM` — 2-5¢ edge
- `TIGHT` — 0.5-2¢ edge
- `NEGATIVE` — <0 edge (skip)

## Development Setup

### Prerequisites
- Python 3.10+
- Polymarket account with CLOB API credentials

### Installation
```bash
python3 -m venv venv
source venv/bin/activate
pip install httpx python-dotenv py-clob-client py-builder-relayer-client web3 eth-abi
```

### Configuration
```bash
cp .env.example .env
```

Set environment variables in `.env`:
- `POLYMARKET_PRIVATE_KEY` — Wallet private key
- `POLYMARKET_FUNDER` — Funder/proxy address (optional)
- `POLYMARKET_API_KEY` / `POLYMARKET_API_SECRET` / `POLYMARKET_PASSPHRASE` — CLOB API creds
- `POLYMARKET_BUILDER_*` — Builder relayer credentials for gasless redemptions (optional)

Bot configuration uses `BOT_*` environment variables (e.g., `BOT_SESSION_CAPITAL_LIMIT`, `BOT_SHARES_PER_ORDER`). See `src/bot/bot_config.py` for all supported parameters and defaults.

## Common Development Tasks

### Run the Simple 45c Bot
```bash
python scripts/place_45.py
```
Places 45c limit orders on both UP/DOWN every 15 minutes, monitors with WebSocket, auto-redeems via relayer.

### Run the Full Market-Making Bot
```bash
python -c "from src.bot.runner import main; main()"
```
Or set entry point in your runner. Reads `BOT_*` config variables. Use `--help` for CLI options (if implemented).

### Backtest a Strategy
```bash
python -m src.bot.backtest [options]
```
Runs backtesting on historical data. Check `src/bot/backtest.py` for available parameters.

### Test Configuration Loading
```bash
python -c "from src.bot.bot_config import load_bot_config; cfg = load_bot_config({'shares_per_order': 1}); print(cfg)"
```

### Debug Market Discovery
```bash
python -c "from src.api.gamma import get_btc_15m_markets; import asyncio; asyncio.run(get_btc_15m_markets())"
```

### Monitor Live Order Book
Start a monitoring session using `src/monitor/orderbook.py` and `src/monitor/spreads.py` directly or via custom runner.

### Check Position State
```bash
python -c "from src.bot.state_manager import StateManager; sm = StateManager(); print(sm.load())"
```

## Key Code Patterns

### Configuration Precedence
1. Environment variables (`BOT_*` prefix)
2. CLI argument overrides (if supported)
3. Built-in defaults in `BotConfig`
4. Auto-scaling adjusts risk limits based on `session_capital_limit` (see `auto_scale_thresholds()`)

### Order Placement Flow
1. `order_engine.py` builds ladder levels based on strategy
2. Orders are created and signed by py-clob-client
3. Posted to CLOB API with GTD expiration
4. `fill_monitor.py` polls for fills and updates `PositionState`
5. `position_tracker.py` tracks cumulative P&L

### WebSocket Feed Pattern
`ws_book_feed.py` maintains a persistent WebSocket connection:
- Subscribe to token IDs
- Receive real-time bid/ask updates
- Lookup best prices via `get_best_bid(token_id)` / `get_best_ask(token_id)`
- Auto-reconnect on disconnect

### Risk Management
`risk_engine.py` enforces:
- Daily/hourly loss limits
- Worst-case PNL limits
- Unhedged exposure limits
- Imbalance thresholds

Triggered before order placement; blocks if limits exceeded.

### State Persistence
`state_manager.py` saves/loads bot state to disk:
- Position tracking
- Market metadata
- Order history
- Market phase info

Enables recovery on restart.

## Code Navigation

**Entry points:**
- `scripts/place_45.py` — Simple bot
- `src/bot/runner.py` — Main orchestrator

**Market lifecycle:**
- `market_scheduler.py` → detects new markets, rotation
- `session_loop.py` → per-market trading loop
- `order_engine.py` → places orders

**Monitoring:**
- `fill_monitor.py` → checks fills
- `position_tracker.py` → tracks P&L
- `ws_book_feed.py` → real-time prices

**Risk & Config:**
- `risk_engine.py` → enforces limits
- `bot_config.py` → loads configuration
- `state_manager.py` → persistence

**Analysis:**
- `analysis/strategy.py` → strategy evaluation
- `analysis/calculator.py` → P&L metrics
- `monitor/orderbook.py` → order book monitoring

## Testing & Debugging

- **Backtesting**: Use `src/bot/backtest.py` to evaluate strategies on historical data
- **Dry-run**: Set `session_capital_limit=5` and `shares_per_order=1` for minimal stake testing
- **Logging**: Set `logging.basicConfig(level=logging.DEBUG)` for verbose output
- **Relayer debugging**: Check builder relayer API responses for redemption issues

## Dependencies

- `httpx` — Async HTTP client
- `python-dotenv` — Environment configuration
- `py-clob-client` — Polymarket CLOB SDK
- `py-builder-relayer-client` — Gasless relayer for redemptions
- `web3` — Ethereum/Polygon interactions
- `eth-abi` — Encoding contract calls

## Important Notes

- **Private key security**: Never commit `.env` with real credentials; use `.env.example` template
- **Order expiration**: Default 1 hour for active orders, 5 minutes for exit orders
- **WebSocket reconnection**: Auto-reconnect with exponential backoff
- **Redemption**: Requires builder relayer credentials for gasless execution; manual redemption possible without
- **15-minute markets**: Bot is optimized for BTC 15m binary markets; adaptation needed for other markets
