"""Backtest framework: replay recorded order book data through bot logic.

Architecture:
- ReplayClient (ExchangeClient): replays recorded order books, simulates fills
- BacktestRunner: drives a strategy against recorded data, collects results
- record_books(): utility to record live book snapshots to JSON for replay

Data format:
  [{"ts": float, "up_book": {"bids": [...], "asks": [...]}, "down_book": {...}}, ...]
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.bot.types import BotConfig, FillEvent, LadderLevel, PositionState
from src.bot.client import ExchangeClient

logger = logging.getLogger(__name__)


class ReplayClient(ExchangeClient):
    """Replays recorded order book data and simulates fills.

    Implements ExchangeClient ABC so it can be passed to OrderEngine, etc.
    Fill simulation: when a buy order price >= best_ask at snapshot time,
    the order fills immediately.
    """

    def __init__(
        self,
        snapshots: List[Dict],
        up_token: str = "up_token",
        down_token: str = "down_token",
        condition_id: str = "backtest_condition",
        tick_size: float = 0.01,
    ) -> None:
        self._snapshots = sorted(snapshots, key=lambda s: s.get("ts", 0))
        self._up_token = up_token
        self._down_token = down_token
        self._condition_id = condition_id
        self._tick_size = tick_size
        self._snap_idx = 0
        self._sim_time = self._snapshots[0]["ts"] if self._snapshots else time.time()

        # Virtual state
        self._orders: Dict[str, Dict] = {}  # order_id -> order details
        self._fills: List[FillEvent] = []
        self._balance = 10000.0
        self._position: Dict[str, float] = {}  # token_id -> shares

    @property
    def sim_time(self) -> float:
        return self._sim_time

    @property
    def fills(self) -> List[FillEvent]:
        return list(self._fills)

    def advance_to(self, ts: float) -> None:
        """Advance simulation time and check for fills at new book state."""
        self._sim_time = ts
        while (
            self._snap_idx < len(self._snapshots) - 1
            and self._snapshots[self._snap_idx + 1]["ts"] <= ts
        ):
            self._snap_idx += 1
        self._check_fills()

    def _current_snapshot(self) -> Dict:
        if not self._snapshots:
            return {"up_book": {"bids": [], "asks": []}, "down_book": {"bids": [], "asks": []}}
        return self._snapshots[min(self._snap_idx, len(self._snapshots) - 1)]

    def _book_for_token(self, token_id: str) -> Dict:
        snap = self._current_snapshot()
        if token_id == self._up_token:
            return snap.get("up_book", {"bids": [], "asks": []})
        elif token_id == self._down_token:
            return snap.get("down_book", {"bids": [], "asks": []})
        return {"bids": [], "asks": []}

    def _best_ask(self, token_id: str) -> float:
        book = self._book_for_token(token_id)
        asks = book.get("asks", [])
        if not asks:
            return 1.0
        return min(float(a.get("price", 1.0)) for a in asks)

    def _best_bid(self, token_id: str) -> float:
        book = self._book_for_token(token_id)
        bids = book.get("bids", [])
        if not bids:
            return 0.0
        return max(float(b.get("price", 0.0)) for b in bids)

    def _check_fills(self) -> None:
        """Check if any open orders should fill at current book state."""
        to_fill = []
        for oid, order in self._orders.items():
            if order["status"] != "LIVE":
                continue
            token_id = order["token_id"]
            price = order["price"]
            size = order["size"]
            is_sell = order.get("sell", False)

            if is_sell:
                # Sell fills when price <= best_bid
                best_bid = self._best_bid(token_id)
                if price <= best_bid:
                    to_fill.append(oid)
            else:
                # Buy fills when price >= best_ask
                best_ask = self._best_ask(token_id)
                if price >= best_ask:
                    to_fill.append(oid)

        for oid in to_fill:
            order = self._orders[oid]
            order["status"] = "MATCHED"
            order["size_matched"] = order["size"]

            side = "up" if order["token_id"] == self._up_token else "down"
            is_sell = order.get("sell", False)

            if is_sell:
                shares = -order["size"]
                cost = -order["price"] * order["size"]
            else:
                shares = order["size"]
                cost = order["price"] * order["size"]

            fill = FillEvent(
                trade_id=f"bt-{uuid.uuid4().hex[:8]}",
                order_id=oid,
                side=side,
                price=order["price"],
                filled_shares=shares,
                usdc_cost=cost,
                trader_side="MAKER",
                timestamp=int(self._sim_time),
            )
            self._fills.append(fill)
            self._position[order["token_id"]] = (
                self._position.get(order["token_id"], 0) + shares
            )
            self._balance -= cost

    async def place_limit_order(
        self, token_id: str, price: float, size: float, side: str, expiration_ts: int,
    ) -> str:
        oid = f"bt-order-{uuid.uuid4().hex[:8]}"
        self._orders[oid] = {
            "id": oid,
            "token_id": token_id,
            "price": price,
            "size": size,
            "original_size": size,
            "size_matched": 0.0,
            "side": side,
            "status": "LIVE",
            "sell": False,
        }
        self._balance -= price * size  # reserve
        self._check_fills()
        return oid

    async def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            order = self._orders[order_id]
            if order["status"] == "LIVE":
                order["status"] = "CANCELLED"
                # Release reservation
                remaining = order["size"] - order["size_matched"]
                if remaining > 0 and not order.get("sell"):
                    self._balance += order["price"] * remaining
            return True
        return False

    async def cancel_orders(self, order_ids: List[str]) -> bool:
        for oid in order_ids:
            await self.cancel_order(oid)
        return True

    async def cancel_all(self) -> bool:
        for oid in list(self._orders.keys()):
            await self.cancel_order(oid)
        return True

    async def cancel_market_orders(self, asset_id: str) -> bool:
        for oid, order in list(self._orders.items()):
            if order["token_id"] == asset_id and order["status"] == "LIVE":
                await self.cancel_order(oid)
        return True

    async def get_trades(self, asset_id: str, after_ts: int, maker_address: str = "") -> List[FillEvent]:
        return [f for f in self._fills if f.timestamp >= after_ts]

    async def get_open_orders(self, identifier: str) -> List[Dict]:
        return [
            o for o in self._orders.values()
            if o["status"] == "LIVE"
        ]

    async def get_order(self, order_id: str) -> Dict:
        return self._orders.get(order_id, {})

    async def get_order_book(self, token_id: str) -> Dict:
        return self._book_for_token(token_id)

    async def get_balance(self) -> float:
        return self._balance

    async def get_server_time(self) -> float:
        return self._sim_time

    async def get_min_order_size(self, token_id: str) -> float:
        return 5.0

    async def place_taker_order(
        self, token_id: str, price: float, size: float, side: str,
        expiration_ts: int, *, sell: bool = False,
    ) -> str:
        oid = f"bt-taker-{uuid.uuid4().hex[:8]}"
        self._orders[oid] = {
            "id": oid,
            "token_id": token_id,
            "price": price,
            "size": size,
            "original_size": size,
            "size_matched": size,  # taker fills immediately
            "side": side,
            "status": "MATCHED",
            "sell": sell,
        }
        if sell:
            self._balance += price * size
            self._position[token_id] = self._position.get(token_id, 0) - size
        else:
            self._balance -= price * size
            self._position[token_id] = self._position.get(token_id, 0) + size

        s = "up" if token_id == self._up_token else "down"
        fill = FillEvent(
            trade_id=f"bt-{uuid.uuid4().hex[:8]}",
            order_id=oid,
            side=s,
            price=price,
            filled_shares=-size if sell else size,
            usdc_cost=-price * size if sell else price * size,
            trader_side="TAKER",
            timestamp=int(self._sim_time),
        )
        self._fills.append(fill)
        return oid

    async def get_position(self, token_id: str) -> float:
        return self._position.get(token_id, 0)

    async def close(self):
        pass


def record_books(
    client: ExchangeClient,
    up_token: str,
    down_token: str,
    output_path: str | Path,
    interval_s: float = 2.0,
    duration_s: float = 900.0,
) -> None:
    """Record live order book snapshots to a JSON file for later replay.

    This is a synchronous helper intended to be called from a script.
    """
    import asyncio

    snapshots: list[dict] = []
    start = time.time()

    async def _record():
        nonlocal snapshots
        while time.time() - start < duration_s:
            try:
                up_book = await client.get_order_book(up_token)
                down_book = await client.get_order_book(down_token)
                snapshots.append({
                    "ts": time.time(),
                    "up_book": up_book,
                    "down_book": down_book,
                })
                logger.info("Recorded snapshot %d", len(snapshots))
            except Exception as e:
                logger.warning("Snapshot failed: %s", e)
            await asyncio.sleep(interval_s)

    asyncio.run(_record())

    path = Path(output_path)
    path.write_text(json.dumps(snapshots, indent=2))
    logger.info("Saved %d snapshots to %s", len(snapshots), path)


class BacktestRunner:
    """Run a strategy against recorded order book data.

    Usage:
        data = json.loads(Path("recorded_session.json").read_text())
        runner = BacktestRunner(data, config)
        result = runner.run()
        print(result)
    """

    def __init__(
        self,
        snapshots: List[Dict],
        config: BotConfig,
        up_token: str = "up_token",
        down_token: str = "down_token",
    ) -> None:
        self._snapshots = snapshots
        self._config = config
        self._up_token = up_token
        self._down_token = down_token

    def run(self) -> Dict:
        """Run the backtest and return results.

        Returns a dict with:
        - total_fills: number of fill events
        - up_shares, down_shares: final position
        - combined_vwap: final combined VWAP
        - worst_case_pnl: final worst-case P&L
        - expected_profit: total_shares/2 - total_cost
        - snapshots_replayed: number of book snapshots processed
        - fills: list of fill events
        """
        import asyncio
        from src.bot.position_tracker import PositionTracker
        from src.bot.order_engine import build_ladder, OrderEngine
        from src.bot.fill_monitor import FillMonitor
        from src.bot.market_scheduler import _get_effective_price

        client = ReplayClient(
            self._snapshots,
            up_token=self._up_token,
            down_token=self._down_token,
        )
        tracker = PositionTracker()

        async def _run():
            # Get initial prices
            up_book = await client.get_order_book(self._up_token)
            down_book = await client.get_order_book(self._down_token)
            up_price = _get_effective_price(up_book)
            down_price = _get_effective_price(down_book)

            if up_price <= 0 or down_price <= 0:
                return {"error": "No valid prices in initial snapshot"}

            # Build ladders
            half_cap = self._config.session_capital_limit / 2
            up_levels = max(2, min(15, int(half_cap / (self._config.shares_per_order * up_price))))
            down_levels = max(2, min(15, int(half_cap / (self._config.shares_per_order * down_price))))

            up_ladder = build_ladder(
                up_price, 0.01, self._config.shares_per_order, up_levels,
                other_side_best_ask=down_price, max_combined=self._config.max_combined_vwap,
            )
            down_ladder = build_ladder(
                down_price, 0.01, self._config.shares_per_order, down_levels,
                other_side_best_ask=up_price, max_combined=self._config.max_combined_vwap,
            )

            # Place initial orders
            expiration_ts = int(self._snapshots[-1]["ts"]) + 3600 if self._snapshots else int(time.time()) + 3600
            for level in up_ladder:
                price = level.price_cents / 100.0
                oid = await client.place_limit_order(
                    self._up_token, price, level.target_shares, "up", expiration_ts,
                )
                level.order_id = oid

            for level in down_ladder:
                price = level.price_cents / 100.0
                oid = await client.place_limit_order(
                    self._down_token, price, level.target_shares, "down", expiration_ts,
                )
                level.order_id = oid

            # Replay snapshots
            for snap in self._snapshots:
                client.advance_to(snap["ts"])

            # Collect fills
            for fill in client.fills:
                tracker.on_fill(fill)

            return {
                "total_fills": len(client.fills),
                "up_shares": tracker.position.up_shares,
                "down_shares": tracker.position.down_shares,
                "combined_vwap": tracker.position.combined_vwap,
                "worst_case_pnl": tracker.position.risk_worst_case,
                "expected_profit": tracker.position.expected_profit,
                "avg_cost_per_share": tracker.position.avg_cost_per_share,
                "snapshots_replayed": len(self._snapshots),
                "balance": client._balance,
                "fills": [
                    {
                        "trade_id": f.trade_id,
                        "side": f.side,
                        "price": f.price,
                        "shares": f.filled_shares,
                        "ts": f.timestamp,
                    }
                    for f in client.fills
                ],
            }

        return asyncio.run(_run())
