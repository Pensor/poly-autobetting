"""WebSocket orderbook monitor."""

import asyncio
import json
from datetime import datetime
from typing import Callable, Optional
import websockets

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class OrderbookMonitor:
    """Real-time orderbook monitoring via WebSocket."""

    def __init__(
        self,
        token_ids: list[str],
        on_update: Optional[Callable[[str, dict], None]] = None,
    ):
        self.token_ids = token_ids
        self.on_update = on_update
        self.orderbooks: dict[str, dict] = {}
        self.running = False
        self.ws = None

    async def connect(self):
        """Connect to WebSocket and subscribe to orderbooks."""
        self.running = True

        while self.running:
            try:
                async with websockets.connect(WS_URL) as ws:
                    self.ws = ws
                    print(f"Connected to Polymarket WebSocket")

                    # Subscribe to each token
                    for token_id in self.token_ids:
                        subscribe_msg = {
                            "type": "subscribe",
                            "channel": "book",
                            "market": token_id,
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        print(f"Subscribed to {token_id[:16]}...")

                    # Listen for updates
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            await self.handle_message(data)
                        except json.JSONDecodeError:
                            continue

            except websockets.exceptions.ConnectionClosed:
                print("WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"WebSocket error: {e}")
                await asyncio.sleep(5)

    async def handle_message(self, data: dict):
        """Handle incoming WebSocket message."""
        msg_type = data.get("type")
        market = data.get("market")

        if msg_type == "book":
            # Full orderbook snapshot
            self.orderbooks[market] = {
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
                "timestamp": datetime.now(),
            }

            if self.on_update:
                self.on_update(market, self.orderbooks[market])

        elif msg_type == "book_update":
            # Incremental update
            if market in self.orderbooks:
                # Apply updates (simplified - should properly merge)
                if "bids" in data:
                    self.orderbooks[market]["bids"] = data["bids"]
                if "asks" in data:
                    self.orderbooks[market]["asks"] = data["asks"]
                self.orderbooks[market]["timestamp"] = datetime.now()

                if self.on_update:
                    self.on_update(market, self.orderbooks[market])

    def get_best_prices(self, token_id: str) -> Optional[dict]:
        """Get current best bid/ask for a token."""
        book = self.orderbooks.get(token_id)
        if not book:
            return None

        bids = book.get("bids", [])
        asks = book.get("asks", [])

        return {
            "best_bid": float(bids[0]["price"]) if bids else None,
            "best_ask": float(asks[0]["price"]) if asks else None,
            "bid_size": float(bids[0]["size"]) if bids else 0,
            "ask_size": float(asks[0]["size"]) if asks else 0,
        }

    async def stop(self):
        """Stop monitoring."""
        self.running = False
        if self.ws:
            await self.ws.close()


async def monitor_pair(
    up_token_id: str,
    down_token_id: str,
    duration: int = 900,  # 15 minutes
):
    """Monitor a Up/Down pair for arbitrage opportunities.

    Args:
        up_token_id: Token ID for Up outcome
        down_token_id: Token ID for Down outcome
        duration: How long to monitor in seconds
    """
    spreads = []

    def on_update(token_id: str, book: dict):
        # Calculate spread when either book updates
        pass  # Simplified

    monitor = OrderbookMonitor(
        token_ids=[up_token_id, down_token_id],
        on_update=on_update,
    )

    # Run for specified duration
    task = asyncio.create_task(monitor.connect())

    try:
        await asyncio.sleep(duration)
    finally:
        await monitor.stop()
        task.cancel()

    return spreads
