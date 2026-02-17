"""Spread tracker for arbitrage opportunities."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

from ..api import clob, gamma
from ..analysis.calculator import calculate_arbitrage_pnl


@dataclass
class SpreadOpportunity:
    """Represents a potential arbitrage opportunity."""
    market_id: str
    market_question: str
    up_token_id: str
    down_token_id: str
    up_price: float
    down_price: float
    total_spread: float
    maker_profit: float
    maker_profit_pct: float
    taker_profit: float
    taker_profit_pct: float
    timestamp: datetime
    end_time: Optional[datetime]


class SpreadTracker:
    """Tracks spreads across 15-minute BTC markets."""

    def __init__(
        self,
        profit_threshold: float = 0.0,  # Minimum profit to alert
        callback: Optional[Callable[[SpreadOpportunity], None]] = None,
    ):
        self.profit_threshold = profit_threshold
        self.callback = callback
        self.opportunities: list[SpreadOpportunity] = []
        self.running = False

    async def check_market(self, market: dict) -> Optional[SpreadOpportunity]:
        """Check a single market for arbitrage opportunity."""
        try:
            outcomes = gamma.parse_market_outcomes(market)

            # Get token IDs
            up_data = outcomes.get("Up") or outcomes.get("Yes")
            down_data = outcomes.get("Down") or outcomes.get("No")

            if not up_data or not down_data:
                return None

            up_token = up_data.get("token_id")
            down_token = down_data.get("token_id")

            if not up_token or not down_token:
                return None

            # Get current prices from orderbook
            up_book = await clob.get_orderbook(up_token)
            down_book = await clob.get_orderbook(down_token)

            up_analysis = clob.analyze_orderbook(up_book)
            down_analysis = clob.analyze_orderbook(down_book)

            # Use best ask prices (what we'd pay as taker)
            # For maker, we'd place at best bid + small increment
            up_ask = up_analysis.get("best_ask")
            down_ask = down_analysis.get("best_ask")
            up_bid = up_analysis.get("best_bid")
            down_bid = down_analysis.get("best_bid")

            if not all([up_ask, down_ask, up_bid, down_bid]):
                return None

            # For maker orders, we'd buy at slightly above best bid
            # Simplified: use midpoint as maker price
            up_maker_price = (up_bid + up_ask) / 2
            down_maker_price = (down_bid + down_ask) / 2

            total_spread = up_ask + down_ask

            # Calculate P&L for both scenarios
            maker_result = calculate_arbitrage_pnl(
                up_maker_price, down_maker_price, shares=100, is_maker=True
            )
            taker_result = calculate_arbitrage_pnl(
                up_ask, down_ask, shares=100, is_maker=False
            )

            opportunity = SpreadOpportunity(
                market_id=market.get("id", ""),
                market_question=market.get("question", ""),
                up_token_id=up_token,
                down_token_id=down_token,
                up_price=up_ask,
                down_price=down_ask,
                total_spread=total_spread,
                maker_profit=maker_result.net_profit,
                maker_profit_pct=maker_result.profit_pct,
                taker_profit=taker_result.net_profit,
                taker_profit_pct=taker_result.profit_pct,
                timestamp=datetime.now(),
                end_time=None,  # Parse from market if available
            )

            return opportunity

        except Exception as e:
            print(f"Error checking market: {e}")
            return None

    async def scan_markets(self) -> list[SpreadOpportunity]:
        """Scan all active 15-min BTC markets."""
        opportunities = []

        try:
            markets = await gamma.get_btc_15min_markets(limit=50)
            print(f"Found {len(markets)} BTC 15-min markets")

            for market in markets:
                opp = await self.check_market(market)
                if opp:
                    opportunities.append(opp)

                    # Alert if profitable
                    if opp.maker_profit >= self.profit_threshold:
                        if self.callback:
                            self.callback(opp)
                        self.opportunities.append(opp)

                # Rate limiting
                await asyncio.sleep(0.2)

        except Exception as e:
            print(f"Error scanning markets: {e}")

        return opportunities

    async def run(self, interval: int = 30):
        """Run continuous monitoring.

        Args:
            interval: Seconds between scans
        """
        self.running = True
        print(f"Starting spread tracker (interval: {interval}s)")

        while self.running:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning markets...")
            opportunities = await self.scan_markets()

            profitable = [o for o in opportunities if o.maker_profit > 0]
            print(f"Found {len(opportunities)} markets, {len(profitable)} profitable")

            if profitable:
                print("\n🎯 Profitable opportunities:")
                for opp in profitable:
                    print(f"  {opp.market_question[:50]}...")
                    print(f"    Spread: ${opp.total_spread:.4f}")
                    print(f"    Maker profit: ${opp.maker_profit:.2f} ({opp.maker_profit_pct:.2f}%)")

            await asyncio.sleep(interval)

    def stop(self):
        """Stop monitoring."""
        self.running = False


def print_opportunity(opp: SpreadOpportunity):
    """Print opportunity details."""
    print("\n" + "=" * 60)
    print("🎯 ARBITRAGE OPPORTUNITY DETECTED")
    print("=" * 60)
    print(f"Market: {opp.market_question}")
    print(f"Time: {opp.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nPrices:")
    print(f"  Up: ${opp.up_price:.4f}")
    print(f"  Down: ${opp.down_price:.4f}")
    print(f"  Total: ${opp.total_spread:.4f}")
    print(f"\nProfit (100 shares):")
    print(f"  As Maker: ${opp.maker_profit:.2f} ({opp.maker_profit_pct:.2f}%)")
    print(f"  As Taker: ${opp.taker_profit:.2f} ({opp.taker_profit_pct:.2f}%)")
    print("=" * 60)
