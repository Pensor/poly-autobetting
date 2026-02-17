"""P&L Calculator with fee calculations for Polymarket."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Trade:
    """Represents a single trade."""
    side: str  # "buy" or "sell"
    outcome: str  # "Up", "Down", "Yes", "No"
    price: float
    shares: float
    is_maker: bool = False
    timestamp: Optional[str] = None
    market_id: Optional[str] = None


@dataclass
class TradeResult:
    """Result of P&L calculation for a trade pair."""
    up_trade: Optional[Trade]
    down_trade: Optional[Trade]
    gross_cost: float
    fees: float
    rebates: float
    net_cost: float
    guaranteed_payout: float
    net_profit: float
    profit_pct: float


def calculate_taker_fee(shares: float, price: float) -> float:
    """Calculate taker fee.

    Polymarket fee formula:
    fee = shares * base_rate * price * (1 - price)

    At price=0.50: fee ≈ 2% of trade value
    At price=0.90: fee ≈ 0.9% of trade value
    At price=0.10: fee ≈ 0.9% of trade value
    """
    # Fee rate is effectively ~2% at the extremes, adjusted by p*(1-p)
    base_rate = 0.02
    fee_multiplier = 4 * price * (1 - price)  # Peaks at 1.0 when p=0.5
    fee = shares * base_rate * fee_multiplier * price
    return fee


def calculate_maker_rebate(shares: float, price: float) -> float:
    """Calculate maker rebate (20% of what taker would pay)."""
    taker_fee = calculate_taker_fee(shares, price)
    return taker_fee * 0.20


def calculate_arbitrage_pnl(
    up_price: float,
    down_price: float,
    shares: float = 100,
    is_maker: bool = False,
) -> TradeResult:
    """Calculate P&L for Up+Down arbitrage position.

    Strategy: Buy both Up and Down outcomes.
    At resolution, one pays $1, other pays $0.
    Profit if total cost < $1 per share pair.

    Args:
        up_price: Price per share for Up outcome
        down_price: Price per share for Down outcome
        shares: Number of shares to buy of each
        is_maker: If True, no fees + receive rebates

    Returns:
        TradeResult with detailed P&L breakdown
    """
    # Gross cost (before fees)
    up_cost = up_price * shares
    down_cost = down_price * shares
    gross_cost = up_cost + down_cost

    # Fees
    if is_maker:
        fees = 0
        rebates = (
            calculate_maker_rebate(shares, up_price) +
            calculate_maker_rebate(shares, down_price)
        )
    else:
        fees = (
            calculate_taker_fee(shares, up_price) +
            calculate_taker_fee(shares, down_price)
        )
        rebates = 0

    # Net cost
    net_cost = gross_cost + fees - rebates

    # Guaranteed payout (one outcome wins)
    guaranteed_payout = shares * 1.0  # $1 per winning share

    # Net profit
    net_profit = guaranteed_payout - net_cost
    profit_pct = (net_profit / net_cost * 100) if net_cost > 0 else 0

    up_trade = Trade(
        side="buy",
        outcome="Up",
        price=up_price,
        shares=shares,
        is_maker=is_maker,
    )
    down_trade = Trade(
        side="buy",
        outcome="Down",
        price=down_price,
        shares=shares,
        is_maker=is_maker,
    )

    return TradeResult(
        up_trade=up_trade,
        down_trade=down_trade,
        gross_cost=gross_cost,
        fees=fees,
        rebates=rebates,
        net_cost=net_cost,
        guaranteed_payout=guaranteed_payout,
        net_profit=net_profit,
        profit_pct=profit_pct,
    )


def find_profitable_spread(
    is_maker: bool = False,
    shares: float = 100,
) -> float:
    """Find the maximum total price (Up+Down) for profitable arbitrage.

    Returns the threshold below which arbitrage is profitable.
    """
    # Binary search for break-even point
    low, high = 0.90, 1.00

    for _ in range(20):  # Precision sufficient
        mid = (low + high) / 2
        # Assume symmetric pricing
        up_price = mid / 2
        down_price = mid / 2

        result = calculate_arbitrage_pnl(up_price, down_price, shares, is_maker)

        if result.net_profit > 0:
            low = mid
        else:
            high = mid

    return high


def print_pnl_analysis(result: TradeResult):
    """Print detailed P&L analysis."""
    print("\n" + "=" * 50)
    print("ARBITRAGE P&L ANALYSIS")
    print("=" * 50)

    if result.up_trade:
        print(f"\nUp Trade:")
        print(f"  Price: ${result.up_trade.price:.4f}")
        print(f"  Shares: {result.up_trade.shares:.0f}")
        print(f"  Cost: ${result.up_trade.price * result.up_trade.shares:.2f}")
        print(f"  Maker: {result.up_trade.is_maker}")

    if result.down_trade:
        print(f"\nDown Trade:")
        print(f"  Price: ${result.down_trade.price:.4f}")
        print(f"  Shares: ${result.down_trade.shares:.0f}")
        print(f"  Cost: ${result.down_trade.price * result.down_trade.shares:.2f}")
        print(f"  Maker: {result.down_trade.is_maker}")

    print(f"\nCosts:")
    print(f"  Gross Cost: ${result.gross_cost:.2f}")
    print(f"  Fees: ${result.fees:.2f}")
    print(f"  Rebates: ${result.rebates:.2f}")
    print(f"  Net Cost: ${result.net_cost:.2f}")

    print(f"\nPayout:")
    print(f"  Guaranteed: ${result.guaranteed_payout:.2f}")

    print(f"\nProfit:")
    print(f"  Net Profit: ${result.net_profit:.2f}")
    print(f"  Return: {result.profit_pct:.2f}%")

    status = "✅ PROFITABLE" if result.net_profit > 0 else "❌ NOT PROFITABLE"
    print(f"\nStatus: {status}")
    print("=" * 50)


if __name__ == "__main__":
    # Example calculations
    print("\n🔍 TAKER Analysis (market orders):")
    result_taker = calculate_arbitrage_pnl(0.49, 0.50, shares=100, is_maker=False)
    print_pnl_analysis(result_taker)

    print("\n🔍 MAKER Analysis (limit orders):")
    result_maker = calculate_arbitrage_pnl(0.49, 0.50, shares=100, is_maker=True)
    print_pnl_analysis(result_maker)

    print("\n📊 Break-even thresholds:")
    taker_threshold = find_profitable_spread(is_maker=False)
    maker_threshold = find_profitable_spread(is_maker=True)
    print(f"  Taker: Up+Down must be < ${taker_threshold:.4f}")
    print(f"  Maker: Up+Down must be < ${maker_threshold:.4f}")
