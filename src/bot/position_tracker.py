"""Position tracker: VWAP, P&L, and fill processing."""

from __future__ import annotations

import logging
from typing import List

from src.bot.types import FillEvent, PositionState

logger = logging.getLogger(__name__)


class PositionTracker:
    """Tracks cumulative position state across fills.

    Single-writer pattern: only the runner's main loop should call on_fill().
    Deduplicates fills by trade_id to prevent double-counting from reconciliation
    or late fills leaking across sessions.
    """

    def __init__(self) -> None:
        self.position = PositionState()
        self._fills: List[FillEvent] = []
        self._seen_trade_ids: set[str] = set()

    def on_fill(self, fill: FillEvent) -> None:
        """Process a fill event, updating position state.

        Ignores duplicate trade_ids to prevent double-counting.
        """
        if fill.trade_id and fill.trade_id in self._seen_trade_ids:
            logger.warning("Ignoring duplicate fill: trade_id=%s order_id=%s", fill.trade_id, fill.order_id)
            return
        if fill.trade_id:
            self._seen_trade_ids.add(fill.trade_id)
        self._fills.append(fill)
        if fill.side == "up":
            self.position.up_shares += fill.filled_shares
            self.position.up_cost += fill.usdc_cost
        elif fill.side == "down":
            self.position.down_shares += fill.filled_shares
            self.position.down_cost += fill.usdc_cost

    @property
    def fills(self) -> List[FillEvent]:
        """All recorded fills."""
        return list(self._fills)

    @property
    def fill_count(self) -> int:
        """Total number of fills processed."""
        return len(self._fills)

    # --- Delegated properties from PositionState ---

    @property
    def up_vwap(self) -> float:
        return self.position.up_vwap

    @property
    def down_vwap(self) -> float:
        return self.position.down_vwap

    @property
    def combined_vwap(self) -> float:
        return self.position.combined_vwap

    @property
    def hedged_shares(self) -> float:
        return self.position.hedged_shares

    @property
    def hedged_profit(self) -> float:
        return self.position.hedged_profit

    @property
    def excess_shares(self) -> float:
        return self.position.excess_shares

    @property
    def excess_side(self) -> str:
        return self.position.excess_side

    @property
    def share_imbalance(self) -> float:
        return self.position.share_imbalance

    @property
    def unhedged_exposure(self) -> float:
        return self.position.unhedged_exposure

    # --- Split P&L (v2.2) ---

    @property
    def risk_worst_case(self) -> float:
        """Worst-case P&L excluding rebates (conservative, for circuit breakers)."""
        return self.position.risk_worst_case

    @property
    def rebate_estimate(self) -> float:
        """Estimated maker rebate from all fills.

        Formula: 0.016 * price * (1 - price) * shares per fill.
        Only MAKER fills earn rebates.
        """
        return sum(
            0.016 * f.price * (1.0 - f.price) * f.filled_shares
            for f in self._fills
            if f.trader_side == "MAKER"
        )

    @property
    def taker_fee_estimate(self) -> float:
        """Estimated taker fees paid across all taker fills."""
        from src.bot.math_engine import estimate_taker_fee
        return sum(
            estimate_taker_fee(f.filled_shares, f.price)
            for f in self._fills
            if f.trader_side == "TAKER"
        )

    @property
    def net_profit_estimate(self) -> float:
        """worst_case + rebates - taker_fees."""
        return self.position.risk_worst_case + self.rebate_estimate - self.taker_fee_estimate

    @property
    def report_worst_case(self) -> float:
        """Worst-case P&L including rebate estimate (for reporting/alerts)."""
        return self.risk_worst_case + self.rebate_estimate

    @property
    def risk_pnl_if_up(self) -> float:
        """P&L if Up wins (no rebate)."""
        return self.position.risk_pnl_if_up

    @property
    def risk_pnl_if_down(self) -> float:
        """P&L if Down wins (no rebate)."""
        return self.position.risk_pnl_if_down

    def reset(self) -> None:
        """Reset for new session."""
        self.position = PositionState()
        self._fills.clear()
        self._seen_trade_ids.clear()
