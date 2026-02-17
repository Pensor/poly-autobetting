"""Rebalance and re-anchor coordination logic.

Extracted from runner.py to provide reusable imbalance detection,
VWAP headroom checking, and re-anchor decision-making across strategies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from src.bot.types import BotConfig, LadderLevel, PositionState

logger = logging.getLogger(__name__)


class ImbalanceAction(Enum):
    """What to do about position imbalance."""
    NONE = "none"
    CANCEL_HEAVY = "cancel_heavy"
    HARD_REBALANCE = "hard_rebalance"


@dataclass
class ImbalanceVerdict:
    """Result of an imbalance check."""
    action: ImbalanceAction
    heavy_side: str  # "up" or "down"
    lagging_side: str  # "up" or "down"
    imbalance: float
    reason: str


def check_imbalance(
    pos: PositionState,
    config: BotConfig,
) -> ImbalanceVerdict:
    """Check position imbalance and recommend action.

    Returns an ImbalanceVerdict indicating what to do:
    - NONE: balanced enough, continue normally
    - CANCEL_HEAVY: soft imbalance — cancel heavy side orders
    - HARD_REBALANCE: hard imbalance — cancel all and rebuild ladders
    """
    if pos.total_shares < config.rebalance_min_total_shares - 0.5:
        return ImbalanceVerdict(
            ImbalanceAction.NONE, "", "", 0.0,
            "insufficient_shares",
        )

    imbalance = pos.share_imbalance
    heavy = pos.excess_side
    lagging = "down" if heavy == "up" else "up"

    if imbalance >= config.rebalance_hard_imbalance:
        return ImbalanceVerdict(
            ImbalanceAction.HARD_REBALANCE, heavy, lagging, imbalance,
            f"hard_imbalance={imbalance:.1%}",
        )

    if imbalance >= config.rebalance_soft_imbalance:
        return ImbalanceVerdict(
            ImbalanceAction.CANCEL_HEAVY, heavy, lagging, imbalance,
            f"soft_imbalance={imbalance:.1%}",
        )

    return ImbalanceVerdict(
        ImbalanceAction.NONE, heavy, lagging, imbalance,
        f"balanced={imbalance:.1%}",
    )


def candidate_has_vwap_headroom(
    side: str,
    candidate_ladder: List[LadderLevel],
    pos: PositionState,
    max_combined_vwap: float,
) -> bool:
    """Return True if ALL candidate levels filling keeps projected combined VWAP profitable.

    Simulates worst case — every level fills simultaneously.
    Blocks re-anchor when full ladder would push combined past max_combined_vwap.
    """
    if not candidate_ladder:
        return False

    total_new_cost = sum(
        (l.price_cents / 100.0) * l.target_shares for l in candidate_ladder
    )
    total_new_shares = sum(l.target_shares for l in candidate_ladder)

    if side == "up":
        new_cost = pos.up_cost + total_new_cost
        new_shares = pos.up_shares + total_new_shares
        new_side_vwap = new_cost / new_shares if new_shares > 0 else 0.0
        other_vwap = pos.down_vwap if pos.down_vwap > 0 else new_side_vwap
    else:
        new_cost = pos.down_cost + total_new_cost
        new_shares = pos.down_shares + total_new_shares
        new_side_vwap = new_cost / new_shares if new_shares > 0 else 0.0
        other_vwap = pos.up_vwap if pos.up_vwap > 0 else new_side_vwap

    return new_side_vwap + other_vwap <= max_combined_vwap


@dataclass
class ReanchorDecision:
    """Result of a re-anchor decision."""
    should_reanchor: bool
    reason: str


def should_reanchor_side(
    side: str,
    new_price: float,
    anchor_price: float,
    candidate_ladder: list,
    *,
    now_ts: float,
    time_left_s: float,
    current_edge: float,
    candidate_edge: float,
    out_of_range_ratio: float,
    lagging_side: str,
    imbalance: float,
    total_shares: float,
    combined_vwap: float,
    side_order_count: int,
    last_reanchor_ts: float,
    last_fill_ts: float,
    config: BotConfig,
) -> ReanchorDecision:
    """Decide whether to re-anchor a side based on execution value.

    Pure function — no side effects. Caller handles the actual cancel + rebuild.
    """
    drift = abs(new_price - anchor_price)

    # Fix #25: Force re-anchor when side has no active orders but position exists.
    force_reanchor = side_order_count == 0 and total_shares > 0

    if not force_reanchor and drift < config.reanchor_drift_threshold:
        return ReanchorDecision(False, f"drift {drift:.3f} < {config.reanchor_drift_threshold:.3f}")

    if time_left_s < config.reanchor_min_time_left_s:
        return ReanchorDecision(False, f"time_left {time_left_s:.0f}s < {config.reanchor_min_time_left_s:.0f}s")

    if now_ts - last_reanchor_ts < config.reanchor_min_action_cooldown_s:
        return ReanchorDecision(False, "cooldown")

    if now_ts - last_fill_ts < config.reanchor_side_fill_grace_s:
        return ReanchorDecision(False, "recent_fill")

    if not candidate_ladder:
        return ReanchorDecision(False, "empty_candidate_ladder")

    # When imbalanced, re-anchor only the lagging side.
    lagging_only = (
        total_shares >= config.rebalance_min_total_shares - 0.5
        and bool(lagging_side)
        and imbalance >= max(0.0, config.rebalance_soft_imbalance)
    )
    # Fix #28: Allow non-lagging side to re-anchor when it has 0 orders.
    if lagging_only and side != lagging_side and not force_reanchor:
        return ReanchorDecision(False, f"lagging_only:{lagging_side} imbalance={imbalance:.1%}")

    edge_gain = candidate_edge - current_edge
    lagging_signal = (
        lagging_side == side and imbalance >= config.rebalance_hard_imbalance
    )

    # Fix #23+#27: Don't chase worse prices when VWAP is near the limit,
    # and cap max deviation to 5c above anchor.
    worse_deviation = new_price - anchor_price
    emergency_worse_price_ok = (
        lagging_side == side
        and imbalance >= config.reanchor_worse_price_imbalance_emergency
        and combined_vwap < config.max_combined_vwap
        and worse_deviation <= 0.05
    )
    if new_price > anchor_price and not emergency_worse_price_ok and not force_reanchor:
        return ReanchorDecision(False, f"worse_price {anchor_price:.3f}->{new_price:.3f}")

    if force_reanchor or edge_gain > 0.0 or lagging_signal:
        return ReanchorDecision(
            True,
            (
                f"drift={drift:.3f} out={out_of_range_ratio:.2f} edge_gain={edge_gain:.3f} "
                f"hard_lagging={lagging_signal} emergency_worse={emergency_worse_price_ok} "
                f"force={force_reanchor}"
            ),
        )

    return ReanchorDecision(
        False,
        (
            f"low_value out={out_of_range_ratio:.2f} edge_gain={edge_gain:.3f} "
            f"hard_lagging={lagging_signal} emergency_worse={emergency_worse_price_ok}"
        ),
    )
