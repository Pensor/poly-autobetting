"""Common session loop primitives shared across strategies.

Extracts duplicated logic from runner.py's _run_session, _run_passive_session,
and _run_hedge_sell_session into reusable functions.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from src.bot.types import (
    BotConfig, BotState, FillEvent, LadderLevel, MarketInfo, MarketPhase,
)
from src.bot.fill_monitor import FillMonitor, OrderReconciler
from src.bot.order_engine import OrderEngine
from src.bot.position_tracker import PositionTracker
from src.bot.alerts import AlertManager
from src.bot.market_scheduler import MarketScheduler

logger = logging.getLogger(__name__)


def drain_fills(
    fill_queue: asyncio.Queue,
    tracker: PositionTracker,
    state: BotState,
    order_engine: OrderEngine,
    update_fill_tracking: Callable[[FillEvent], None],
    *,
    last_side_fill_ts: Optional[dict[str, float]] = None,
    recent_fill_oids: Optional[set[str]] = None,
) -> dict[str, int]:
    """Drain all pending fills from queue and update tracker.

    Returns per-side fill count for hot-side detection.

    This is the single-writer fill processing path. All fill events
    flow through here to prevent double-counting or missed updates.
    """
    drain_count: dict[str, int] = {"up": 0, "down": 0}

    while not fill_queue.empty():
        try:
            fill = fill_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        if not fill.order_id:
            logger.warning(
                "Ignoring fill without order_id trade_id=%s side=%s size=%.4f",
                fill.trade_id, fill.side, fill.filled_shares,
            )
            continue

        meta = state.order_map.get(fill.order_id)

        # Map side from token_id to "up"/"down" via order_map metadata
        if fill.side not in ("up", "down"):
            mapped_side = meta.get("side", "") if meta else ""
            if mapped_side in ("up", "down"):
                fill.side = mapped_side
            else:
                logger.warning(
                    "Ignoring fill with unmappable side trade_id=%s order_id=%s",
                    fill.trade_id, fill.order_id,
                )
                continue

        if not meta:
            # Cancel-race fill: order was cancelled and cleanup_cancelled_orders
            # already removed the entry, but the exchange matched it before the
            # cancel was processed. This is a REAL fill we paid for — must track it.
            # The fill already passed _registered_ids in FillMonitor, so it IS
            # from the current session. Skip on_fill_release (cancel already
            # released reserved_notional) and update_fill_tracking (no level).
            logger.info(
                "Cancel-race fill trade_id=%s order_id=%s side=%s size=%.4f price=%.4f "
                "(not in order_map — applying to tracker as late cancel-race fill)",
                fill.trade_id, fill.order_id, fill.side, fill.filled_shares,
                fill.price,
            )
            tracker.on_fill(fill)
            # Reduce cancel_race_buffer since this fill materialized
            state.cancel_race_buffer = max(0.0, state.cancel_race_buffer - fill.usdc_cost)
            state.total_trades += 1
            state.position = tracker.position
            drain_count[fill.side] = drain_count.get(fill.side, 0) + 1
            if last_side_fill_ts is not None:
                last_side_fill_ts[fill.side] = time.time()
            if recent_fill_oids is not None:
                recent_fill_oids.add(fill.order_id)
            continue

        tracker.on_fill(fill)
        # Cancelled orders: reserved_notional was already released by
        # on_cancel_release. Skip on_fill_release to avoid double-decrement.
        # Taker orders: never reserved notional (Bug #11 fix).
        is_cancelled = meta.get("cancelled", False)
        if fill.trader_side != "TAKER" and not is_cancelled:
            order_engine.on_fill_release(fill, state)
        update_fill_tracking(fill)
        # Clean up cancelled order entry after fill is processed
        if is_cancelled:
            # Fill arrived for cancelled order — reduce cancel race buffer
            state.cancel_race_buffer = max(0.0, state.cancel_race_buffer - fill.usdc_cost)
            state.order_map.pop(fill.order_id, None)

        state.total_trades += 1
        state.position = tracker.position
        drain_count[fill.side] = drain_count.get(fill.side, 0) + 1

        if last_side_fill_ts is not None:
            last_side_fill_ts[fill.side] = time.time()
        if recent_fill_oids is not None:
            recent_fill_oids.add(fill.order_id)

    # Clean up stale cancelled order_map entries.
    # Fix #82: Reduced from 30s to 8s. Cancel-race fills arrive within 1-3s
    # (Polymarket API latency). 30s caused 10+ rapid re-anchors to accumulate
    # $500+ phantom reserve, blocking new orders for the entire duration.
    # 8s gives ~3 re-anchor cycles for late fills while clearing fast enough.
    cleanup_cancelled_orders(state, max_age=8.0)

    return drain_count


def mark_orders_cancelled(state: BotState, order_ids: list) -> None:
    """Mark orders as cancelled in order_map instead of removing them.

    Keeps the metadata so late-arriving fills can still find their side mapping
    and avoid 'unmatched fill' warnings. The entry is cleaned up either when
    drain_fills processes a fill for it, or by cleanup_cancelled_orders().
    """
    now = time.time()
    for oid in order_ids:
        meta = state.order_map.get(oid)
        if meta is not None:
            meta["cancelled"] = True
            meta["cancel_ts"] = now


def cleanup_cancelled_orders(state: BotState, max_age: float = 5.0) -> int:
    """Remove stale cancelled order entries from order_map.

    Called periodically in the main loop. Orders older than max_age seconds
    that weren't filled are truly gone — safe to remove.
    Also decays cancel_race_buffer proportionally as stale entries expire.
    """
    now = time.time()
    cancelled_before = [
        oid for oid, m in state.order_map.items()
        if m.get("cancelled")
    ]
    stale = [
        oid for oid in cancelled_before
        if (now - state.order_map[oid].get("cancel_ts", 0)) > max_age
    ]
    # Decay buffer proportionally: if N of M cancelled entries expired, release N/M of buffer
    if stale and cancelled_before and state.cancel_race_buffer > 0:
        decay_fraction = len(stale) / len(cancelled_before)
        state.cancel_race_buffer *= (1.0 - decay_fraction)
        if state.cancel_race_buffer < 0.01:
            state.cancel_race_buffer = 0.0
    for oid in stale:
        state.order_map.pop(oid, None)
    # If no cancelled entries remain at all, fully reset buffer
    has_cancelled = any(m.get("cancelled") for m in state.order_map.values())
    if not has_cancelled:
        state.cancel_race_buffer = 0.0
    return len(stale)


def check_phase_transition(
    scheduler: MarketScheduler,
    market: MarketInfo,
    state: BotState,
) -> MarketPhase:
    """Check current market phase and detect transitions.

    Returns the current phase. Caller should handle WINDING_DOWN/CLOSED transitions.
    """
    return scheduler.get_current_phase(market)


async def setup_fill_monitor(
    client: Any,
    config: BotConfig,
    market: MarketInfo,
    fill_monitor_cls: type = FillMonitor,
) -> tuple[asyncio.Queue, FillMonitor, asyncio.Event, asyncio.Task]:
    """Create and start a fill monitor with its queue and stop event.

    Returns (fill_queue, fill_monitor, stop_event, monitor_task).
    """
    fill_queue: asyncio.Queue = asyncio.Queue()
    fill_monitor = fill_monitor_cls(
        client, fill_queue, market.condition_id,
        market.up_token, market.down_token,
    )
    stop_event = asyncio.Event()
    monitor_task = asyncio.create_task(
        fill_monitor.run(
            interval=max(0.2, config.fill_poll_interval_s),
            stop_event=stop_event,
        )
    )
    return fill_queue, fill_monitor, stop_event, monitor_task


async def cleanup_session(
    client: Any,
    market: MarketInfo,
    stop_event: asyncio.Event,
    monitor_task: asyncio.Task,
) -> None:
    """Stop fill monitor and cancel all orders for a market."""
    stop_event.set()
    try:
        await asyncio.wait_for(monitor_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        monitor_task.cancel()

    await client.cancel_and_verify(market.up_token)
    await client.cancel_and_verify(market.down_token)


async def do_heartbeat(
    tracker: PositionTracker,
    state: BotState,
    alerts: AlertManager,
    summary: str,
) -> None:
    """Log heartbeat summary and send alert."""
    logger.info(summary)
    await alerts.heartbeat_status(state.phase.value, summary)


async def check_kill_command(alerts: AlertManager) -> bool:
    """Check for /kill Telegram command. Returns True if killed."""
    return await alerts.check_kill_command()
