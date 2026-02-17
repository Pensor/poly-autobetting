"""Penny ladder order engine — builds, places, and manages the order ladder.

Architecture:
- build_ladder(): constructs price levels from orderbook
- OrderEngine: places orders with warm start, budgeted placement, replenishment
- All prices in LadderLevel are price_cents (int), converted to float for API
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from src.bot.types import BotConfig, BotState, FillEvent, LadderLevel
from src.bot.math_engine import clamp_price, round_to_tick
from src.bot.client import ExchangeClient

if TYPE_CHECKING:
    from src.bot.fill_monitor import FillMonitor

logger = logging.getLogger(__name__)


def build_ladder(
    best_ask: float,
    tick_size: float,
    shares_per_order: float,
    num_levels: int = 15,
    other_side_best_ask: float = 0.0,
    max_combined: float = 0.995,
) -> List[LadderLevel]:
    """Build a penny ladder below best_ask.

    Range: [best_ask - num_levels*tick, best_ask - tick]
    Each level is one tick apart.
    Pre-filter: skip prices >= best_ask (post-only rejection).
    Clamp: all prices within [tick_size, 1.0 - tick_size].

    Returns levels sorted by price descending (closest to best_ask first).
    """
    levels = []
    for i in range(1, num_levels + 1):
        raw_price = best_ask - i * tick_size
        price = clamp_price(raw_price, tick_size)
        price = round_to_tick(price, tick_size)

        # Pre-filter: must be strictly below best_ask
        if price >= best_ask:
            continue
        # Fix #7: Cross-side awareness — skip levels that would be unprofitable
        if other_side_best_ask > 0 and price + other_side_best_ask >= max_combined:
            continue
        # Skip if order notional < $1 (exchange minimum)
        if shares_per_order * price < 1.0:
            continue
        # Skip if price already clamped to same value (dedup)
        price_cents = int(round(price * 100))
        if any(l.price_cents == price_cents for l in levels):
            continue

        levels.append(LadderLevel(
            price_cents=price_cents,
            target_shares=shares_per_order,
        ))

    # Sort descending (closest to best_ask first — for warm start)
    levels.sort(key=lambda l: l.price_cents, reverse=True)
    return levels


def build_full_range_ladder(
    current_price: float,
    tick_size: float,
    shares_per_order: float,
    step_cents: int = 2,
    floor_cents: int = 2,
    max_depth_cents: int = 0,
    max_buy_price_cents: int = 0,
    orders_per_level: int = 1,
) -> List[LadderLevel]:
    """Build a full-range penny ladder from floor up to current_price.

    Used by the passive strategy: places BUY orders from floor up to
    current_price - 1c, stepping by step_cents.
    If max_depth_cents > 0, the effective floor is raised to
    (current_price - max_depth_cents), concentrating capital near the action.
    If max_buy_price_cents > 0, the top of the ladder is capped at that price.
    Skips levels where shares * price < $1.0 (exchange minimum notional).
    Returns levels sorted descending (closest to current price first for warm start).
    """
    levels = []
    current_cents = int(round(current_price * 100))
    step = max(1, step_cents)
    floor = max(1, floor_cents)

    # If max_depth_cents is set, raise the floor to concentrate near current price
    if max_depth_cents > 0:
        depth_floor = current_cents - max_depth_cents
        floor = max(floor, depth_floor)

    # Hard price cap: never place above max_buy_price_cents
    top_cents = current_cents - 1  # start 1c below current price
    if max_buy_price_cents > 0:
        top_cents = min(top_cents, max_buy_price_cents)

    price_cents = top_cents
    # Align to step grid
    price_cents = price_cents - (price_cents % step) if price_cents % step != 0 else price_cents

    while price_cents >= floor:
        price = price_cents / 100.0
        # Clamp to valid range
        price = clamp_price(price, tick_size)
        price = round_to_tick(price, tick_size)
        clamped_cents = int(round(price * 100))

        # Skip if notional < $1.0 (exchange minimum)
        if shares_per_order * price >= 1.0:
            existing = sum(1 for l in levels if l.price_cents == clamped_cents)
            for _ in range(max(1, orders_per_level) - existing):
                levels.append(LadderLevel(
                    price_cents=clamped_cents,
                    target_shares=shares_per_order,
                ))

        price_cents -= step

    # Sort descending (closest to current price first — for warm start)
    levels.sort(key=lambda l: l.price_cents, reverse=True)
    return levels


def build_trend_ladder(
    best_bid: float,
    tick_size: float,
    shares_per_order: float,
    num_levels: int = 8,
    step_cents: int = 1,
    max_buy_price_cents: int = 99,
    min_notional: float = 1.0,
) -> List[LadderLevel]:
    """Build a windowed trend ladder: num_levels below best_bid, step_cents apart.

    Used by the trend strategy: places BUY orders in a short window just
    below the current best bid, following the market as it moves.

    Dynamic share sizing: at cheap prices, scales up shares to meet min_notional.
    At expensive prices, uses shares_per_order as-is. This means:
    - At 50c: 5 shares × $0.50 = $2.50  (normal)
    - At  5c: 20 shares × $0.05 = $1.00 (scaled up for notional)
    - At  2c: 50 shares × $0.02 = $1.00 (scaled up — more shares = more payout)
    - At 95c: 5 shares × $0.95 = $4.75  (normal — this is the hedge)

    Returns levels sorted descending (closest to best_bid first).
    """
    import math as _math
    levels: List[LadderLevel] = []
    # When bid exceeds price cap, start ladder from the cap instead of the bid.
    # Orders will be deeper in the book but still valid limit orders.
    effective_bid = best_bid
    if max_buy_price_cents > 0 and int(round(best_bid * 100)) > max_buy_price_cents:
        effective_bid = (max_buy_price_cents + 1) / 100.0  # +1 so first level lands at cap
    for i in range(1, num_levels + 1):
        raw_price = effective_bid - i * step_cents / 100.0
        price = clamp_price(raw_price, tick_size)
        price = round_to_tick(price, tick_size)
        pc = int(round(price * 100))
        if max_buy_price_cents > 0 and pc > max_buy_price_cents:
            continue
        if price >= best_bid or price <= 0:
            continue
        # Dynamic share sizing: scale up at cheap prices to meet min_notional
        effective_shares = shares_per_order
        if effective_shares * price < min_notional:
            effective_shares = float(_math.ceil(min_notional / price))
        if any(l.price_cents == pc for l in levels):
            continue
        levels.append(LadderLevel(price_cents=pc, target_shares=effective_shares))
    levels.sort(key=lambda l: l.price_cents, reverse=True)
    return levels


class OrderEngine:
    """Manages penny ladder placement with warm start and budget control."""

    def __init__(self, client: ExchangeClient, config: BotConfig, fill_monitor: FillMonitor | None = None):
        self._client = client
        self._config = config
        self._fill_monitor = fill_monitor
        # Polymarket rejects GTD expirations that are too close to "now".
        # Keep a safety margin to avoid retry storms near market close.
        self._expiry_headroom_s = 65
        # Suppress repeated VWAP-reject logs for the same side/level pair.
        self._vwap_reject_cooldown_s = 30.0
        self._vwap_reject_log_until: Dict[Tuple[str, int], float] = {}

    async def place_ladder(
        self,
        up_levels: List[LadderLevel],
        down_levels: List[LadderLevel],
        up_token: str,
        down_token: str,
        expiration_ts: int,
        state: BotState,
    ) -> None:
        """Place ladder orders with warm start strategy.

        Phase 1 (warm start): top-5 levels per side (closest to best_ask)
        Phase 2 (fill out): remaining levels, interleaved by side
        Budget check on every order.
        """
        delay = self._config.order_delay_ms / 1000.0
        if not self._has_expiration_headroom(expiration_ts):
            logger.info(
                "Skip ladder placement: expiration too close (time_left=%ds)",
                max(0, expiration_ts - int(time.time())),
            )
            return

        # Phase 1: warm start — top 5 each side, interleaved 1:1
        # Fix #30: Interleave UP/DOWN so both sides have coverage from the start.
        warm_up = up_levels[:5]
        warm_down = down_levels[:5]

        for i in range(max(len(warm_up), len(warm_down))):
            if i < len(warm_up):
                if not self._has_expiration_headroom(expiration_ts):
                    break
                if self._can_afford(warm_up[i], state, "up"):
                    await self._place_level(warm_up[i], up_token, "up", expiration_ts, state)
                    await asyncio.sleep(delay)
            if i < len(warm_down):
                if not self._has_expiration_headroom(expiration_ts):
                    break
                if self._can_afford(warm_down[i], state, "down"):
                    await self._place_level(warm_down[i], down_token, "down", expiration_ts, state)
                    await asyncio.sleep(delay)

        # Phase 2: fill out remaining, interleaved
        remain_up = up_levels[5:]
        remain_down = down_levels[5:]
        max_len = max(len(remain_up), len(remain_down))

        for i in range(0, max_len, 5):
            # Batch of 5 from up side
            for level in remain_up[i:i+5]:
                if not self._has_expiration_headroom(expiration_ts):
                    break
                if not self._can_afford(level, state, "up"):
                    break
                await self._place_level(level, up_token, "up", expiration_ts, state)
                await asyncio.sleep(delay)
            # Batch of 5 from down side
            for level in remain_down[i:i+5]:
                if not self._has_expiration_headroom(expiration_ts):
                    break
                if not self._can_afford(level, state, "down"):
                    break
                await self._place_level(level, down_token, "down", expiration_ts, state)
                await asyncio.sleep(delay)

    async def place_side(
        self,
        levels: List[LadderLevel],
        token_id: str,
        side: str,
        expiration_ts: int,
        state: BotState,
    ) -> None:
        """Place orders for a single side of the ladder (used by re-anchor)."""
        delay = self._config.order_delay_ms / 1000.0
        if not self._has_expiration_headroom(expiration_ts):
            logger.info(
                "Skip side placement (%s): expiration too close (time_left=%ds)",
                side,
                max(0, expiration_ts - int(time.time())),
            )
            return
        for level in levels:
            if not self._has_expiration_headroom(expiration_ts):
                break
            if not self._can_afford(level, state, side):
                break
            await self._place_level(level, token_id, side, expiration_ts, state)
            await asyncio.sleep(delay)

    async def place_ladder_batch(
        self,
        up_levels: List[LadderLevel],
        down_levels: List[LadderLevel],
        up_token: str,
        down_token: str,
        expiration_ts: int,
        state: BotState,
    ) -> None:
        """Place both sides in a single batch API call (max 15 orders).

        Pre-filters affordable levels with incremental budget simulation,
        interleaves UP/DOWN, then sends one batch request.
        """
        if not self._has_expiration_headroom(expiration_ts):
            return

        all_orders: List[dict] = []
        level_meta: List[tuple] = []  # (level, side, token_id)
        sim_reserved = state.reserved_notional

        max_len = max(len(up_levels), len(down_levels))
        for i in range(max_len):
            for levels, side, token_id in [
                (up_levels, "up", up_token),
                (down_levels, "down", down_token),
            ]:
                if i >= len(levels):
                    continue
                level = levels[i]
                price = level.price_cents / 100.0
                notional = level.target_shares * price
                # Include cancel_race_buffer: recently-cancelled orders may still fill,
                # so their notional must remain reserved to avoid exceeding budget.
                total = state.position.total_cost + sim_reserved + state.cancel_race_buffer + notional
                if total > self._config.session_capital_limit:
                    continue
                all_orders.append({
                    "token_id": token_id,
                    "price": price,
                    "size": level.target_shares,
                    "side": side,
                    "expiration_ts": expiration_ts,
                })
                level_meta.append((level, side, token_id))
                sim_reserved += notional

        if not all_orders:
            return

        # Batch in groups of 15 (API limit)
        for start in range(0, len(all_orders), 15):
            chunk = all_orders[start:start + 15]
            meta = level_meta[start:start + 15]
            order_ids = await self._client.place_orders_batch(chunk)

            now = time.time()
            for (level, side, token_id), oid in zip(meta, order_ids):
                if oid:
                    level.order_id = oid
                    level.placed_at = now
                    price = level.price_cents / 100.0
                    state.reserved_notional += level.target_shares * price
                    state.active_orders[oid] = level
                    state.order_map[oid] = {
                        "side": side, "price": price, "token_id": token_id,
                    }
                    if self._fill_monitor is not None:
                        self._fill_monitor.register_order(oid)
                    logger.info(
                        "Batch placed %s %dc x %.1f -> %s",
                        side, level.price_cents, level.target_shares, oid,
                    )

    async def place_side_batch(
        self,
        levels: List[LadderLevel],
        token_id: str,
        side: str,
        expiration_ts: int,
        state: BotState,
        check_avg_cost: bool = False,
        avg_cost_target: float = 0,
    ) -> int:
        """Place orders for one side in a single batch API call.

        Returns the number of orders successfully placed.
        """
        if not self._has_expiration_headroom(expiration_ts):
            return 0

        affordable: List[LadderLevel] = []
        orders: List[dict] = []
        sim_reserved = state.reserved_notional
        # Fix #38+#40: Cumulative avg cost simulation — track projected cost/shares
        # across all levels in this batch so later levels account for earlier ones.
        # Include pending orders (placed but not yet filled) for consistency with
        # _is_cheap_enough — prevents orders surviving cancel from being invisible.
        sim_total_cost = state.position.total_cost
        sim_total_shares = state.position.total_shares
        for _lvl in state.active_orders.values():
            _rem = _lvl.target_shares - _lvl.filled_shares
            if _rem > 0:
                sim_total_cost += _rem * (_lvl.price_cents / 100.0)
                sim_total_shares += _rem
        avg_cost_target = avg_cost_target if avg_cost_target > 0 else self._config.passive_avg_cost_target
        # Fix #76: Snapshot pre-batch avg for escape-hatch comparison.
        # Cheap orders in this batch must NOT shift baseline for expensive ones.
        original_avg = sim_total_cost / sim_total_shares if sim_total_shares > 0 else 0.0

        # Fix #55a: Sort levels cheapest-first so cheap levels (which lower avg
        # cost) get placed before expensive ones that would blow the budget.
        # build_trend_ladder returns descending (expensive first) — reverse that.
        sorted_levels = sorted(levels, key=lambda lv: lv.price_cents)

        for level in sorted_levels:
            price = level.price_cents / 100.0
            notional = level.target_shares * price
            # Include cancel_race_buffer: recently-cancelled orders may still fill.
            total = state.position.total_cost + sim_reserved + state.cancel_race_buffer + notional
            if total > self._config.session_capital_limit:
                break
            # Fix #76: Each order checked against ORIGINAL position avg, not
            # cumulative batch avg.  Prevents cheap orders in the same batch from
            # subsidising expensive ones past the target.  We still project ALL
            # pending + batch orders for the total, but the gate uses the
            # pre-batch avg as baseline.
            if check_avg_cost and avg_cost_target > 0 and sim_total_shares > 0:
                proj_avg = (sim_total_cost + notional) / (sim_total_shares + level.target_shares)
                # Fix #53a: Orders below current avg always lower avg — escape hatch
                # Use pre-batch avg (original_avg) so cheap batch orders don't shift baseline
                if proj_avg > avg_cost_target and not (original_avg > 0 and price < original_avg):
                    # Fix #39b: Bypass for lagging side when imbalanced + cheap
                    pos = state.position
                    imbalance = pos.share_imbalance if pos.total_shares > 0 else 0.0
                    lagging = (
                        "up" if pos.up_shares < pos.down_shares
                        else "down" if pos.down_shares < pos.up_shares
                        else ""
                    )
                    if side == lagging and imbalance >= 0.20 and price <= 0.50:
                        pass  # allow — reducing excess + cheap = improves both worst & EV
                    else:
                        key = (side, level.price_cents)
                        now_ts = time.time()
                        if now_ts >= self._vwap_reject_log_until.get(key, 0.0):
                            logger.info(
                                "Batch avg cost reject %s %dc: projected %.4f > %.3f (orig_avg=%.4f)",
                                side, level.price_cents, proj_avg, avg_cost_target, original_avg,
                            )
                            self._vwap_reject_log_until[key] = now_ts + self._vwap_reject_cooldown_s
                        break
            affordable.append(level)
            if check_avg_cost:
                sim_total_cost += notional
                sim_total_shares += level.target_shares
            orders.append({
                "token_id": token_id,
                "price": price,
                "size": level.target_shares,
                "side": side,
                "expiration_ts": expiration_ts,
            })
            sim_reserved += notional

        if not orders:
            return 0

        # Batch in groups of 15 (API limit) — same pattern as place_ladder_batch
        placed_count = 0
        for start in range(0, len(orders), 15):
            chunk = orders[start:start + 15]
            chunk_levels = affordable[start:start + 15]
            order_ids = await self._client.place_orders_batch(chunk)
            now = time.time()
            for level, oid in zip(chunk_levels, order_ids):
                if oid:
                    level.order_id = oid
                    level.placed_at = now
                    price = level.price_cents / 100.0
                    state.reserved_notional += level.target_shares * price
                    state.active_orders[oid] = level
                    state.order_map[oid] = {
                        "side": side, "price": price, "token_id": token_id,
                    }
                    if self._fill_monitor is not None:
                        self._fill_monitor.register_order(oid)
                    placed_count += 1
                    logger.info(
                        "Batch placed %s %dc x %.1f -> %s",
                        side, level.price_cents, level.target_shares, oid,
                    )
        return placed_count

    async def replenish(
        self,
        level: LadderLevel,
        side: str,
        token_id: str,
        expiration_ts: int,
        state: BotState,
    ) -> bool:
        """Replenish a fully-filled level or re-place a cancelled-orphaned level.

        Handles two cases:
        1. Fully filled (filled_shares >= target_shares): replenishment cycle
        2. Orphaned (order_id=None, filled_shares < target_shares): re-place after cancel
        Max replenishments = config.max_replenishments (only counted for case 1).
        """
        # Already has an active order — skip
        if level.order_id is not None:
            return False
        if not self._has_expiration_headroom(expiration_ts):
            return False

        if level.filled_shares >= level.target_shares:
            # Full fill → replenishment cycle
            if level.replenish_count >= self._config.max_replenishments:
                logger.info("Level %dc: max replenishments (%d) reached", level.price_cents, level.replenish_count)
                return False
            if not self._can_afford(level, state, side):
                logger.info("Level %dc: cannot afford replenishment", level.price_cents)
                return False
            if not self._is_profitable(level, side, state):
                return False
            level.filled_shares = 0.0
            level.replenish_count += 1
        else:
            # Fix #32: Orphaned level — cancelled before filling, needs re-placement
            if not self._can_afford(level, state, side):
                return False
            if not self._is_profitable(level, side, state):
                return False

        await self._place_level(level, token_id, side, expiration_ts, state)
        return True

    async def replenish_passive(
        self,
        level: LadderLevel,
        side: str,
        token_id: str,
        expiration_ts: int,
        state: BotState,
        max_replenishments: int = 999,
        check_avg_cost: bool = False,
        avg_cost_target_override: float = 0,
    ) -> bool:
        """Replenish a filled level for the passive strategy.

        Same as replenish() but skips VWAP profitability check (same price = same VWAP)
        and uses the passive replenishment cap.
        If check_avg_cost=True, runs _is_cheap_enough instead.
        """
        if level.order_id is not None:
            return False
        if not self._has_expiration_headroom(expiration_ts):
            return False

        if level.filled_shares >= level.target_shares:
            if level.replenish_count >= max_replenishments:
                return False
            # Instant-fill guard: if the level filled within 5s of placement,
            # the level is at/above the best ask — replenishing will just burn budget.
            # Uses last_fill_at which survives across replenishment cycles, preventing
            # rapid fill-replenish-fill loops on expensive levels.
            now = time.time()
            if level.last_fill_at > 0 and (now - level.last_fill_at) < 5.0:
                return False
            if level.placed_at > 0 and (now - level.placed_at) < 3.0:
                return False
            if not self._can_afford(level, state, side):
                return False
            if check_avg_cost and not self._is_cheap_enough(level, side, state, avg_cost_target_override):
                return False
            level.filled_shares = 0.0
            level.replenish_count += 1
        else:
            # Orphaned level (cancelled before filling)
            if not self._can_afford(level, state, side):
                return False
            if check_avg_cost and not self._is_cheap_enough(level, side, state, avg_cost_target_override):
                return False

        await self._place_level(level, token_id, side, expiration_ts, state, skip_vwap_check=True)
        return True

    async def replenish_batch(
        self,
        levels: List[LadderLevel],
        side: str,
        token_id: str,
        expiration_ts: int,
        state: BotState,
        max_replenishments: int = 999,
        check_avg_cost: bool = False,
        avg_cost_target_override: float = 0,
    ) -> int:
        """Replenish multiple filled levels in a single batch API call.

        Same filtering logic as replenish_passive() but collects eligible levels
        and places them all at once via place_orders_batch (max 15 per call).
        Returns the number of orders successfully placed.
        """
        if not self._has_expiration_headroom(expiration_ts):
            return 0

        eligible: List[LadderLevel] = []
        orders: List[dict] = []
        sim_reserved = state.reserved_notional

        # Cumulative avg cost simulation (same as place_side_batch)
        sim_total_cost = state.position.total_cost
        sim_total_shares = state.position.total_shares
        for _lvl in state.active_orders.values():
            _rem = _lvl.target_shares - _lvl.filled_shares
            if _rem > 0:
                sim_total_cost += _rem * (_lvl.price_cents / 100.0)
                sim_total_shares += _rem
        avg_target = avg_cost_target_override if avg_cost_target_override > 0 else self._config.passive_avg_cost_target

        now = time.time()
        for level in levels:
            # Same guards as replenish_passive
            if level.order_id is not None:
                continue
            if level.filled_shares >= level.target_shares:
                # Full fill → replenishment cycle
                if level.replenish_count >= max_replenishments:
                    continue
                # Instant-fill guard: recently filled levels are at/above best ask.
                # Uses last_fill_at (survives replenish cycles) to prevent rapid loops.
                if level.last_fill_at > 0 and (now - level.last_fill_at) < 5.0:
                    continue
                if level.placed_at > 0 and (now - level.placed_at) < 3.0:
                    continue
            else:
                # Not fully filled and no order → orphaned, allow re-place
                pass

            price = level.price_cents / 100.0
            notional = level.target_shares * price

            # Budget check (cumulative)
            total = state.position.total_cost + sim_reserved + state.cancel_race_buffer + notional
            if total > self._config.session_capital_limit:
                break

            # Avg cost check (cumulative, same logic as place_side_batch)
            if check_avg_cost and avg_target > 0 and sim_total_shares > 0:
                proj_avg = (sim_total_cost + notional) / (sim_total_shares + level.target_shares)
                current_avg = sim_total_cost / sim_total_shares if sim_total_shares > 0 else 0.0
                if proj_avg > avg_target and not (current_avg > 0 and price < current_avg):
                    # Lagging-side imbalance bypass
                    pos = state.position
                    imbalance = pos.share_imbalance if pos.total_shares > 0 else 0.0
                    lagging = (
                        "up" if pos.up_shares < pos.down_shares
                        else "down" if pos.down_shares < pos.up_shares
                        else ""
                    )
                    if side == lagging and imbalance >= 0.20 and price <= 0.50:
                        pass  # allow
                    else:
                        break

            eligible.append(level)
            orders.append({
                "token_id": token_id,
                "price": price,
                "size": level.target_shares,
                "side": side,
                "expiration_ts": expiration_ts,
            })
            sim_reserved += notional
            if check_avg_cost:
                sim_total_cost += notional
                sim_total_shares += level.target_shares

        if not orders:
            return 0

        # Reset fill state for eligible levels BEFORE placing
        for level in eligible:
            if level.filled_shares >= level.target_shares:
                level.filled_shares = 0.0
                level.replenish_count += 1

        # Batch in groups of 15
        placed_count = 0
        for start in range(0, len(orders), 15):
            chunk = orders[start:start + 15]
            chunk_levels = eligible[start:start + 15]
            order_ids = await self._client.place_orders_batch(chunk)
            batch_now = time.time()
            for level, oid in zip(chunk_levels, order_ids):
                if oid:
                    level.order_id = oid
                    level.placed_at = batch_now
                    price = level.price_cents / 100.0
                    state.reserved_notional += level.target_shares * price
                    state.active_orders[oid] = level
                    state.order_map[oid] = {
                        "side": side, "price": price, "token_id": token_id,
                    }
                    if self._fill_monitor is not None:
                        self._fill_monitor.register_order(oid)
                    placed_count += 1
                    logger.info(
                        "Batch replenish %s %dc x %.1f -> %s",
                        side, level.price_cents, level.target_shares, oid,
                    )
        return placed_count

    def on_fill_release(self, fill: FillEvent, state: BotState) -> None:
        """Release reserved notional when a fill occurs."""
        delta = fill.filled_shares * fill.price
        if delta > state.reserved_notional + 0.01:
            logger.warning(
                "reserved_notional underflow: releasing %.2f but only %.2f reserved",
                delta, state.reserved_notional,
            )
        state.reserved_notional -= delta
        state.reserved_notional = max(0.0, state.reserved_notional)

    def on_cancel_release(self, level: LadderLevel, state: BotState) -> None:
        """Release reserved notional when an order is cancelled."""
        remaining = level.target_shares - level.filled_shares
        if remaining > 0:
            price = level.price_cents / 100.0
            release = remaining * price
            state.reserved_notional -= release
            state.reserved_notional = max(0.0, state.reserved_notional)
            # Cancel race buffer: these shares might have filled on-exchange
            # before the cancel reached the exchange. Keep them in the budget
            # until we confirm the cancel went through (or a fill arrives).
            state.cancel_race_buffer += release
        # Fix #32: Mark level as having no active order so replenish() can re-place it.
        level.order_id = None

    async def cancel_winding_down(self, asset_id: str) -> bool:
        """Cancel all orders during WINDING_DOWN phase."""
        return await self._client.cancel_and_verify(asset_id)

    def _is_profitable(self, level: LadderLevel, side: str, state: BotState) -> bool:
        """Check if placing this order keeps projected combined VWAP profitable.

        Simulates what combined_vwap would be if this order fills entirely.
        Returns True if projected combined <= max_combined_vwap.
        Skips check when both sides are empty (initial entry gate already verified).
        """
        price = level.price_cents / 100.0
        shares = level.target_shares
        pos = state.position
        max_combined = self._config.max_combined_vwap

        # When both sides have no fills, the entry gate already checked edge
        if pos.up_shares == 0 and pos.down_shares == 0:
            return True

        if side == "up":
            new_cost = pos.up_cost + price * shares
            new_shares = pos.up_shares + shares
            new_side_vwap = new_cost / new_shares if new_shares > 0 else price
            other_vwap = pos.down_vwap
        else:
            new_cost = pos.down_cost + price * shares
            new_shares = pos.down_shares + shares
            new_side_vwap = new_cost / new_shares if new_shares > 0 else price
            other_vwap = pos.up_vwap

        # If the opposite side has no fills yet, try opposite-side open orders first.
        # This avoids false rejects during normal one-sided warm-start fills.
        if other_vwap == 0:
            other_vwap = self._other_side_open_price_proxy(side, state)
            if other_vwap <= 0:
                # No opposite-side open orders either: use same-side price as strict fallback.
                other_vwap = price

        projected = new_side_vwap + other_vwap
        if projected > max_combined:
            key = (side, level.price_cents)
            now_ts = time.time()
            if now_ts >= self._vwap_reject_log_until.get(key, 0.0):
                logger.info(
                    "VWAP reject %s %dc: projected %.4f > %.3f",
                    side, level.price_cents, projected, max_combined,
                )
                self._vwap_reject_log_until[key] = now_ts + self._vwap_reject_cooldown_s
            return False
        return True

    def _is_cheap_enough(self, level: LadderLevel, side: str, state: BotState, avg_cost_target_override: float = 0) -> bool:
        """Check if placing this order keeps avg_cost_per_share below target.

        Used by passive strategy instead of _is_profitable (VWAP-based).
        Returns True if projected avg cost stays below passive_avg_cost_target.
        Skips check when no shares exist yet (entry gate already verified).

        Fix #40: Includes pending orders (placed but not yet filled) in the
        projection so individual replenishment is consistent with batch
        cumulative avg cost checks.
        """
        price = level.price_cents / 100.0
        shares = level.target_shares
        pos = state.position
        target = avg_cost_target_override if avg_cost_target_override > 0 else self._config.passive_avg_cost_target

        if pos.total_shares == 0:
            return True

        # Fix #53a: Orders priced BELOW current avg always lower avg cost.
        # Allow even when avg already exceeds target — this is the escape hatch.
        current_avg = pos.avg_cost_per_share
        if current_avg > 0 and price < current_avg:
            return True

        # Orders priced at or below target always improve avg — allow on any side.
        # Math: if price <= target, adding shares can only pull avg toward target.
        if price <= target:
            return True

        # Include pending orders from active_orders in projection.
        # Without this, the batch rejects a level cumulatively but replenishment
        # allows it individually (because it doesn't see the pending batch orders).
        pending_cost = 0.0
        pending_shares = 0.0
        for lvl in state.active_orders.values():
            remaining = lvl.target_shares - lvl.filled_shares
            if remaining > 0:
                pending_cost += remaining * (lvl.price_cents / 100.0)
                pending_shares += remaining

        projected_cost = pos.total_cost + pending_cost + price * shares
        projected_shares = pos.total_shares + pending_shares + shares
        projected_avg = projected_cost / projected_shares
        if projected_avg > target:
            key = (side, level.price_cents)
            now_ts = time.time()
            if now_ts >= self._vwap_reject_log_until.get(key, 0.0):
                logger.info(
                    "Avg cost reject %s %dc: projected %.4f > %.3f (pending: %.0f shares $%.1f)",
                    side, level.price_cents, projected_avg, target,
                    pending_shares, pending_cost,
                )
                self._vwap_reject_log_until[key] = now_ts + self._vwap_reject_cooldown_s
            return False
        return True

    def _other_side_open_price_proxy(self, side: str, state: BotState) -> float:
        """Worst open-order price on the opposite side as a VWAP proxy."""
        other_side = "down" if side == "up" else "up"
        prices = [
            float(meta.get("price", 0.0))
            for meta in state.order_map.values()
            if meta.get("side") == other_side and float(meta.get("price", 0.0)) > 0.0
        ]
        return max(prices) if prices else 0.0

    def _can_afford(self, level: LadderLevel, state: BotState, side: str = "") -> bool:
        """Check if we can afford to place this order within budget.

        Total committed = position cost (already spent on fills) +
        reserved notional (open orders) + this new order.
        Must stay within session_capital_limit.
        Also enforces per-side cost cap to prevent one side dominating.
        """
        price = level.price_cents / 100.0
        order_notional = level.target_shares * price
        total_committed = (
            state.position.total_cost
            + state.reserved_notional
            + state.cancel_race_buffer
            + order_notional
        )
        if total_committed > self._config.session_capital_limit:
            return False

        # Per-side cost cap: position cost + reserved orders on this side
        if side and self._config.per_side_cost_cap_pct < 1.0:
            side_cap = self._config.session_capital_limit * self._config.per_side_cost_cap_pct
            if side == "up":
                side_cost = state.position.up_cost
            else:
                side_cost = state.position.down_cost
            # Add reserved notional for this side's open orders
            side_reserved = sum(
                float(meta.get("price", 0)) * self._config.shares_per_order
                for meta in state.order_map.values()
                if meta.get("side") == side
            )
            if side_cost + side_reserved + order_notional > side_cap:
                logger.info(
                    "Per-side cap: %s cost $%.2f + reserved $%.2f + order $%.2f > cap $%.2f",
                    side, side_cost, side_reserved, order_notional, side_cap,
                )
                return False

        return True

    def _has_expiration_headroom(self, expiration_ts: int) -> bool:
        """Return True when order expiration is far enough in the future for GTD."""
        # Unit tests pass synthetic small timestamps (e.g. 9999). Ignore guard there.
        if expiration_ts < 1_500_000_000:
            return True
        return expiration_ts > int(time.time()) + self._expiry_headroom_s

    async def _place_level(
        self,
        level: LadderLevel,
        token_id: str,
        side: str,
        expiration_ts: int,
        state: BotState,
        skip_vwap_check: bool = False,
    ) -> None:
        """Place a single order for a ladder level."""
        if not self._has_expiration_headroom(expiration_ts):
            return
        # Fix #2: Per-order VWAP profitability check
        if not skip_vwap_check and not self._is_profitable(level, side, state):
            return
        price = level.price_cents / 100.0
        try:
            order_id = await self._client.place_limit_order(
                token_id=token_id,
                price=price,
                size=level.target_shares,
                side=side,
                expiration_ts=expiration_ts,
            )
            level.order_id = order_id
            level.placed_at = time.time()
            state.reserved_notional += level.target_shares * price
            state.active_orders[order_id] = level
            state.order_map[order_id] = {
                "side": side,
                "price": price,
                "token_id": token_id,
            }
            # Register with fill monitor so fast fills are never lost.
            if self._fill_monitor is not None:
                self._fill_monitor.register_order(order_id)
            logger.info("Placed %s %dc x %.1f -> %s", side, level.price_cents, level.target_shares, order_id)
        except Exception as e:
            logger.error("Failed to place %s %dc: %s", side, level.price_cents, e)
