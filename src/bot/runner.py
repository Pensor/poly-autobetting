"""Main orchestrator: wires all modules into a single-writer event loop.

Architecture:
- BotRunner owns the main loop, all state mutations happen here
- FillMonitor runs as a background task, publishes to async queue
- Runner drains queue and updates PositionTracker (single-writer pattern)
- RiskEngine checks are evaluated every iteration
"""

from __future__ import annotations

import asyncio
from collections import Counter
import logging
import math
import time
import uuid
from typing import Optional

from src.bot.types import (
    BotConfig, BotState, EdgeTier, LadderLevel, MarketInfo, MarketPhase, PositionState,
)
from src.bot.order_engine import build_ladder, build_full_range_ladder, build_trend_ladder, OrderEngine
from src.bot.math_engine import clamp_price, estimate_ladder_edge, estimate_taker_fee, recalculate_live_edge, round_to_tick
from src.bot.market_scheduler import _get_effective_price, _get_best_bid_price
from src.bot.fill_monitor import FillMonitor, OrderReconciler
from src.bot.risk_engine import RiskAction, RiskEngine, RiskVerdict
from src.bot.position_tracker import PositionTracker
from src.bot.state_manager import StateManager
from src.bot.alerts import AlertManager
from src.bot.market_scheduler import MarketScheduler
from src.bot.client import ExchangeClient
from src.bot.ws_book_feed import WSBookFeed
from src.bot.session_loop import (
    drain_fills, setup_fill_monitor, cleanup_session, check_kill_command,
    mark_orders_cancelled, cleanup_cancelled_orders,
)
from src.bot.rebalance import (
    ImbalanceAction, check_imbalance, candidate_has_vwap_headroom,
    should_reanchor_side,
)

logger = logging.getLogger(__name__)


class BotRunner:
    """Main orchestrator for the penny-ladder market-making bot."""

    def __init__(
        self,
        client: ExchangeClient,
        config: BotConfig,
        state_manager: StateManager,
        alert_manager: AlertManager,
        maker_address: str = "",
    ):
        self._client = client
        self._config = config
        self._state_manager = state_manager
        self._alerts = alert_manager
        self._maker_address = maker_address
        self._risk_engine = RiskEngine(config)
        self._scheduler = MarketScheduler(client, config)
        self._tracker = PositionTracker()
        self._ws_feed: Optional[WSBookFeed] = None
        self._shutdown = False
        self._last_risk_action = RiskAction.NONE
        self._last_risk_reason = ""
        self._last_cancel_all_ts = 0.0
        self._daily_pnl = 0.0
        self._hourly_pnl = 0.0
        self._hourly_pnl_start_ts = 0.0
        self._daily_markets = 0
        self._run_started_ts = 0.0
        self._no_edge_cooldown_until: dict[str, float] = {}
        # Fix #55c: Reconciliation debounce — require 2 consistent readings
        # before applying a correction. Prevents flip-flop from laggy endpoint.
        self._pending_reconcile: Optional[PositionState] = None

    async def _cancel_orphaned_orders(
        self, market: "MarketInfo", max_attempts: int = 12, base_delay: float = 2.0,
    ) -> None:
        """Fix #42: Retry cancelling orphaned orders until API responds.

        After a placement timeout the orders may be live on the exchange.
        Retry cancel with exponential backoff (2s→4s→8s…capped at 15s)
        so we stop the bleeding as soon as the API recovers.
        """
        for attempt in range(max_attempts):
            try:
                await self._client.cancel_and_verify(market.up_token)
                await self._client.cancel_and_verify(market.down_token)
                logger.info(
                    "Orphaned order cleanup succeeded (attempt %d/%d)",
                    attempt + 1, max_attempts,
                )
                return
            except Exception as e:
                delay = min(base_delay * (2 ** attempt), 15.0)
                logger.warning(
                    "Orphaned order cancel attempt %d/%d failed (retry in %.0fs): %s",
                    attempt + 1, max_attempts, delay, e,
                )
                await asyncio.sleep(delay)
        logger.error(
            "Failed to cancel orphaned orders after %d attempts — "
            "orders may still be live on exchange!",
            max_attempts,
        )

    @staticmethod
    def _flush_fill_queue(fill_queue: asyncio.Queue) -> int:
        """Discard all pending fills from queue after reconciliation overwrites tracker.

        Returns the number of flushed fills.
        """
        flushed = 0
        while not fill_queue.empty():
            try:
                fill_queue.get_nowait()
                flushed += 1
            except asyncio.QueueEmpty:
                break
        if flushed:
            logger.warning("Flushed %d stale fills from queue after reconciliation", flushed)
        return flushed

    @staticmethod
    def _is_finite_number(value: object) -> bool:
        return isinstance(value, (int, float)) and math.isfinite(float(value))

    @staticmethod
    def _project_cost(
        target_shares: float,
        tracked_shares: float,
        tracked_cost: float,
        trade_position: tuple[float, float] | None,
    ) -> float:
        """Project side cost for reconciled shares using best available VWAP."""
        if trade_position and abs(trade_position[0]) > 1e-9:
            trade_vwap = trade_position[1] / trade_position[0]
            return target_shares * trade_vwap
        if abs(tracked_shares) > 1e-9:
            tracked_vwap = tracked_cost / tracked_shares
            return target_shares * tracked_vwap
        # Fallback: if we have shares but no cost data (e.g. session restart
        # after crash), use 0.50 as conservative VWAP (break-even assumption).
        # This prevents the EV delusion where shares appear "free" (cost=0).
        if abs(target_shares) > 1e-9:
            return target_shares * 0.50
        return 0.0

    @staticmethod
    def _price_to_cents(value: object) -> int:
        try:
            return int(round(float(value) * 100.0))
        except Exception:
            return -1

    @classmethod
    def _plan_smart_keep_ids(
        cls,
        order_map: dict[str, dict],
        side: str,
        target_levels: list,
    ) -> set[str]:
        """Pick local orders to keep when re-anchoring.

        Keeps at most the desired count per target price level.
        """
        desired_by_price: Counter[int] = Counter(
            int(getattr(level, "price_cents", -1)) for level in target_levels
        )
        keep_by_price: Counter[int] = Counter()
        keep_ids: set[str] = set()

        for oid, meta in order_map.items():
            if meta.get("side") != side:
                continue
            price_cents = cls._price_to_cents(meta.get("price", 0.0))
            if desired_by_price.get(price_cents, 0) <= 0:
                continue
            if keep_by_price[price_cents] >= desired_by_price[price_cents]:
                continue
            keep_ids.add(oid)
            keep_by_price[price_cents] += 1

        return keep_ids

    async def _fetch_trade_position(
        self,
        token_id: str,
        after_ts: int,
    ) -> tuple[float, float] | None:
        """Rebuild side shares/cost from exchange trade history for this maker."""
        if not self._maker_address:
            return None
        try:
            fills = await self._client.get_trades(
                token_id,
                max(0, after_ts),
                maker_address=self._maker_address,
            )
        except Exception as exc:
            logger.warning("Trade position fetch failed for %s: %s", token_id, exc)
            return None
        if not isinstance(fills, list):
            return None

        shares = 0.0
        cost = 0.0
        for fill in fills:
            try:
                shares += float(getattr(fill, "filled_shares", 0.0) or 0.0)
                cost += float(getattr(fill, "usdc_cost", 0.0) or 0.0)
            except Exception:
                continue
        return (shares, cost)

    async def _reconcile_via_orders(
        self,
        all_placed_orders: dict[str, dict],
        state: BotState,
    ) -> None:
        """Reconcile tracker by querying final size_matched on every placed order.

        This is more reliable than the trades API (which has indexing lag) or the
        balance endpoint (which may have settlement delays).
        """
        if not all_placed_orders:
            return

        up_shares = 0.0
        up_cost = 0.0
        down_shares = 0.0
        down_cost = 0.0
        queried = 0
        errors = 0

        for oid, meta in all_placed_orders.items():
            side = meta.get("side", "")
            price = float(meta.get("price", 0))
            if side not in ("up", "down") or price <= 0:
                continue
            try:
                order = await self._client.get_order(oid)
                matched = float(order.get("size_matched", 0) or 0)
                queried += 1
            except Exception:
                errors += 1
                continue
            if matched <= 0:
                continue
            cost = matched * price
            if side == "up":
                up_shares += matched
                up_cost += cost
            else:
                down_shares += matched
                down_cost += cost

        # If too many errors, don't trust the exchange totals
        if queried == 0:
            logger.warning(
                "Order-based reconciliation skipped: queried=0 errors=%d — keeping tracker as-is",
                errors,
            )
            return

        tracked = self._tracker.position
        tol = 0.5  # shares tolerance (4.99 vs 5.00)

        drift_up = abs(up_shares - tracked.up_shares)
        drift_down = abs(down_shares - tracked.down_shares)

        if drift_up > tol or drift_down > tol:
            logger.warning(
                "Order-based reconciliation: DRIFT detected "
                "tracker=(up=%.1f/%.2f down=%.1f/%.2f) vs orders=(up=%.1f/%.2f down=%.1f/%.2f). Updating.",
                tracked.up_shares, tracked.up_cost,
                tracked.down_shares, tracked.down_cost,
                up_shares, up_cost, down_shares, down_cost,
            )
            self._tracker.position = PositionState(
                up_shares=up_shares,
                up_cost=up_cost,
                down_shares=down_shares,
                down_cost=down_cost,
            )
            state.position = self._tracker.position
        else:
            logger.info(
                "Order-based reconciliation OK: up=%.1f down=%.1f (queried=%d errors=%d)",
                up_shares, down_shares, queried, errors,
            )

    async def _snapshot_usdc_balance(self) -> Optional[float]:
        """Fetch current USDC balance for session-level cost cross-check."""
        try:
            bal = await self._client.get_balance()
            logger.info("USDC balance snapshot: $%.2f", bal)
            return bal
        except Exception:
            logger.debug("Could not snapshot USDC balance", exc_info=True)
            return None

    async def _reconcile_live_position(
        self,
        market: MarketInfo,
        state: BotState,
        *,
        reason: str,
        force: bool = False,
    ) -> bool:
        """Reconcile tracker position to exchange state (on-chain balance or trades fallback)."""
        tracked = self._tracker.position
        tol_shares = max(0.0, self._config.position_reconcile_tolerance_shares)
        tol_cost = max(0.0, self._config.position_reconcile_tolerance_cost)

        up_raw = float("nan")
        down_raw = float("nan")
        try:
            up_raw = await self._client.get_position(market.up_token)
            down_raw = await self._client.get_position(market.down_token)
        except Exception as exc:
            # Fix #52D: Downgrade to DEBUG — 404 is normal for new/empty positions
            logger.debug("Live position endpoint fetch failed: %s", exc)

        endpoint_ok = self._is_finite_number(up_raw) and self._is_finite_number(down_raw)

        trade_after_ts = max(0, int(market.open_ts) - 900)
        up_trade_pos = await self._fetch_trade_position(market.up_token, trade_after_ts)
        down_trade_pos = await self._fetch_trade_position(market.down_token, trade_after_ts)
        trades_ok = up_trade_pos is not None and down_trade_pos is not None

        if endpoint_ok:
            up_shares = float(up_raw)
            down_shares = float(down_raw)

            # Fix #54: Sanity guard — reject wildly implausible jumps.
            # A position can't grow more than 10x the capital limit in shares
            # (e.g. $1000 / $0.01 = 100k shares max). If raw value is way
            # beyond tracked + capital headroom, the endpoint returned garbage
            # (e.g. atomic units not divided by 1e6).
            _max_plausible = max(
                tracked.up_shares + tracked.down_shares + 1000,
                self._config.session_capital_limit / 0.01,
            )
            if up_shares > _max_plausible or down_shares > _max_plausible:
                logger.warning(
                    "Position endpoint returned implausible values (%s): "
                    "up=%.0f down=%.0f (max plausible=%.0f) — skipping",
                    reason, up_shares, down_shares, _max_plausible,
                )
                endpoint_ok = False
            else:
                # Fix #67: Guard against one-sided wipeouts from the positions endpoint.
                #
                # We've observed the endpoint occasionally returning 0.0 for one token
                # while the other token remains correct. Accepting that reading can
                # nuke the tracker (e.g. UP 500->0), blow up risk stats, and trigger
                # bad downstream decisions. This bot is buy-only, so a sudden
                # one-sided wipeout is almost always endpoint noise.
                _min_meaningful = max(self._config.shares_per_order * 3.0, 25.0)
                _near_zero = max(tol_shares, 0.01)

                def _is_one_sided_wipeout(new_shares: float, old_shares: float, other_new: float) -> bool:
                    return (
                        new_shares <= _near_zero
                        and old_shares >= _min_meaningful
                        and other_new >= _min_meaningful
                    )

                if _is_one_sided_wipeout(up_shares, tracked.up_shares, down_shares):
                    logger.warning(
                        "Position endpoint suspicious wipeout (%s): up %.1f->%.1f while down=%.1f — ignoring endpoint",
                        reason, tracked.up_shares, up_shares, down_shares,
                    )
                    endpoint_ok = False
                elif _is_one_sided_wipeout(down_shares, tracked.down_shares, up_shares):
                    logger.warning(
                        "Position endpoint suspicious wipeout (%s): down %.1f->%.1f while up=%.1f — ignoring endpoint",
                        reason, tracked.down_shares, down_shares, up_shares,
                    )
                    endpoint_ok = False

            if endpoint_ok:
                up_cost = self._project_cost(
                    up_shares, tracked.up_shares, tracked.up_cost, up_trade_pos
                )
                down_cost = self._project_cost(
                    down_shares, tracked.down_shares, tracked.down_cost, down_trade_pos
                )
                source = "positions"

        if endpoint_ok:
            pass  # already handled above
        elif trades_ok:
            # Be conservative: empty trade history is not authoritative when local
            # tracker already has inventory (could be API lag or temporary failure).
            if (
                abs(up_trade_pos[0]) + abs(down_trade_pos[0]) <= 1e-9
                and tracked.total_shares > tol_shares
            ):
                # Fix #52D: Downgrade to DEBUG — empty trades with active tracker is expected lag
                logger.debug(
                    "Trade-based position sync skipped (%s): empty trade history with non-zero tracker",
                    reason,
                )
                return False
            up_shares, up_cost = up_trade_pos
            down_shares, down_cost = down_trade_pos
            # Strategy places buy-only inventory. Trade-based periodic sync should
            # not invent negative or backward inventory/cost moves.
            if (
                up_shares < -tol_shares
                or down_shares < -tol_shares
                or up_cost < -tol_cost
                or down_cost < -tol_cost
            ):
                # Fix #52D: Downgrade to DEBUG — negative values from API lag/inconsistency
                logger.debug(
                    "Trade-based position sync skipped (%s): negative inventory/cost "
                    "(up=%.3f down=%.3f cost_up=%.3f cost_down=%.3f)",
                    reason,
                    up_shares,
                    down_shares,
                    up_cost,
                    down_cost,
                )
                return False
            if not force and (
                up_shares + tol_shares < tracked.up_shares
                or down_shares + tol_shares < tracked.down_shares
                or up_cost + tol_cost < tracked.up_cost
                or down_cost + tol_cost < tracked.down_cost
            ):
                # Fix #52D: Downgrade to DEBUG — non-monotonic is expected API limitation
                logger.debug(
                    "Trade-based position sync skipped (%s): non-monotonic move "
                    "(up %.3f->%.3f down %.3f->%.3f cost_up %.3f->%.3f cost_down %.3f->%.3f)",
                    reason,
                    tracked.up_shares,
                    up_shares,
                    tracked.down_shares,
                    down_shares,
                    tracked.up_cost,
                    up_cost,
                    tracked.down_cost,
                    down_cost,
                )
                return False
            source = "trades"
        else:
            if force:
                logger.warning(
                    "Position sync unavailable (%s): no endpoint or trade source",
                    reason,
                )
            return False

        # Fix #56 (rev): Allow periodic reconciliation to correct *downward* drift.
        #
        # Rationale:
        # - We used to apply a monotonic floor here (never decrease shares/cost)
        #   because on-chain settlement can lag right after fills.
        # - However, FillMonitor can over-count due to non-monotonic API snapshots
        #   (phantom fills), which makes worst_case too negative and can trigger
        #   unnecessary taker rebalancing.
        # - The caller already waits a settle window after the last fill, and we
        #   debounce below, so it's safe to allow downward corrections.
        if not force:
            if source == "positions":
                # Positions source has no cost data; keep cost stable when moving
                # upward, but scale cost when moving downward (phantom-fill cleanup).
                _up_delta = up_shares - tracked.up_shares
                if _up_delta >= -tol_shares:
                    # Upward or ~flat: preserve tracked cost basis, only add delta at tracked VWAP.
                    if tracked.up_shares > 0.01:
                        if _up_delta > 0.01:
                            _up_vwap = tracked.up_cost / tracked.up_shares
                            up_cost = tracked.up_cost + _up_delta * _up_vwap
                        else:
                            up_cost = tracked.up_cost
                    else:
                        up_cost = max(up_cost, tracked.up_cost)
                else:
                    # Downward correction: project cost proportionally.
                    up_cost = self._project_cost(
                        up_shares, tracked.up_shares, tracked.up_cost, up_trade_pos
                    )

                _down_delta = down_shares - tracked.down_shares
                if _down_delta >= -tol_shares:
                    if tracked.down_shares > 0.01:
                        if _down_delta > 0.01:
                            _down_vwap = tracked.down_cost / tracked.down_shares
                            down_cost = tracked.down_cost + _down_delta * _down_vwap
                        else:
                            down_cost = tracked.down_cost
                    else:
                        down_cost = max(down_cost, tracked.down_cost)
                else:
                    down_cost = self._project_cost(
                        down_shares, tracked.down_shares, tracked.down_cost, down_trade_pos
                    )
            else:
                # Trade-based source includes cost; non-monotonic downward moves are
                # already rejected above unless force=True, so keep as-is here.
                pass

        new_position = PositionState(
            up_shares=up_shares,
            down_shares=down_shares,
            up_cost=up_cost,
            down_cost=down_cost,
        )

        changed = (
            abs(new_position.up_shares - tracked.up_shares) > tol_shares
            or abs(new_position.down_shares - tracked.down_shares) > tol_shares
            or abs(new_position.up_cost - tracked.up_cost) > tol_cost
            or abs(new_position.down_cost - tracked.down_cost) > tol_cost
        )

        if not changed:
            if force:
                logger.info(
                    "Position baseline sync (%s/%s): up=%.1f down=%.1f",
                    reason,
                    source,
                    new_position.up_shares,
                    new_position.down_shares,
                )
                self._tracker.position = new_position
                state.position = self._tracker.position
            # Reading matches tracker — clear any pending debounce
            self._pending_reconcile = None
            return False

        # Fix #55c: Debounce — require 2 consistent readings before applying
        # a periodic correction. Prevents flip-flop from laggy/noisy endpoint.
        if not force:
            pending = self._pending_reconcile
            if pending is not None:
                # Check if new reading is consistent with pending
                consistent = (
                    abs(new_position.up_shares - pending.up_shares) <= tol_shares
                    and abs(new_position.down_shares - pending.down_shares) <= tol_shares
                )
                if consistent:
                    # Two consistent readings — apply the correction
                    logger.warning(
                        "Position reconcile (%s/%s): confirmed after debounce — "
                        "up %.1f->%.1f down %.1f->%.1f "
                        "cost_up %.2f->%.2f cost_down %.2f->%.2f",
                        reason,
                        source,
                        tracked.up_shares,
                        new_position.up_shares,
                        tracked.down_shares,
                        new_position.down_shares,
                        tracked.up_cost,
                        new_position.up_cost,
                        tracked.down_cost,
                        new_position.down_cost,
                    )
                    self._pending_reconcile = None
                    self._tracker.position = new_position
                    state.position = self._tracker.position
                    return True
                else:
                    # Inconsistent with pending — replace pending with new reading
                    logger.debug(
                        "Position reconcile debounce (%s): reading changed, resetting "
                        "(pending up=%.1f/down=%.1f, new up=%.1f/down=%.1f)",
                        reason,
                        pending.up_shares, pending.down_shares,
                        new_position.up_shares, new_position.down_shares,
                    )
                    self._pending_reconcile = new_position
                    return False
            else:
                # First divergent reading — store as pending, don't apply yet
                logger.debug(
                    "Position reconcile debounce (%s): storing pending correction "
                    "up %.1f->%.1f down %.1f->%.1f",
                    reason,
                    tracked.up_shares, new_position.up_shares,
                    tracked.down_shares, new_position.down_shares,
                )
                self._pending_reconcile = new_position
                return False

        # force=True — apply immediately (session start, explicit reconcile)
        logger.warning(
            "Position reconcile (%s/%s): up %.1f->%.1f down %.1f->%.1f "
            "cost_up %.2f->%.2f cost_down %.2f->%.2f",
            reason,
            source,
            tracked.up_shares,
            new_position.up_shares,
            tracked.down_shares,
            new_position.down_shares,
            tracked.up_cost,
            new_position.up_cost,
            tracked.down_cost,
            new_position.down_cost,
        )
        self._pending_reconcile = None
        self._tracker.position = new_position
        state.position = self._tracker.position
        return True

    async def run(self) -> None:
        """Main loop: discover markets, gate entry, run sessions."""
        if not self._state_manager.acquire_lock():
            logger.error("Could not acquire lock -- another instance running?")
            return

        try:
            self._run_started_ts = time.time()
            if self._hourly_pnl_start_ts == 0.0:
                self._hourly_pnl_start_ts = time.time()
            await self._scheduler.sync_time()

            # Fix #42: Cancel any orphaned orders from previous crash/session.
            # If the bot crashed mid-placement, orders may be live with no tracking.
            try:
                await self._client.cancel_all()
                logger.info("Startup cleanup: cancelled all open orders")
            except Exception as e:
                logger.warning("Startup cleanup cancel_all failed: %s", e)

            # Initialize WS book feed if client supports it
            if hasattr(self._client, '_ws_feed') and self._client._ws_feed is None:
                try:
                    self._ws_feed = WSBookFeed()
                    self._client._ws_feed = self._ws_feed
                    logger.info("WS book feed initialized")
                except Exception as e:
                    logger.warning("WS book feed init failed (REST fallback): %s", e)
                    self._ws_feed = None

            while not self._shutdown:
                # Fix #12: Hourly PnL reset
                if time.time() - self._hourly_pnl_start_ts >= 3600:
                    self._hourly_pnl = 0.0
                    self._hourly_pnl_start_ts = time.time()
                if self._config.max_run_minutes > 0:
                    elapsed_min = (time.time() - self._run_started_ts) / 60.0
                    if elapsed_min >= self._config.max_run_minutes:
                        logger.info(
                            "Reached max_run_minutes=%.1f, stopping",
                            self._config.max_run_minutes,
                        )
                        self._shutdown = True
                        break

                market = await self._scheduler.discover_next_market()
                if not market:
                    await asyncio.sleep(5)
                    continue

                now_adj = self._scheduler.adjusted_now()
                cooldown_until = self._no_edge_cooldown_until.get(market.condition_id, 0.0)
                if cooldown_until > now_adj:
                    remaining = cooldown_until - now_adj
                    logger.debug(
                        "Cooldown: skip market %s for %.0fs",
                        market.condition_id[:16], remaining,
                    )
                    await asyncio.sleep(min(60.0, max(0.5, remaining)))
                    continue

                phase = self._scheduler.get_current_phase(market)
                logger.info(
                    "Market %s phase=%s open_ts=%d close_ts=%d",
                    market.condition_id[:16], phase.value,
                    int(market.open_ts), int(market.close_ts),
                )

                # If market is already closed or winding down, skip it
                if phase in (MarketPhase.CLOSED, MarketPhase.WINDING_DOWN):
                    logger.info("Market already %s, looking for next", phase.value)
                    await asyncio.sleep(5)
                    continue

                # Wait for market to open (DISCOVERED → open_ts)
                if phase == MarketPhase.DISCOVERED:
                    wait_secs = market.open_ts - self._scheduler.adjusted_now()
                    if wait_secs > 0:
                        logger.info(
                            "Market not open yet, waiting %.0fs until open_ts",
                            wait_secs,
                        )
                    while (
                        self._scheduler.get_current_phase(market) == MarketPhase.DISCOVERED
                        and not self._shutdown
                    ):
                        remaining = market.open_ts - self._scheduler.adjusted_now()
                        # Poll every 1s in the last 5s for fast entry
                        if remaining <= 5.0:
                            await asyncio.sleep(min(max(remaining, 0.2), 1.0))
                        else:
                            await asyncio.sleep(min(remaining - 4.0, 30))
                    if self._shutdown:
                        break
                    phase = self._scheduler.get_current_phase(market)

                # Wait through PRE_ENTRY delay (trend skips — enter immediately at open)
                if self._config.strategy != "trend":
                    while (
                        self._scheduler.get_current_phase(market) == MarketPhase.PRE_ENTRY
                        and not self._shutdown
                    ):
                        await asyncio.sleep(0.5)

                if self._shutdown:
                    break

                phase = self._scheduler.get_current_phase(market)
                if phase not in (MarketPhase.ACTIVE,):
                    logger.info("Market moved to %s, skipping", phase.value)
                    await asyncio.sleep(5)
                    continue

                # Now market is ACTIVE — orderbook has real prices
                # Skip markets already well into ACTIVE phase.
                # Prices skew away from 50/50 quickly, so entering late = guaranteed imbalance.
                # Trend: skip if > 45s in (15-min markets).
                # Passive/hedge_sell: skip if > 90s in.
                _max_entry_age_s = 45.0 if self._config.strategy == "trend" else 90.0
                if self._config.strategy in ("passive", "hedge_sell", "trend"):
                    time_since_open = self._scheduler.adjusted_now() - market.open_ts
                    if time_since_open > _max_entry_age_s:
                        logger.info(
                            "%s: skipping market %s — already %.0fs into session. "
                            "Waiting for next fresh market.",
                            self._config.strategy.upper(),
                            market.condition_id[:16], time_since_open,
                        )
                        # Cooldown until this market closes so we pick up the next one
                        self._no_edge_cooldown_until[market.condition_id] = market.close_ts
                        await asyncio.sleep(5)
                        continue

                # Pre-entry gate (passive/hedge_sell have their own guards)
                if self._config.strategy in ("passive", "hedge_sell", "trend"):
                    passed, tier = True, EdgeTier.WIDE
                else:
                    passed, tier = await self._scheduler.check_pre_entry_gate(market)
                if not passed:
                    self._no_edge_cooldown_until[market.condition_id] = (
                        self._scheduler.adjusted_now() + self._config.no_edge_cooldown_s
                    )
                    logger.info("Skipping market %s: no edge", market.condition_id)
                    await self._alerts.market_skipped("no edge")
                    await asyncio.sleep(min(10.0, self._config.no_edge_cooldown_s))
                    continue

                # Risk entry check
                verdict = self._risk_engine.check_entry(
                    self._daily_pnl, self._hourly_pnl, self._daily_markets
                )
                if verdict.action != RiskAction.NONE:
                    logger.warning("Entry blocked: %s", verdict.reason)
                    if verdict.action == RiskAction.KILL_DAY:
                        self._shutdown = True
                        break
                    await asyncio.sleep(10)
                    continue

                # Run session
                _session_start_ts = time.time()
                if self._config.strategy == "trend":
                    result = await self._run_trend_session(market, tier)
                    if result is None:
                        self._no_edge_cooldown_until[market.condition_id] = (
                            self._scheduler.adjusted_now() + 10.0
                        )
                        logger.info("Trend entry rejected, cooling down 10s for market %s", market.condition_id[:16])
                        await asyncio.sleep(5)
                        continue
                elif self._config.strategy == "passive":
                    result = await self._run_passive_session(market, tier)
                    if result is None:
                        # Entry guard rejected — skip this market, wait for next one
                        self._no_edge_cooldown_until[market.condition_id] = (
                            self._scheduler.adjusted_now() + 120.0
                        )
                        logger.info("Passive entry rejected, cooling down 120s for market %s", market.condition_id[:16])
                        await asyncio.sleep(10)
                        continue
                elif self._config.strategy == "hedge_sell":
                    result = await self._run_hedge_sell_session(market, tier)
                else:
                    result = await self._run_session(market, tier)

                # Detect abnormally short session — likely a crash
                if time.time() - _session_start_ts < 30.0:
                    logger.warning("Session lasted < 30s — adding 60s cooldown for market %s", market.condition_id[:16])
                    self._no_edge_cooldown_until[market.condition_id] = (
                        self._scheduler.adjusted_now() + 60.0
                    )

                # Update daily/hourly PnL
                if result:
                    self._daily_pnl += result.get("worst_case_pnl", 0)
                    self._hourly_pnl += result.get("worst_case_pnl", 0)
                    self._daily_markets += 1
                    self._no_edge_cooldown_until.pop(market.condition_id, None)
                    if (
                        self._config.max_run_sessions > 0
                        and self._daily_markets >= self._config.max_run_sessions
                    ):
                        logger.info(
                            "Reached max_run_sessions=%d, stopping",
                            self._config.max_run_sessions,
                        )
                        self._shutdown = True
                        break

                # Reset for next
                self._tracker.reset()
                self._risk_engine.reset_session()

        finally:
            # Shutdown WS feed
            if self._ws_feed is not None:
                try:
                    await self._ws_feed.close()
                except Exception:
                    pass
                self._ws_feed = None
            self._state_manager.release_lock()

    async def _run_session(
        self, market: MarketInfo, tier: EdgeTier
    ) -> Optional[dict]:
        """Run a single market session: place ladder, monitor, cleanup."""
        session_id = uuid.uuid4().hex[:12]
        state = BotState(
            phase=MarketPhase.ACTIVE,
            market=market,
            session_id=session_id,
        )
        # Subscribe WS feed to this market's tokens
        if self._ws_feed is not None:
            await self._ws_feed.start([market.up_token, market.down_token])

        # Seed inventory from exchange before placing new orders.
        await self._reconcile_live_position(
            market,
            state,
            reason="session_start",
            force=True,
        )

        # Get orderbooks, use effective prices (bids, not extreme asks)
        up_book = await self._client.get_order_book(market.up_token)
        down_book = await self._client.get_order_book(market.down_token)
        up_price = _get_effective_price(up_book)
        down_price = _get_effective_price(down_book)

        combined = up_price + down_price
        logger.info(
            "Session entry: up_eff=%.3f down_eff=%.3f combined=%.3f",
            up_price, down_price, combined,
        )
        # Build ladders — budget each side separately based on actual prices
        half_capital = self._config.session_capital_limit / 2
        up_cost_per_level = self._config.shares_per_order * up_price
        down_cost_per_level = self._config.shares_per_order * down_price
        up_levels = min(15, max(2, int(half_capital / up_cost_per_level))) if up_cost_per_level > 0 else 2
        down_levels = min(15, max(2, int(half_capital / down_cost_per_level))) if down_cost_per_level > 0 else 2
        logger.info(
            "Building ladder: up=%d levels (%.2f$/lvl) down=%d levels (%.2f$/lvl)",
            up_levels, up_cost_per_level, down_levels, down_cost_per_level,
        )
        up_ladder = build_ladder(
            up_price, market.tick_size, self._config.shares_per_order, up_levels,
            other_side_best_ask=down_price, max_combined=self._config.max_combined_vwap,
        )
        down_ladder = build_ladder(
            down_price, market.tick_size, self._config.shares_per_order, down_levels,
            other_side_best_ask=up_price, max_combined=self._config.max_combined_vwap,
        )

        # Create engines
        fill_queue: asyncio.Queue = asyncio.Queue()
        fill_monitor = FillMonitor(
            self._client,
            fill_queue,
            market.condition_id,
            market.up_token,
            market.down_token,
        )

        order_engine = OrderEngine(self._client, self._config, fill_monitor)

        # Start fill monitor
        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(
            fill_monitor.run(
                interval=max(0.2, self._config.fill_poll_interval_s),
                stop_event=stop_event,
            )
        )

        # Place initial ladder
        await self._alerts.market_entry(market, tier, combined)
        expiration_ts = int(market.close_ts)
        # Fix #42: cancel orphaned orders if placement times out
        try:
            await order_engine.place_ladder(
                up_ladder, down_ladder,
                market.up_token, market.down_token,
                expiration_ts, state,
            )
        except Exception as e:
            logger.error("Initial placement failed — cancelling orphaned orders: %s", e)
            await self._cancel_orphaned_orders(market)
            stop_event.set()
            monitor_task.cancel()
            return None

        # Track anchors for re-anchoring
        up_anchor = up_price
        down_anchor = down_price
        last_rebalance_cancel = 0.0
        last_soft_rebalance_lagging_side = ""
        last_rebalance_mode = ""
        last_side_fill_ts: dict[str, float] = {"up": 0.0, "down": 0.0}
        last_reanchor_action_ts: dict[str, float] = {"up": 0.0, "down": 0.0}

        # Hot-side detection: track fill rate per drain cycle
        hot_side_until: dict[str, float] = {"up": 0.0, "down": 0.0}
        _active_side_fill_times: dict[str, list] = {"up": [], "down": []}  # rolling window
        # CANCEL_ALL escalation: count consecutive no-op cancel cycles
        cancel_all_noop_count = 0
        cancel_all_last_position_hash = ""
        # CANCEL_HEAVY_SIDE escalation: count consecutive no-op cancels
        cancel_heavy_noop_count = 0

        def _update_fill_tracking(fill: FillEvent) -> None:
            """Update level fill accounting and clear fully-filled local open-order state."""
            level = state.active_orders.get(fill.order_id)
            if not level:
                return
            level.filled_shares += fill.filled_shares
            # Keep local open-order tracking aligned with exchange reality to avoid
            # repeated rebalance cancel calls against already-filled orders.
            if level.filled_shares + 1e-9 >= level.target_shares:
                level.last_fill_at = time.time()
                level.order_id = None
                state.active_orders.pop(fill.order_id, None)
                state.order_map.pop(fill.order_id, None)

        async def _cancel_side_orders(
            side: str,
            token_id: str,
            reason: str,
            target_levels: list | None = None,
        ) -> dict[int, tuple[str, object]]:
            """Cancel one side and release local reserved state.

            When smart-cancel is enabled with target_levels, keep local orders that
            still match target prices and cancel only stale ones.
            Returns kept orders keyed by price_cents.
            """
            keep_ids: set[str] = set()
            kept_by_price: dict[int, tuple[str, object]] = {}

            use_smart_cancel = bool(
                self._config.smart_cancel_enabled
                and target_levels
                and reason.startswith("re-anchor:")
            )

            if use_smart_cancel:
                keep_ids = self._plan_smart_keep_ids(state.order_map, side, target_levels)
                try:
                    if keep_ids:
                        # Cancel only non-kept exchange orders for this asset.
                        ok = True
                        for _ in range(5):
                            open_orders = await self._client.get_open_orders(token_id)
                            extra_ids = []
                            for order in open_orders:
                                oid = order.get("id", order.get("order_id", ""))
                                if oid and oid not in keep_ids:
                                    extra_ids.append(oid)
                            if not extra_ids:
                                break
                            for oid in extra_ids:
                                await self._client.cancel_order(oid)
                            await asyncio.sleep(0.3)

                        if extra_ids:
                            ok = False
                        logger.info(
                            "Smart cancel side=%s keep=%d removed=%d reason=%s",
                            side,
                            len(keep_ids),
                            max(0, len(open_orders) - len(keep_ids)),
                            reason,
                        )
                    else:
                        ok = await self._client.cancel_and_verify(token_id)
                except Exception as exc:
                    logger.warning(
                        "Smart cancel failed side=%s reason=%s err=%s -- falling back to full cancel",
                        side,
                        reason,
                        exc,
                    )
                    keep_ids = set()
                    ok = await self._client.cancel_and_verify(token_id)
            else:
                ok = await self._client.cancel_and_verify(token_id)

            if not ok:
                logger.warning("Side cancel verify failed side=%s reason=%s", side, reason)
            # Fix #8: Drain any pending fills BEFORE clearing tracking
            drain_fills(
                fill_queue, self._tracker, state, order_engine,
                _update_fill_tracking,
                last_side_fill_ts=last_side_fill_ts,
                recent_fill_oids=recent_fill_oids,
            )
            side_oids = [
                oid
                for oid, meta in state.order_map.items()
                if meta.get("side") == side
            ]
            cancelled_oids: list[str] = []
            for oid in side_oids:
                if oid in keep_ids:
                    meta = state.order_map.get(oid, {})
                    price_cents = self._price_to_cents(meta.get("price", 0.0))
                    if price_cents >= 0 and price_cents not in kept_by_price:
                        kept_by_price[price_cents] = (oid, state.active_orders.get(oid))
                    continue
                level = state.active_orders.pop(oid, None)
                if level:
                    order_engine.on_cancel_release(level, state)
                cancelled_oids.append(oid)
            if cancelled_oids:
                mark_orders_cancelled(state, cancelled_oids)
            return kept_by_price

        def _bind_kept_orders_to_ladder(
            side: str,
            ladder: list,
            kept_by_price: dict[int, tuple[str, object]],
        ) -> int:
            """Attach retained order IDs to rebuilt ladder levels."""
            if not kept_by_price:
                return 0
            rebound = 0
            for lvl in ladder:
                entry = kept_by_price.get(int(getattr(lvl, "price_cents", -1)))
                if not entry:
                    continue
                oid, prev_level = entry
                lvl.order_id = oid
                if prev_level is not None:
                    lvl.filled_shares = float(getattr(prev_level, "filled_shares", 0.0) or 0.0)
                    lvl.replenish_count = int(getattr(prev_level, "replenish_count", 0) or 0)
                state.active_orders[oid] = lvl
                if oid in state.order_map:
                    state.order_map[oid]["side"] = side
                    state.order_map[oid]["price"] = lvl.price_cents / 100.0
                fill_monitor.register_order(oid)
                rebound += 1
            return rebound

        def _build_reanchor_ladder(
            price: float,
            other_side_best_ask: float = 0.0,
            max_levels_override: int = 0,
        ) -> list:
            """Build replacement ladder for current price using side budget.

            Args:
                max_levels_override: if >0, cap the number of levels (used during rebalance).
            """
            half_cap = self._config.session_capital_limit / 2
            levels_count = (
                min(
                    15,
                    max(
                        2,
                        int(half_cap / (self._config.shares_per_order * price)),
                    ),
                )
                if price > 0
                else 2
            )
            if max_levels_override > 0:
                levels_count = min(levels_count, max_levels_override)
            return build_ladder(
                price,
                market.tick_size,
                self._config.shares_per_order,
                levels_count,
                other_side_best_ask=other_side_best_ask,
                max_combined=self._config.max_combined_vwap,
            )

        def _out_of_range_ratio(side: str, candidate_ladder: list) -> float:
            """How much of active side orders sit outside candidate ladder prices."""
            active_prices = [
                int(round(float(meta.get("price", 0.0)) * 100))
                for _, meta in state.order_map.items()
                if meta.get("side") == side
            ]
            if not active_prices:
                return 1.0
            candidate_prices = {lvl.price_cents for lvl in candidate_ladder}
            if not candidate_prices:
                return 1.0
            outside = sum(1 for price_cents in active_prices if price_cents not in candidate_prices)
            return outside / max(1, len(active_prices))

        def _candidate_has_vwap_headroom(side: str, candidate_ladder: list, pos) -> bool:
            """Delegate to extracted rebalance module."""
            return candidate_has_vwap_headroom(
                side, candidate_ladder, pos, self._config.max_combined_vwap
            )

        def _should_reanchor_side(
            side: str,
            new_price: float,
            candidate_ladder: list,
            *,
            now_ts: float,
            time_left_s: float,
            current_edge: float,
            lagging_side: str,
            imbalance: float,
            total_shares: float,
            combined_vwap: float = 0.0,
        ) -> tuple[bool, str]:
            """Delegate to extracted rebalance module with captured session context."""
            anchor = up_anchor if side == "up" else down_anchor
            side_order_count = sum(1 for m in state.order_map.values() if m.get("side") == side)
            out_ratio = _out_of_range_ratio(side, candidate_ladder)
            if side == "up":
                candidate_edge = estimate_ladder_edge(candidate_ladder, down_ladder)
            else:
                candidate_edge = estimate_ladder_edge(up_ladder, candidate_ladder)

            decision = should_reanchor_side(
                side, new_price, anchor, candidate_ladder,
                now_ts=now_ts,
                time_left_s=time_left_s,
                current_edge=current_edge,
                candidate_edge=candidate_edge,
                out_of_range_ratio=out_ratio,
                lagging_side=lagging_side,
                imbalance=imbalance,
                total_shares=total_shares,
                combined_vwap=combined_vwap,
                side_order_count=side_order_count,
                last_reanchor_ts=last_reanchor_action_ts[side],
                last_fill_ts=last_side_fill_ts.get(side, 0.0),
                config=self._config,
            )
            return (decision.should_reanchor, decision.reason)

        # Monitor loop
        session_entry_ts = time.time()
        last_reanchor_check = time.time()
        last_heartbeat = time.time()
        last_reconcile_ts = time.time()
        last_position_reconcile_ts = time.time()
        last_edge_check = time.time()
        last_taker_rebalance_ts = 0.0
        initial_edge = estimate_ladder_edge(up_ladder, down_ladder)
        defensive_mode = False
        reconciler = OrderReconciler(self._client)
        recent_fill_oids: set = set()
        try:
            while not self._shutdown:
                phase = self._scheduler.get_current_phase(market)

                if phase in (MarketPhase.CLOSED, MarketPhase.RESOLVED):
                    break

                if (
                    phase == MarketPhase.WINDING_DOWN
                    and state.phase != MarketPhase.WINDING_DOWN
                ):
                    state.phase = MarketPhase.WINDING_DOWN
                    logger.info("Entering WINDING_DOWN phase — cancelling all orders")
                    # Fix #4: Cancel all orders immediately to prevent late bad fills
                    await _cancel_side_orders("up", market.up_token, reason="winding_down")
                    await _cancel_side_orders("down", market.down_token, reason="winding_down")
                    # Fix #14+#59: Taker flatten if imbalanced, but only if ask < 80c.
                    pos = self._tracker.position
                    if pos.share_imbalance >= self._config.taker_flatten_imbalance and pos.total_shares > 10:
                        _lag_side_wd = "up" if pos.up_shares < pos.down_shares else "down"
                        _lag_tok_wd = market.up_token if _lag_side_wd == "up" else market.down_token
                        try:
                            _bk_wd = await self._client.get_order_book(_lag_tok_wd)
                            _asks_wd = _bk_wd.get("asks", [])
                            _best_ask_wd = min(float(a["price"]) for a in _asks_wd) if _asks_wd else 1.0
                        except Exception:
                            _best_ask_wd = 1.0
                        if _best_ask_wd <= 0.80:
                            await self._try_taker_flatten(market, state, fill_monitor, skip_price_cap=True)
                        else:
                            logger.info(
                                "WINDING_DOWN: skipping taker flatten — ask %.0fc too expensive",
                                _best_ask_wd * 100,
                            )

                # Drain fills — track per-side fill count for hot-side detection
                drain_fill_count = drain_fills(
                    fill_queue, self._tracker, state, order_engine,
                    _update_fill_tracking,
                    last_side_fill_ts=last_side_fill_ts,
                    recent_fill_oids=recent_fill_oids,
                )

                # Hot-side detection: if >50% of a side's levels filled in one drain,
                # mark that side as "hot" and pause new placements.
                # Fix #29: Skip during initial grace period — initial ladder fills are expected.
                _hs_now = time.time()
                if _hs_now - session_entry_ts >= 15.0:
                    for hs_side in ("up", "down"):
                        hs_count = drain_fill_count.get(hs_side, 0)
                        if hs_count <= 0:
                            continue
                        # Record fill timestamps for rolling-window detection
                        _active_side_fill_times[hs_side].extend([_hs_now] * hs_count)
                        # Burst detection (existing)
                        ladder_ref = up_ladder if hs_side == "up" else down_ladder
                        total_levels = max(1, len(ladder_ref))
                        fill_ratio = hs_count / total_levels
                        if hs_count >= 2 and fill_ratio >= self._config.hot_side_fill_ratio:
                            hot_side_until[hs_side] = _hs_now + self._config.hot_side_cooldown_s
                            logger.warning(
                                "Hot side detected (burst): %s — %d/%d levels filled (%.0f%%), pausing %.0fs",
                                hs_side, drain_fill_count[hs_side], total_levels,
                                fill_ratio * 100, self._config.hot_side_cooldown_s,
                            )
                    # Rolling-window detection: sustained fill rate over trailing window
                    for hs_side in ("up", "down"):
                        window = self._config.hot_side_rolling_window_s
                        cutoff = _hs_now - window
                        _active_side_fill_times[hs_side] = [t for t in _active_side_fill_times[hs_side] if t > cutoff]
                        rolling_count = len(_active_side_fill_times[hs_side])
                        if rolling_count >= self._config.hot_side_rolling_threshold and hot_side_until[hs_side] <= _hs_now:
                            hot_side_until[hs_side] = _hs_now + self._config.hot_side_cooldown_s
                            logger.warning(
                                "Hot side detected (rolling): %s — %d fills in %.0fs window, pausing %.0fs",
                                hs_side, rolling_count, window, self._config.hot_side_cooldown_s,
                            )

                # Keep local inventory aligned with exchange during session.
                # Uses get_balance_allowance (on-chain conditional token balance)
                # with trade-based cost projection as fallback.
                # Fix #56: Skip when fills arrived recently — endpoint lags settlement.
                _last_any_fill_active = max(
                    last_side_fill_ts.get("up", 0.0),
                    last_side_fill_ts.get("down", 0.0),
                )
                _reconcile_settle_active = 30.0
                if (
                    state.phase == MarketPhase.ACTIVE
                    and time.time() - last_position_reconcile_ts >= self._config.position_reconcile_interval_s
                    and time.time() - _last_any_fill_active >= _reconcile_settle_active
                ):
                    last_position_reconcile_ts = time.time()
                    try:
                        old_pos = self._tracker.position
                        changed = await self._reconcile_live_position(
                            market, state, reason="periodic", force=False,
                        )
                        if changed:
                            new_pos = self._tracker.position
                            logger.warning(
                                "Periodic reconcile corrected position: "
                                "UP %.1f→%.1f DOWN %.1f→%.1f "
                                "cost $%.1f→$%.1f",
                                old_pos.up_shares, new_pos.up_shares,
                                old_pos.down_shares, new_pos.down_shares,
                                old_pos.total_cost, new_pos.total_cost,
                            )
                    except Exception as e:
                        logger.debug("Periodic reconcile failed: %s", e)

                # Risk checks are always active in live loop.
                verdict = self._risk_engine.check_position(
                    self._tracker.position, state
                )

                # Fix #22: CANCEL_HEAVY_SIDE escalation — avoid futile API spam
                # when heavy side has no open orders (all filled, exposure locked in).
                if verdict.action == RiskAction.CANCEL_HEAVY_SIDE:
                    heavy_side = self._tracker.position.excess_side
                    heavy_open = sum(
                        1 for m in state.order_map.values()
                        if m.get("side") == heavy_side
                    )
                    if heavy_open == 0:
                        cancel_heavy_noop_count += 1
                        if cancel_heavy_noop_count >= self._config.cancel_all_noop_escalation:
                            logger.error(
                                "CANCEL_HEAVY_SIDE escalation: %d consecutive no-ops "
                                "(no %s orders to cancel, exposure locked in)",
                                cancel_heavy_noop_count, heavy_side,
                            )
                            verdict = RiskVerdict(RiskAction.CANCEL_ALL, verdict.reason)
                            cancel_heavy_noop_count = 0
                        else:
                            # Suppress futile API call; treat as stop-adding
                            verdict = RiskVerdict(RiskAction.STOP_ADDING, verdict.reason)
                    else:
                        cancel_heavy_noop_count = 0

                # Fix #26: CANCEL_ALL with 0 active orders = position locked in.
                # Downgrade to STOP_ADDING BEFORE dispatching to avoid wasteful
                # cancel_and_verify API calls when there's nothing to cancel.
                if verdict.action == RiskAction.CANCEL_ALL:
                    total_open = len(state.order_map)
                    if total_open == 0:
                        logger.info(
                            "CANCEL_ALL downgraded to STOP_ADDING: 0 open orders, "
                            "position locked in (up=%.0f down=%.0f)",
                            self._tracker.position.up_shares,
                            self._tracker.position.down_shares,
                        )
                        verdict = RiskVerdict(RiskAction.STOP_ADDING, verdict.reason)
                    else:
                        # Orders exist — track for escalation
                        pos = self._tracker.position
                        pos_hash = f"{pos.up_shares:.1f}/{pos.down_shares:.1f}"
                        if pos_hash == cancel_all_last_position_hash:
                            cancel_all_noop_count += 1
                            if cancel_all_noop_count >= self._config.cancel_all_noop_escalation:
                                logger.error(
                                    "CANCEL_ALL escalation: %d consecutive no-ops — killing session "
                                    "(pos=%s, orders keep re-appearing)",
                                    cancel_all_noop_count, pos_hash,
                                )
                                verdict = RiskVerdict(
                                    RiskAction.KILL_SESSION,
                                    f"CANCEL_ALL escalated after {cancel_all_noop_count} no-ops",
                                )
                        else:
                            cancel_all_noop_count = 1
                            cancel_all_last_position_hash = pos_hash
                else:
                    cancel_all_noop_count = 0
                    cancel_all_last_position_hash = ""

                await self._handle_risk_verdict(
                    verdict, order_engine, market, state
                )

                if verdict.action in (
                    RiskAction.KILL_SESSION,
                    RiskAction.KILL_DAY,
                ):
                    if verdict.action == RiskAction.KILL_DAY:
                        self._shutdown = True
                    break

                # Replenish if ACTIVE.
                # In defensive mode, allow continued placement when position
                # has VWAP headroom — each additional hedged pair is profit.
                # Imbalanced: lagging-side only. Balanced: both sides ok.
                pos_for_def = self._tracker.position
                defensive_allows_placement = (
                    defensive_mode
                    and pos_for_def.hedged_shares > 0
                    and pos_for_def.combined_vwap < self._config.max_combined_vwap
                )
                if (
                    state.phase == MarketPhase.ACTIVE
                    and verdict.action in (RiskAction.NONE, RiskAction.STOP_ADDING)
                    and (not defensive_mode or defensive_allows_placement)
                ):
                    pos = self._tracker.position
                    soft_imb = max(0.0, self._config.rebalance_soft_imbalance)
                    hard_imb = max(soft_imb, self._config.rebalance_hard_imbalance)
                    imbalance = pos.share_imbalance
                    lagging_side = (
                        "up" if pos.up_shares < pos.down_shares
                        else "down" if pos.down_shares < pos.up_shares
                        else ""
                    )
                    min_total_for_rebalance = max(
                        0.0, self._config.rebalance_min_total_shares - 0.5
                    )
                    heavy_side = "down" if lagging_side == "up" else "up"
                    heavy_side_open_orders = (
                        sum(
                            1
                            for meta in state.order_map.values()
                            if meta.get("side") == heavy_side
                        )
                        if lagging_side
                        else 0
                    )

                    replenish_up = True
                    replenish_down = True
                    mode = "balanced"

                    if (
                        lagging_side
                        and imbalance >= soft_imb
                        and pos.total_shares < min_total_for_rebalance
                    ):
                        # Informational mode: imbalance is high but we are still below
                        # the minimum size gate for rebalance logic.
                        mode = f"below_min_total:{lagging_side}"
                    elif (
                        pos.total_shares >= min_total_for_rebalance
                        and imbalance >= soft_imb
                        and lagging_side
                    ):
                        # Pause heavy side; only replenish lagging side.
                        replenish_up = lagging_side == "up"
                        replenish_down = lagging_side == "down"
                        mode = f"lagging_only:{lagging_side}"
                        now = time.time()
                        cancel_cooldown = max(0.5, self._config.rebalance_cancel_cooldown_s)

                        # Soft imbalance: do one heavy-side purge per lagging-side regime.
                        if (
                            imbalance < hard_imb
                            and last_soft_rebalance_lagging_side != lagging_side
                            and now - last_rebalance_cancel >= cancel_cooldown
                            and heavy_side_open_orders > 0
                        ):
                            heavy_token = (
                                market.up_token if heavy_side == "up"
                                else market.down_token
                            )
                            await _cancel_side_orders(
                                heavy_side,
                                heavy_token,
                                reason=f"soft_imbalance={imbalance:.1%}",
                            )
                            last_rebalance_cancel = now
                            last_soft_rebalance_lagging_side = lagging_side

                        # Hard imbalance: periodically purge heavy-side queue to free budget.
                        if imbalance >= hard_imb:
                            mode = f"hard_rebalance:{lagging_side}"
                            if (
                                heavy_side_open_orders > 0
                                and now - last_rebalance_cancel >= cancel_cooldown
                            ):
                                heavy_token = (
                                    market.up_token if heavy_side == "up"
                                    else market.down_token
                                )
                                await _cancel_side_orders(
                                    heavy_side,
                                    heavy_token,
                                    reason=f"imbalance={imbalance:.1%}",
                                )
                                last_rebalance_cancel = now
                                last_soft_rebalance_lagging_side = lagging_side
                    else:
                        # New lagging regime should allow one soft cancel again.
                        last_soft_rebalance_lagging_side = ""

                    if mode != last_rebalance_mode:
                        logger.info(
                            "Rebalance mode=%s imbalance=%.1f%% up=%.1f down=%.1f",
                            mode,
                            imbalance * 100.0,
                            pos.up_shares,
                            pos.down_shares,
                        )
                        last_rebalance_mode = mode

                    now_hs = time.time()
                    if replenish_up and now_hs >= hot_side_until.get("up", 0.0):
                        for level in up_ladder:
                            await order_engine.replenish(
                                level, "up", market.up_token, expiration_ts, state
                            )
                    if replenish_down and now_hs >= hot_side_until.get("down", 0.0):
                        for level in down_ladder:
                            await order_engine.replenish(
                                level, "down", market.down_token, expiration_ts, state
                            )

                # Break-even taker rebalance: when excess shares exceed what
                # hedged profit can cover, taker-buy the lagging side immediately.
                # Allowed in defensive mode — VWAP + break-even checks prevent bad buys.
                if (
                    self._config.taker_rebalance_enabled
                    and state.phase == MarketPhase.ACTIVE
                    and verdict.action in (RiskAction.NONE, RiskAction.STOP_ADDING)
                    and time.time() - last_taker_rebalance_ts
                    >= self._config.taker_rebalance_cooldown_s
                ):
                    pos = self._tracker.position
                    if (
                        pos.hedged_shares > 0
                        and pos.excess_shares >= self._config.shares_per_order
                        and pos.total_shares >= self._config.taker_rebalance_min_total_shares
                        and pos.combined_vwap < 1.0
                    ):
                        excess_side = pos.excess_side
                        excess_vwap = (
                            pos.down_vwap if excess_side == "down" else pos.up_vwap
                        )
                        hedged_profit = pos.hedged_profit
                        max_excess = (
                            hedged_profit / excess_vwap if excess_vwap > 0 else 0.0
                        )
                        if pos.excess_shares > max_excess:
                            logger.warning(
                                "Break-even exceeded: excess=%.1f max=%.1f "
                                "(hedged_profit=$%.2f, combined=%.3f) — taker rebalancing",
                                pos.excess_shares,
                                max_excess,
                                hedged_profit,
                                pos.combined_vwap,
                            )
                            await self._try_taker_flatten(market, state, fill_monitor)
                            last_taker_rebalance_ts = time.time()

                # Re-anchor: check frequently, act only when it adds value.
                # Allow during STOP_ADDING — lagging-side re-anchor can
                # improve VWAP, not worsen it. VWAP headroom check is the gate.
                if (
                    state.phase == MarketPhase.ACTIVE
                    and verdict.action in (RiskAction.NONE, RiskAction.STOP_ADDING)
                    and time.time() - last_reanchor_check >= self._config.reanchor_check_interval_s
                ):
                    last_reanchor_check = time.time()
                    try:
                        now_ts = time.time()
                        time_left_s = market.close_ts - self._scheduler.adjusted_now()
                        pos = self._tracker.position
                        imbalance = pos.share_imbalance
                        lagging_side = (
                            "up" if pos.up_shares < pos.down_shares
                            else "down" if pos.down_shares < pos.up_shares
                            else ""
                        )
                        current_edge = estimate_ladder_edge(up_ladder, down_ladder)

                        ra_up_book = await self._client.get_order_book(market.up_token)
                        ra_down_book = await self._client.get_order_book(market.down_token)
                        new_up = _get_effective_price(ra_up_book)
                        new_down = _get_effective_price(ra_down_book)

                        # During hard rebalance, cap the ladder to rebalance_max_levels.
                        is_hard_rebalance = (
                            imbalance >= max(0.0, self._config.rebalance_hard_imbalance)
                            and pos.total_shares >= self._config.rebalance_min_total_shares - 0.5
                        )
                        rebal_cap = self._config.rebalance_max_levels if is_hard_rebalance else 0
                        up_candidate = _build_reanchor_ladder(
                            new_up, other_side_best_ask=new_down, max_levels_override=rebal_cap,
                        )
                        down_candidate = _build_reanchor_ladder(
                            new_down, other_side_best_ask=new_up, max_levels_override=rebal_cap,
                        )
                        candidates: list[tuple[str, float, list, str]] = []

                        # Hot-side check: skip re-anchor for sides on cooldown.
                        ra_now = time.time()

                        ok_up, reason_up = _should_reanchor_side(
                            "up",
                            new_up,
                            up_candidate,
                            now_ts=now_ts,
                            time_left_s=time_left_s,
                            current_edge=current_edge,
                            lagging_side=lagging_side,
                            imbalance=imbalance,
                            total_shares=pos.total_shares,
                            combined_vwap=pos.combined_vwap,
                        )
                        # Fix #31: Bypass hot cooldown when force_reanchor (0 orders, has position)
                        up_order_count = sum(1 for m in state.order_map.values() if m.get("side") == "up")
                        up_force = up_order_count == 0 and pos.up_shares > 0
                        if ok_up and ra_now < hot_side_until.get("up", 0.0) and not up_force:
                            ok_up = False
                        elif ok_up and up_force and ra_now < hot_side_until.get("up", 0.0):
                            logger.info("Re-anchor up: bypassing hot cooldown (force_reanchor, 0 orders)")
                            hot_side_until["up"] = 0.0
                        if ok_up:
                            if (
                                pos.total_shares >= self._config.rebalance_min_total_shares - 0.5
                                and not _candidate_has_vwap_headroom("up", up_candidate, pos)
                            ):
                                logger.info(
                                    "Re-anchor up skipped: no VWAP-headroom levels "
                                    "(up_vwap=%.3f down_vwap=%.3f)",
                                    pos.up_vwap,
                                    pos.down_vwap,
                                )
                            else:
                                candidates.append(("up", new_up, up_candidate, reason_up))

                        ok_down, reason_down = _should_reanchor_side(
                            "down",
                            new_down,
                            down_candidate,
                            now_ts=now_ts,
                            time_left_s=time_left_s,
                            current_edge=current_edge,
                            lagging_side=lagging_side,
                            imbalance=imbalance,
                            total_shares=pos.total_shares,
                            combined_vwap=pos.combined_vwap,
                        )
                        # Fix #31: Bypass hot cooldown when force_reanchor (0 orders, has position)
                        down_order_count = sum(1 for m in state.order_map.values() if m.get("side") == "down")
                        down_force = down_order_count == 0 and pos.down_shares > 0
                        if ok_down and ra_now < hot_side_until.get("down", 0.0) and not down_force:
                            ok_down = False
                        elif ok_down and down_force and ra_now < hot_side_until.get("down", 0.0):
                            logger.info("Re-anchor down: bypassing hot cooldown (force_reanchor, 0 orders)")
                            hot_side_until["down"] = 0.0
                        if ok_down:
                            if (
                                pos.total_shares >= self._config.rebalance_min_total_shares - 0.5
                                and not _candidate_has_vwap_headroom("down", down_candidate, pos)
                            ):
                                logger.info(
                                    "Re-anchor down skipped: no VWAP-headroom levels "
                                    "(up_vwap=%.3f down_vwap=%.3f)",
                                    pos.up_vwap,
                                    pos.down_vwap,
                                )
                            else:
                                candidates.append(("down", new_down, down_candidate, reason_down))

                        if candidates:
                            logger.info(
                                "Re-anchor apply sides=%s | up %.3f->%.3f | down %.3f->%.3f",
                                ",".join(side for side, *_ in candidates),
                                up_anchor,
                                new_up,
                                down_anchor,
                                new_down,
                            )
                            for side, new_price, rebuilt, why in candidates:
                                is_up = side == "up"
                                token = market.up_token if is_up else market.down_token
                                kept_by_price = await _cancel_side_orders(
                                    side,
                                    token,
                                    reason=f"re-anchor:{why}",
                                    target_levels=rebuilt if self._config.smart_cancel_enabled else None,
                                )
                                last_reanchor_action_ts[side] = time.time()

                                if is_up:
                                    up_anchor = new_price
                                    up_ladder = rebuilt
                                    kept = _bind_kept_orders_to_ladder("up", up_ladder, kept_by_price)
                                    if kept > 0:
                                        logger.info("Smart cancel retained up orders: %d", kept)
                                    logger.info(
                                        "Re-anchor placing up: %d lvls @ %.3f (%s)",
                                        len(up_ladder), new_price, why,
                                    )
                                    await order_engine.place_side(
                                        up_ladder,
                                        market.up_token,
                                        "up",
                                        expiration_ts,
                                        state,
                                    )
                                else:
                                    down_anchor = new_price
                                    down_ladder = rebuilt
                                    kept = _bind_kept_orders_to_ladder("down", down_ladder, kept_by_price)
                                    if kept > 0:
                                        logger.info("Smart cancel retained down orders: %d", kept)
                                    logger.info(
                                        "Re-anchor placing down: %d lvls @ %.3f (%s)",
                                        len(down_ladder), new_price, why,
                                    )
                                    await order_engine.place_side(
                                        down_ladder,
                                        market.down_token,
                                        "down",
                                        expiration_ts,
                                        state,
                                    )
                    except Exception as e:
                        logger.warning("Re-anchor failed: %s", e)

                # Fill monitor health
                fm_verdict = self._risk_engine.check_fill_monitor(
                    fill_monitor.last_poll_age
                )
                if fm_verdict.action != RiskAction.NONE:
                    logger.warning(
                        "Fill monitor stale: %s", fm_verdict.reason
                    )

                # Fix #11: Order reconciliation — detect exchange-cancelled orders
                if time.time() - last_reconcile_ts >= 30.0 and state.phase == MarketPhase.ACTIVE:
                    last_reconcile_ts = time.time()
                    try:
                        missing = await reconciler.reconcile(
                            market.condition_id, state.order_map, recent_fill_oids,
                        )
                        for oid in missing:
                            lvl = state.active_orders.pop(oid, None)
                            if lvl:
                                order_engine.on_cancel_release(lvl, state)
                            state.order_map.pop(oid, None)
                            fill_monitor.clear_tracking({oid})
                            logger.warning("Reconciler: released exchange-cancelled order %s", oid[:12])
                    except Exception as e:
                        logger.warning("Reconciler failed: %s", e)

                # Fix #13: Live edge recalculation
                if time.time() - last_edge_check >= 30.0 and state.phase == MarketPhase.ACTIVE:
                    last_edge_check = time.time()
                    if not recalculate_live_edge(self._tracker.position, initial_edge):
                        if not defensive_mode:
                            logger.warning("Live edge collapsed — defensive mode")
                            defensive_mode = True
                    else:
                        if defensive_mode:
                            logger.info("Live edge recovered — resuming replenishment")
                            defensive_mode = False

                # Heartbeat
                if time.time() - last_heartbeat >= self._config.heartbeat_interval:
                    pos = self._tracker.position
                    summary = (
                        f"Up={pos.up_shares:.1f} Down={pos.down_shares:.1f} "
                        f"VWAP={pos.combined_vwap:.4f} "
                        f"PnL={pos.risk_worst_case:.2f}"
                    )
                    await self._alerts.heartbeat_status(
                        state.phase.value, summary
                    )
                    last_heartbeat = time.time()

                # Kill command check
                killed = await self._alerts.check_kill_command()
                if killed:
                    logger.warning("Kill command received!")
                    self._shutdown = True
                    break

                # Save state
                state.session_worst_case_pnl = (
                    self._tracker.position.risk_worst_case
                )
                self._state_manager.save_state(state)

                # When idle (0 orders + defensive), sleep longer to avoid API spam.
                if defensive_mode and len(state.order_map) == 0:
                    await asyncio.sleep(5.0)
                else:
                    await asyncio.sleep(max(0.2, self._config.monitor_loop_interval_s))

        finally:
            # Cleanup
            stop_event.set()
            try:
                await asyncio.wait_for(monitor_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                monitor_task.cancel()

            # Cancel all orders
            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)

            # Flush stale fills before reconciliation overwrites tracker
            self._flush_fill_queue(fill_queue)

            # Fix #15: Position reconciliation — verify with exchange
            try:
                changed = await self._reconcile_live_position(
                    market,
                    state,
                    reason="session_end",
                    force=True,
                )
                if changed:
                    pos = self._tracker.position
                    await self._alerts.error(
                        f"Position mismatch corrected at session end: "
                        f"up={pos.up_shares:.1f} down={pos.down_shares:.1f}"
                    )
            except Exception as e:
                logger.warning("Position reconciliation failed: %s", e)

            # Finalize
            pos = self._tracker.position
            result = {
                "session_id": session_id,
                "condition_id": market.condition_id,
                "total_trades": state.total_trades,
                "up_shares": pos.up_shares,
                "down_shares": pos.down_shares,
                "combined_vwap": pos.combined_vwap,
                "worst_case_pnl": pos.risk_worst_case,
                "projected_pnl": self._tracker.report_worst_case,
                "rebate_estimate": self._tracker.rebate_estimate,
            }
            self._state_manager.save_session_result(result)
            await self._alerts.session_complete(
                fills=state.total_trades,
                combined_vwap=pos.combined_vwap,
                risk_pnl=pos.risk_worst_case,
                report_pnl=self._tracker.report_worst_case,
                up_shares=pos.up_shares,
                down_shares=pos.down_shares,
            )

            return result

    async def _run_passive_session(
        self, market: MarketInfo, tier: EdgeTier
    ) -> Optional[dict]:
        """Run a passive penny-ladder session: wide range, replenish-only, no rebalance."""
        session_id = uuid.uuid4().hex[:12]
        state = BotState(
            phase=MarketPhase.ACTIVE,
            market=market,
            session_id=session_id,
        )

        # Subscribe WS feed to this market's tokens
        if self._ws_feed is not None:
            await self._ws_feed.start([market.up_token, market.down_token])

        await self._reconcile_live_position(
            market, state, reason="passive_session_start", force=True,
        )

        # Get orderbook prices — use best BID for passive ladder anchor.
        # Anchoring to the ask places levels above the bid that fill instantly
        # at expensive prices (taker behavior). Bid-based anchor ensures all
        # levels sit in the book as true maker orders.
        up_book = await self._client.get_order_book(market.up_token)
        down_book = await self._client.get_order_book(market.down_token)
        up_price = _get_best_bid_price(up_book)
        down_price = _get_best_bid_price(down_book)

        logger.info(
            "Passive session entry: up=%.3f down=%.3f combined=%.3f",
            up_price, down_price, up_price + down_price,
        )

        # Skew guard: reject markets where one side is above 55c (other below 45c).
        # In skewed markets, the cheap side fills easily but the expensive side's
        # orders sit 30-40c below its ask and never fill → one-sided accumulation.
        _max_entry = self._config.passive_max_entry_price
        if up_price > _max_entry or down_price > _max_entry:
            logger.warning(
                "Passive skip: market too skewed (up=%.3f down=%.3f). "
                "Need both sides <= %.0fc for balanced fills.",
                up_price, down_price, _max_entry * 100,
            )
            return None

        # Passive entry guard: reject extreme skew where one side has <3 levels.
        # At 15 shares, notional floor is 7c. If UP is at 10c → only 1 level (8c).
        # This creates instant one-sided exposure. Need at least 3 levels per side.
        min_price = max(up_price, down_price)
        max_price = min(up_price, down_price)
        step = self._config.passive_step_cents
        floor_eff_cents = max(
            self._config.passive_floor_cents,
            int(100.0 / self._config.shares_per_order) + 1,  # notional floor
        )
        # If max_depth is set, the effective floor is raised
        if self._config.passive_max_depth_cents > 0:
            cheap_price_cents = int(round(max_price * 100))
            depth_floor = cheap_price_cents - self._config.passive_max_depth_cents
            floor_eff_cents = max(floor_eff_cents, depth_floor)
        cheap_levels = max(0, (int(round(max_price * 100)) - 1 - floor_eff_cents) // step + 1)
        if cheap_levels < 3:
            logger.warning(
                "Passive skip: cheap side (%.3f) has only %d levels (need >=3). "
                "Wait for market open with ~50/50 prices.",
                max_price, cheap_levels,
            )
            return None

        floor = self._config.passive_floor_cents

        # Get best asks from WS feed — cap ladder below ask to prevent taker fills.
        # Fail-closed: if no WS ask data, skip this market entirely.
        # WS is real-time; REST book is stale (~5s updates).
        up_best_ask = None
        down_best_ask = None
        if self._ws_feed is not None:
            # Wait briefly for WS book snapshot after subscription
            for _ in range(20):  # up to 2s
                up_best_ask = self._ws_feed.get_best_ask(market.up_token)
                down_best_ask = self._ws_feed.get_best_ask(market.down_token)
                if up_best_ask is not None and down_best_ask is not None:
                    break
                await asyncio.sleep(0.1)
        if up_best_ask is None or down_best_ask is None:
            logger.warning(
                "Passive skip: no WS ask data (up=%s down=%s)",
                up_best_ask, down_best_ask,
            )
            return None
        up_ask_cap_cents = int(round(up_best_ask * 100)) - 1  # 1c below ask
        down_ask_cap_cents = int(round(down_best_ask * 100)) - 1

        max_depth = self._config.passive_max_depth_cents
        price_cap = self._config.passive_max_buy_price_cents
        # Effective cap: min of configured price cap and ask-based cap
        up_eff_cap = min(price_cap, up_ask_cap_cents) if price_cap > 0 else up_ask_cap_cents
        down_eff_cap = min(price_cap, down_ask_cap_cents) if price_cap > 0 else down_ask_cap_cents
        _opl = self._config.passive_orders_per_level
        up_ladder = build_full_range_ladder(
            up_price, market.tick_size, self._config.shares_per_order,
            step_cents=step, floor_cents=floor, max_depth_cents=max_depth,
            max_buy_price_cents=up_eff_cap, orders_per_level=_opl,
        )
        down_ladder = build_full_range_ladder(
            down_price, market.tick_size, self._config.shares_per_order,
            step_cents=step, floor_cents=floor, max_depth_cents=max_depth,
            max_buy_price_cents=down_eff_cap, orders_per_level=_opl,
        )

        logger.info(
            "Passive ladder: up=%d levels (%dc-%dc) down=%d levels (%dc-%dc)",
            len(up_ladder),
            up_ladder[-1].price_cents if up_ladder else 0,
            up_ladder[0].price_cents if up_ladder else 0,
            len(down_ladder),
            down_ladder[-1].price_cents if down_ladder else 0,
            down_ladder[0].price_cents if down_ladder else 0,
        )

        # Reject if level imbalance is too extreme (e.g., 27 UP vs 4 DOWN).
        # All UP orders will fill instantly causing huge one-sided exposure.
        if up_ladder and down_ladder:
            level_ratio = max(len(up_ladder), len(down_ladder)) / max(1, min(len(up_ladder), len(down_ladder)))
            if level_ratio > 3.0:
                logger.warning(
                    "Passive skip: level imbalance too extreme (%.1fx). "
                    "UP=%d levels vs DOWN=%d levels. Wait for ~50/50 prices.",
                    level_ratio, len(up_ladder), len(down_ladder),
                )
                return None

        # Create engines — passive uses no per-side cap (asymmetric ladders are normal)
        # and a much shorter order delay (60+ orders at 300ms = 18s of dead time).
        from dataclasses import replace as _dc_replace
        passive_engine_config = _dc_replace(
            self._config, per_side_cost_cap_pct=1.0, order_delay_ms=50,
        )

        fill_queue: asyncio.Queue = asyncio.Queue()
        fill_monitor = FillMonitor(
            self._client, fill_queue, market.condition_id,
            market.up_token, market.down_token,
        )
        order_engine = OrderEngine(self._client, passive_engine_config, fill_monitor)

        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(
            fill_monitor.run(
                interval=max(0.2, self._config.fill_poll_interval_s),
                stop_event=stop_event,
            )
        )

        await self._alerts.market_entry(market, tier, up_price + down_price)
        expiration_ts = int(market.close_ts)

        # Track ALL order IDs ever placed (never removed) for end-of-session reconciliation
        all_placed_orders: dict[str, dict] = {}  # order_id -> {"side", "price"}

        def _snapshot_new_orders():
            """Copy any new order_map entries into all_placed_orders."""
            for oid, meta in state.order_map.items():
                if oid not in all_placed_orders:
                    all_placed_orders[oid] = dict(meta)

        # Place initial ladder via batch API (fast — 15 orders per call)
        # Fix #42: cancel orphaned orders if placement times out
        try:
            await order_engine.place_ladder_batch(
                up_ladder, down_ladder,
                market.up_token, market.down_token,
                expiration_ts, state,
            )
        except Exception as e:
            logger.error("Initial placement failed — cancelling orphaned orders: %s", e)
            await self._cancel_orphaned_orders(market)
            stop_event.set()
            monitor_task.cancel()
            return None
        _snapshot_new_orders()

        # Track highest placed price for expansion
        up_max_cents = up_ladder[0].price_cents if up_ladder else 0
        down_max_cents = down_ladder[0].price_cents if down_ladder else 0
        # Initial ladder tops — replenishment is only allowed up to these prices.
        # Expansion levels above these are one-shot: fill once, never replenish.
        # This prevents the 67c-72c replenishment cascade that destroys VWAP.
        up_initial_max_cents = up_max_cents
        down_initial_max_cents = down_max_cents
        # Track current market price — don't replenish levels at/above this
        # (those fill instantly = taker behavior, burns budget for nothing)
        up_ref_cents = int(round(up_price * 100))
        down_ref_cents = int(round(down_price * 100))
        last_expand_check = time.time()
        last_heartbeat = time.time()
        _cost_locked = False
        _braked_side: Optional[str] = None  # "up" or "down" when brake is active
        _session_entry_ts = time.time()

        def _update_fill_tracking(fill):
            level = state.active_orders.get(fill.order_id)
            if not level:
                return
            level.filled_shares += fill.filled_shares
            if level.filled_shares + 1e-9 >= level.target_shares:
                level.last_fill_at = time.time()
                level.order_id = None
                state.active_orders.pop(fill.order_id, None)
                state.order_map.pop(fill.order_id, None)

        try:
            while not self._shutdown:
                phase = self._scheduler.get_current_phase(market)

                if phase in (MarketPhase.CLOSED, MarketPhase.RESOLVED):
                    break

                # WINDING_DOWN: cancel all, no taker flatten in passive
                if (
                    phase == MarketPhase.WINDING_DOWN
                    and state.phase != MarketPhase.WINDING_DOWN
                ):
                    state.phase = MarketPhase.WINDING_DOWN
                    logger.info("Passive WINDING_DOWN — cancelling all orders")
                    await self._client.cancel_and_verify(market.up_token)
                    await self._client.cancel_and_verify(market.down_token)
                    # Release local state for cancelled orders
                    for oid, level in list(state.active_orders.items()):
                        order_engine.on_cancel_release(level, state)
                    state.active_orders.clear()
                    state.order_map.clear()

                # Snapshot order IDs before fill drain removes them
                _snapshot_new_orders()

                # Drain fills
                drain_fills(
                    fill_queue, self._tracker, state, order_engine,
                    _update_fill_tracking,
                )

                # Risk checks — passive has NO kill/cancel/stop.
                # Imbalance is temporary: lagging side catches up as fills accumulate.
                # The strategy relies on full-range replenishment to drag VWAP down.
                # Only safety: fill monitor health + telegram /kill command.
                if state.phase == MarketPhase.ACTIVE and state.total_trades > 0:
                    # Fill monitor health check
                    if fill_monitor.last_poll_age > 30.0:
                        logger.warning("Passive: fill monitor stale (%.0fs)", fill_monitor.last_poll_age)

                # Kill command check (before replenish to avoid starvation)
                killed = await self._alerts.check_kill_command()
                if killed:
                    logger.warning("Passive: kill command received")
                    self._shutdown = True
                    break

                # One-sided filling is normal — price moves and the other side catches up.
                # No abort. The -$50 catastrophic stop is the only safety net.

                # Replenish filled levels.
                # Skip levels AT or ABOVE current market price (would be instant taker fills).
                # The 3s instant-fill guard in replenish_passive() handles near-market safety.
                #
                # Worst-case lock: freeze position when guaranteed profit reaches threshold.
                # worst_case = min(pnl_if_up, pnl_if_down) — the real number you get.
                if state.phase == MarketPhase.ACTIVE:
                    replenish_cap = self._config.passive_replenish_cap
                    pos = state.position
                    _avg_cost = pos.avg_cost_per_share
                    _worst = pos.risk_worst_case
                    _price_cap = self._config.passive_max_buy_price_cents

                    # Profit lock: freeze when worst_case >= threshold
                    if pos.total_shares >= self._config.passive_profit_lock_min_shares:
                        _lock_threshold = self._config.passive_profit_lock_threshold
                        _unlock_at = _lock_threshold + self._config.passive_profit_lock_unlock_buffer
                        if not _cost_locked and _worst >= _lock_threshold:
                            _cost_locked = True
                            logger.info(
                                "Passive PROFIT LOCK: worst=$%.2f >= $%.2f, "
                                "freezing position. "
                                "Up=%.1f(if_up=$%.1f) Down=%.1f(if_down=$%.1f) avg_cost=%.4f",
                                _worst, _lock_threshold,
                                pos.up_shares, pos.risk_pnl_if_up,
                                pos.down_shares, pos.risk_pnl_if_down,
                                _avg_cost,
                            )
                            try:
                                await self._client.cancel_and_verify(market.up_token)
                                await self._client.cancel_and_verify(market.down_token)
                                for oid, level in list(state.active_orders.items()):
                                    order_engine.on_cancel_release(level, state)
                            except Exception as e:
                                logger.warning("Passive PROFIT LOCK: cancel failed: %s", e)
                        elif _cost_locked and _worst < _unlock_at:
                            _cost_locked = False
                            logger.info(
                                "Passive PROFIT UNLOCK: worst=$%.2f < $%.2f — resuming",
                                _worst, _unlock_at,
                            )

                    # No catastrophic stop in passive — trader data shows positions
                    # always rebalance over the 15-min session. Use /kill for manual abort.

                    # Replenish filled levels — with imbalance brake.
                    # When one side exceeds passive_imbalance_cap of total shares,
                    # stop replenishing that side to prevent runaway accumulation.
                    # When braked: also cancel expensive open orders on the heavy side
                    # (above that side's VWAP). Keep cheap deep orders — those are the
                    # "catch panic" orders that only fill on real price drops.
                    if not _cost_locked:
                        _imbal_cap = self._config.passive_imbalance_cap
                        _total_sh = pos.total_shares
                        _up_sh = pos.up_shares
                        _down_sh = pos.down_shares
                        _up_ratio = _up_sh / _total_sh if _total_sh > 0 else 0.0
                        _down_ratio = _down_sh / _total_sh if _total_sh > 0 else 0.0
                        _block_up = _up_ratio > _imbal_cap and _total_sh >= 20
                        _block_down = _down_ratio > _imbal_cap and _total_sh >= 20

                        # Cancel ALL orders on newly-braked side.
                        # In skewed markets, even "cheap" orders fill one-sidedly
                        # (e.g., 240 UP / 0 DOWN). Must stop ALL accumulation.
                        if _block_up and _braked_side != "up":
                            _braked_side = "up"
                            _cancelled = 0
                            for level in up_ladder:
                                if level.order_id:
                                    try:
                                        _oid = level.order_id
                                        await self._client.cancel_order(_oid)
                                        order_engine.on_cancel_release(level, state)
                                        state.active_orders.pop(_oid, None)
                                        mark_orders_cancelled(state, [_oid])
                                        _cancelled += 1
                                    except Exception:
                                        pass
                            if _cancelled:
                                logger.info(
                                    "Passive BRAKE UP: cancelled ALL %d orders "
                                    "(up_ratio=%.0f%% > %.0f%%, up=%.0f down=%.0f)",
                                    _cancelled,
                                    _up_ratio * 100, _imbal_cap * 100,
                                    _up_sh, _down_sh,
                                )
                        elif _block_down and _braked_side != "down":
                            _braked_side = "down"
                            _cancelled = 0
                            for level in down_ladder:
                                if level.order_id:
                                    try:
                                        _oid = level.order_id
                                        await self._client.cancel_order(_oid)
                                        order_engine.on_cancel_release(level, state)
                                        state.active_orders.pop(_oid, None)
                                        mark_orders_cancelled(state, [_oid])
                                        _cancelled += 1
                                    except Exception:
                                        pass
                            if _cancelled:
                                logger.info(
                                    "Passive BRAKE DOWN: cancelled ALL %d orders "
                                    "(down_ratio=%.0f%% > %.0f%%, up=%.0f down=%.0f)",
                                    _cancelled,
                                    _down_ratio * 100, _imbal_cap * 100,
                                    _up_sh, _down_sh,
                                )
                        elif not _block_up and not _block_down and _braked_side is not None:
                            logger.info(
                                "Passive BRAKE released (was %s, imbal now %.0f%%)",
                                _braked_side, pos.share_imbalance * 100,
                            )
                            _braked_side = None

                        # Get WS best asks to prevent replenishing at/above ask (taker behavior)
                        # Fail-closed: if no ask data, block replenishment (0 = nothing passes)
                        _up_ask_cents = 0
                        _down_ask_cents = 0
                        if self._ws_feed is not None:
                            _up_ba = self._ws_feed.get_best_ask(market.up_token)
                            if _up_ba is not None:
                                _up_ask_cents = int(round(_up_ba * 100))
                            _down_ba = self._ws_feed.get_best_ask(market.down_token)
                            if _down_ba is not None:
                                _down_ask_cents = int(round(_down_ba * 100))

                        if not _block_up:
                            _up_eligible = [
                                level for level in up_ladder
                                if level.price_cents < up_ref_cents
                                and (_price_cap <= 0 or level.price_cents <= _price_cap)
                                and level.price_cents < _up_ask_cents
                            ]
                            await order_engine.replenish_batch(
                                _up_eligible, "up", market.up_token, expiration_ts, state,
                                max_replenishments=replenish_cap,
                                check_avg_cost=False,
                            )
                        if not _block_down:
                            _down_eligible = [
                                level for level in down_ladder
                                if level.price_cents < down_ref_cents
                                and (_price_cap <= 0 or level.price_cents <= _price_cap)
                                and level.price_cents < _down_ask_cents
                            ]
                            await order_engine.replenish_batch(
                                _down_eligible, "down", market.down_token, expiration_ts, state,
                                max_replenishments=replenish_cap,
                                check_avg_cost=False,
                            )

                # Expand ladder when price moves up — with imbalance brake.
                # Guards: budget, price cap, avg cost, imbalance cap, ask price.
                # Block ALL expansion while ANY side is braked — if position is
                # imbalanced, expanding the lagging side at high prices (60c+)
                # creates expensive fills that worsen avg cost.
                if (
                    self._config.passive_expansion_enabled
                    and state.phase == MarketPhase.ACTIVE
                    and not _cost_locked
                    and _braked_side is None
                    and time.time() - last_expand_check >= self._config.passive_expand_interval_s
                ):
                    last_expand_check = time.time()
                    _expand_price_cap = self._config.passive_max_buy_price_cents
                    _expand_max = self._config.passive_expand_max_cents
                    # Use the tighter of price_cap and expand_max
                    _eff_expand_cap = min(_expand_price_cap, _expand_max) if _expand_price_cap > 0 else _expand_max
                    try:
                        exp_up_book = await self._client.get_order_book(market.up_token)
                        exp_down_book = await self._client.get_order_book(market.down_token)
                        new_up_price = _get_best_bid_price(exp_up_book)
                        new_down_price = _get_best_bid_price(exp_down_book)
                        new_up_cents = int(round(new_up_price * 100))
                        new_down_cents = int(round(new_down_price * 100))

                        # Update ref prices for replenishment gating
                        up_ref_cents = new_up_cents
                        down_ref_cents = new_down_cents

                        # Re-check imbalance for expansion gating
                        _exp_pos = state.position
                        _exp_total = _exp_pos.total_shares
                        _exp_up_ratio = _exp_pos.up_shares / _exp_total if _exp_total > 0 else 0.0
                        _exp_down_ratio = _exp_pos.down_shares / _exp_total if _exp_total > 0 else 0.0
                        _exp_block_up = _exp_up_ratio > self._config.passive_imbalance_cap and _exp_total >= 20
                        _exp_block_down = _exp_down_ratio > self._config.passive_imbalance_cap and _exp_total >= 20

                        # Get WS best asks to cap expansion below ask (prevent taker fills)
                        # Fail-closed: if no ask data, block expansion (0 = nothing passes)
                        _exp_up_ask_cents = 0
                        _exp_down_ask_cents = 0
                        if self._ws_feed is not None:
                            _eua = self._ws_feed.get_best_ask(market.up_token)
                            if _eua is not None:
                                _exp_up_ask_cents = int(round(_eua * 100))
                            _eda = self._ws_feed.get_best_ask(market.down_token)
                            if _eda is not None:
                                _exp_down_ask_cents = int(round(_eda * 100))

                        # Expand UP ladder — batch all new levels at once
                        if new_up_cents > up_max_cents + step and not _exp_block_up:
                            expand_cents = up_max_cents + step
                            if expand_cents % step != 0:
                                expand_cents = expand_cents - (expand_cents % step) + step
                            new_levels = []
                            _up_target = min(new_up_cents, _eff_expand_cap + 1, _exp_up_ask_cents)
                            while expand_cents < _up_target:
                                price = expand_cents / 100.0
                                price = clamp_price(price, market.tick_size)
                                price = round_to_tick(price, market.tick_size)
                                pc = int(round(price * 100))
                                if self._config.shares_per_order * price >= 1.0:
                                    _existing = sum(1 for l in up_ladder if l.price_cents == pc) + sum(1 for l in new_levels if l.price_cents == pc)
                                    for _ in range(max(1, self._config.passive_orders_per_level) - _existing):
                                        lvl = LadderLevel(price_cents=pc, target_shares=self._config.shares_per_order)
                                        new_levels.append(lvl)
                                expand_cents += step
                            if new_levels:
                                placed_count = 0
                                for lvl in new_levels:
                                    if not order_engine._can_afford(lvl, state, "up"):
                                        break
                                    await order_engine._place_level(
                                        lvl, market.up_token, "up", expiration_ts, state,
                                        skip_vwap_check=True,
                                    )
                                    if lvl.order_id is None:
                                        break
                                    up_ladder.append(lvl)
                                    up_max_cents = max(up_max_cents, lvl.price_cents)
                                    placed_count += 1
                                if placed_count > 0:
                                    logger.info(
                                        "Passive expand UP: +%d levels, max now %dc",
                                        placed_count, up_max_cents,
                                    )

                        # Expand DOWN ladder — batch all new levels at once
                        if new_down_cents > down_max_cents + step and not _exp_block_down:
                            expand_cents = down_max_cents + step
                            if expand_cents % step != 0:
                                expand_cents = expand_cents - (expand_cents % step) + step
                            new_levels = []
                            _down_target = min(new_down_cents, _eff_expand_cap + 1, _exp_down_ask_cents)
                            while expand_cents < _down_target:
                                price = expand_cents / 100.0
                                price = clamp_price(price, market.tick_size)
                                price = round_to_tick(price, market.tick_size)
                                pc = int(round(price * 100))
                                if self._config.shares_per_order * price >= 1.0:
                                    _existing = sum(1 for l in down_ladder if l.price_cents == pc) + sum(1 for l in new_levels if l.price_cents == pc)
                                    for _ in range(max(1, self._config.passive_orders_per_level) - _existing):
                                        lvl = LadderLevel(price_cents=pc, target_shares=self._config.shares_per_order)
                                        new_levels.append(lvl)
                                expand_cents += step
                            if new_levels:
                                placed_count = 0
                                for lvl in new_levels:
                                    if not order_engine._can_afford(lvl, state, "down"):
                                        break
                                    await order_engine._place_level(
                                        lvl, market.down_token, "down", expiration_ts, state,
                                        skip_vwap_check=True,
                                    )
                                    if lvl.order_id is None:
                                        break
                                    down_ladder.append(lvl)
                                    down_max_cents = max(down_max_cents, lvl.price_cents)
                                    placed_count += 1
                                if placed_count > 0:
                                    logger.info(
                                        "Passive expand DOWN: +%d levels, max now %dc",
                                        placed_count, down_max_cents,
                                    )
                    except Exception as e:
                        logger.warning("Passive expand failed: %s", e)

                # Snapshot expansion orders before they can fill and be removed
                _snapshot_new_orders()

                # Heartbeat with new metrics (avg_cost, expected_profit)
                if time.time() - last_heartbeat >= self._config.heartbeat_interval:
                    pos = self._tracker.position
                    _imbal_pct = pos.share_imbalance * 100
                    _hv_side = pos.excess_side if _imbal_pct > 10 else ""
                    _hv_ratio = (pos.up_shares if _hv_side == "up" else pos.down_shares) / pos.total_shares if pos.total_shares > 0 else 0.0
                    _braked = _hv_ratio > self._config.passive_imbalance_cap and pos.total_shares >= 20
                    _heavy_tag = f" HEAVY={_hv_side}" if _hv_side else ""
                    _brake_tag = " BRAKE" if _braked else ""
                    _lock_tag = " LOCKED" if _cost_locked else ""
                    summary = (
                        f"[PASSIVE] Up={pos.up_shares:.1f}({pos.up_vwap:.2f}) "
                        f"Down={pos.down_shares:.1f}({pos.down_vwap:.2f}) "
                        f"if_up=${pos.risk_pnl_if_up:.1f} "
                        f"if_down=${pos.risk_pnl_if_down:.1f} "
                        f"imbal={_imbal_pct:.0f}%{_heavy_tag}{_brake_tag}{_lock_tag} "
                        f"cost=${pos.total_cost:.1f}/{self._config.session_capital_limit:.0f}"
                    )
                    logger.info(summary)
                    await self._alerts.heartbeat_status(state.phase.value, summary)
                    last_heartbeat = time.time()

                # Save state
                state.session_worst_case_pnl = self._tracker.position.risk_worst_case
                self._state_manager.save_state(state)

                await asyncio.sleep(max(0.2, self._config.monitor_loop_interval_s))

        finally:
            stop_event.set()
            try:
                await asyncio.wait_for(monitor_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                monitor_task.cancel()

            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)

            # Final snapshot: capture any orders placed during last iteration
            _snapshot_new_orders()

            # Flush stale fills before reconciliation overwrites tracker
            self._flush_fill_queue(fill_queue)

            # Order-status-based reconciliation: query each placed order's final size_matched
            await self._reconcile_via_orders(all_placed_orders, state)

            pos = self._tracker.position
            result = {
                "session_id": session_id,
                "condition_id": market.condition_id,
                "total_trades": state.total_trades,
                "up_shares": pos.up_shares,
                "down_shares": pos.down_shares,
                "combined_vwap": pos.combined_vwap,
                "avg_cost_per_share": pos.avg_cost_per_share,
                "expected_profit": pos.expected_profit,
                "worst_case_pnl": pos.risk_worst_case,
                "projected_pnl": self._tracker.report_worst_case,
                "rebate_estimate": self._tracker.rebate_estimate,
            }
            self._state_manager.save_session_result(result)
            await self._alerts.session_complete(
                fills=state.total_trades,
                combined_vwap=pos.combined_vwap,
                risk_pnl=pos.risk_worst_case,
                report_pnl=self._tracker.report_worst_case,
                up_shares=pos.up_shares,
                down_shares=pos.down_shares,
            )
            return result

    # ------------------------------------------------------------------
    # Trend strategy — windowed ladder following market
    # ------------------------------------------------------------------

    async def _run_trend_session(
        self, market: MarketInfo, tier: EdgeTier
    ) -> Optional[dict]:
        """Run a trend session: windowed ladder near best_bid, re-anchors on drift."""
        from dataclasses import replace as _dc_replace

        cfg = self._config
        session_id = uuid.uuid4().hex[:12]
        state = BotState(
            phase=MarketPhase.ACTIVE,
            market=market,
            session_id=session_id,
        )

        # Subscribe WS feed
        if self._ws_feed is not None:
            await self._ws_feed.start([market.up_token, market.down_token])

        _usdc_start = await self._snapshot_usdc_balance()
        # Fix #71: Track USDC-based cost drift for accurate P&L decisions.
        # Updated every heartbeat; applied as correction to tracker-based worst.
        # Also stored on self for use in _try_taker_flatten.
        _usdc_cost_drift = 0.0  # positive = tracker undercounts cost, negative = overcounts
        self._usdc_cost_drift = 0.0
        _profit_sticky = False  # Fix #72A: hysteresis for PROTECT mode
        _protect_lag_side = ""  # Fix #79C: locked lagging side at PROTECT activation
        _protect_replenish_ts = 0.0  # Fix #79E: PROTECT replenishment cooldown

        await self._reconcile_live_position(
            market, state, reason="trend_session_start", force=True,
        )

        # Get orderbook prices — anchor to best BID (maker orders)
        up_book = await self._client.get_order_book(market.up_token)
        down_book = await self._client.get_order_book(market.down_token)
        up_bid = _get_best_bid_price(up_book)
        down_bid = _get_best_bid_price(down_book)

        logger.info(
            "Trend session entry: up_bid=%.3f down_bid=%.3f combined=%.3f",
            up_bid, down_bid, up_bid + down_bid,
        )

        # Skew guard: reject markets where one side is above 55c (other below 45c).
        # In skewed markets, only the cheap side fills → one-sided accumulation.
        if up_bid <= 0.05 or down_bid <= 0.05 or up_bid > 0.57 or down_bid > 0.57:
            logger.warning(
                "Trend skip: market too skewed (up=%.3f down=%.3f). "
                "Need both sides between 5c and 57c.",
                up_bid, down_bid,
            )
            return None

        # Get best asks from WS feed — cap ladder below ask to prevent taker fills.
        # Fail-closed: if no WS ask data, skip this market entirely.
        # WS is real-time; REST book is stale (~5s updates).
        _up_best_ask = None
        _down_best_ask = None
        if self._ws_feed is not None:
            # Wait briefly for WS book snapshot after subscription
            for _ in range(20):  # up to 2s
                _up_best_ask = self._ws_feed.get_best_ask(market.up_token)
                _down_best_ask = self._ws_feed.get_best_ask(market.down_token)
                if _up_best_ask is not None and _down_best_ask is not None:
                    break
                await asyncio.sleep(0.1)
        if _up_best_ask is None or _down_best_ask is None:
            logger.warning(
                "Trend skip: no WS ask data (up=%s down=%s)",
                _up_best_ask, _down_best_ask,
            )
            return None
        _up_ask_cap = int(round(_up_best_ask * 100)) - 1  # 1c below ask
        _down_ask_cap = int(round(_down_best_ask * 100)) - 1
        _up_price_cap = min(cfg.trend_max_buy_price_cents, _up_ask_cap) if cfg.trend_max_buy_price_cents > 0 else _up_ask_cap
        _down_price_cap = min(cfg.trend_max_buy_price_cents, _down_ask_cap) if cfg.trend_max_buy_price_cents > 0 else _down_ask_cap

        # Build initial windowed ladders
        up_ladder = build_trend_ladder(
            up_bid, market.tick_size, cfg.shares_per_order,
            num_levels=cfg.trend_levels_per_side,
            step_cents=cfg.trend_step_cents,
            max_buy_price_cents=_up_price_cap,
        )
        down_ladder = build_trend_ladder(
            down_bid, market.tick_size, cfg.shares_per_order,
            num_levels=cfg.trend_levels_per_side,
            step_cents=cfg.trend_step_cents,
            max_buy_price_cents=_down_price_cap,
        )

        logger.info(
            "Trend ladder: up=%d levels (%dc-%dc) down=%d levels (%dc-%dc)",
            len(up_ladder),
            up_ladder[-1].price_cents if up_ladder else 0,
            up_ladder[0].price_cents if up_ladder else 0,
            len(down_ladder),
            down_ladder[-1].price_cents if down_ladder else 0,
            down_ladder[0].price_cents if down_ladder else 0,
        )

        if not up_ladder or not down_ladder:
            logger.warning("Trend skip: empty ladder (up=%d down=%d)", len(up_ladder), len(down_ladder))
            return None

        # Create engine with trend-specific config overrides
        trend_engine_config = _dc_replace(
            cfg,
            per_side_cost_cap_pct=1.0,                           # no per-side cap
            order_delay_ms=cfg.trend_order_delay_ms,
            max_combined_vwap=2.0,                               # disable VWAP in _place_level
            passive_avg_cost_target=cfg.trend_avg_cost_target,   # route _is_cheap_enough
        )

        fill_queue: asyncio.Queue = asyncio.Queue()
        fill_monitor = FillMonitor(
            self._client, fill_queue, market.condition_id,
            market.up_token, market.down_token,
        )
        order_engine = OrderEngine(self._client, trend_engine_config, fill_monitor)

        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(
            fill_monitor.run(
                interval=max(0.2, cfg.fill_poll_interval_s),
                stop_event=stop_event,
            )
        )

        await self._alerts.market_entry(market, tier, up_bid + down_bid)
        expiration_ts = int(market.close_ts)

        # Track ALL order IDs ever placed for end-of-session reconciliation
        all_placed_orders: dict[str, dict] = {}

        def _snapshot_new_orders():
            for oid, meta in state.order_map.items():
                if oid not in all_placed_orders:
                    all_placed_orders[oid] = dict(meta)

        def _update_fill_tracking(fill):
            level = state.active_orders.get(fill.order_id)
            if not level:
                return
            level.filled_shares += fill.filled_shares
            if level.filled_shares + 1e-9 >= level.target_shares:
                level.last_fill_at = time.time()
                level.order_id = None
                state.active_orders.pop(fill.order_id, None)
                state.order_map.pop(fill.order_id, None)

        # Place expensive side first — it fills slower, needs a head start.
        # Cheap side fills instantly (takers consume it), so place it second.
        # Fix #42: wrap in try/except — if placement times out, the orders may
        # already be live on the exchange.  Cancel both sides before propagating.
        try:
            if up_bid >= down_bid:
                # UP is expensive → place UP first, then DOWN
                logger.info("Trend: placing expensive side first (up=%.2f >= down=%.2f)", up_bid, down_bid)
                await order_engine.place_side_batch(up_ladder, market.up_token, "up", expiration_ts, state)
                _snapshot_new_orders()
                await order_engine.place_side_batch(down_ladder, market.down_token, "down", expiration_ts, state)
            else:
                # DOWN is expensive → place DOWN first, then UP
                logger.info("Trend: placing expensive side first (down=%.2f > up=%.2f)", down_bid, up_bid)
                await order_engine.place_side_batch(down_ladder, market.down_token, "down", expiration_ts, state)
                _snapshot_new_orders()
                await order_engine.place_side_batch(up_ladder, market.up_token, "up", expiration_ts, state)
            _snapshot_new_orders()
        except Exception as e:
            logger.error("Initial placement failed — cancelling orphaned orders: %s", e)
            await self._cancel_orphaned_orders(market)
            stop_event.set()
            monitor_task.cancel()
            return None

        # Taker entry burst (after maker ladders)
        if cfg.trend_taker_entry_enabled:
            _existing = self._tracker.position
            if _existing.total_shares > 0:
                logger.info(
                    "Trend: skipping taker entry — position exists (%.1f shares)",
                    _existing.total_shares,
                )
            else:
                taker_shares = cfg.trend_taker_entry_shares or cfg.shares_per_order
                taker_budget = cfg.session_capital_limit * cfg.trend_taker_budget_pct

                for side, token_id in [("up", market.up_token), ("down", market.down_token)]:
                    book = await self._client.get_order_book(token_id)
                    asks = book.get("asks", [])
                    if not asks:
                        continue
                    best_ask = min(float(a["price"]) for a in asks)

                    # Price cap for taker entry: allow up to 2c negative edge.
                    # Entry prioritises balanced position over edge; maker fills recover cost.
                    other_bid = down_bid if side == "up" else up_bid
                    max_taker_entry_price = 1.0 - other_bid + 0.02
                    if best_ask > max_taker_entry_price:
                        logger.info(
                            "Taker entry %s: ask %.3f > cap %.3f (other_bid=%.3f) — skipping",
                            side, best_ask, max_taker_entry_price, other_bid,
                        )
                        continue

                    for i in range(cfg.trend_taker_entry_orders):
                        cost = taker_shares * best_ask
                        if state.taker_notional_used + cost > taker_budget:
                            break
                        try:
                            order_id = await self._client.place_taker_order(
                                token_id=token_id, price=best_ask,
                                size=round(taker_shares, 2), side=side,
                                expiration_ts=expiration_ts,
                            )
                            state.taker_notional_used += cost
                            fill_monitor.register_taker_order(
                                order_id, price=best_ask,
                                size=round(taker_shares, 2), token_id=token_id,
                            )
                            state.order_map[order_id] = {"side": side, "price": best_ask, "token_id": token_id, "taker": True}
                        except Exception as e:
                            logger.error("Trend taker entry failed %s: %s", side, e)
                            break

                        # Re-fetch book for next order — previous order may have
                        # consumed the best ask liquidity (BUG 5 fix)
                        if i < cfg.trend_taker_entry_orders - 1:
                            book = await self._client.get_order_book(token_id)
                            asks = book.get("asks", [])
                            if not asks:
                                break
                            best_ask = min(float(a["price"]) for a in asks)

                _snapshot_new_orders()
                logger.info(
                    "Trend taker entry: spent $%.2f of $%.2f budget",
                    state.taker_notional_used, taker_budget,
                )

        # Trend state
        up_anchor_bid = up_bid
        down_anchor_bid = down_bid
        _init_ts = time.time()
        last_reanchor_ts = {"up": _init_ts, "down": _init_ts}  # Fix #79F: start at now, not 0
        _last_replenish_ts = {"up": 0.0, "down": 0.0}  # Fix #79G: per-side replenish cooldown
        last_side_fill_ts = {"up": 0.0, "down": 0.0}
        last_heartbeat = time.time()
        last_mini_hb = time.time()
        last_reanchor_check = time.time()
        last_drift_update = time.time()
        last_reconcile_ts = time.time()
        _ev_locked = False
        _worst_locked = False
        _profit = False
        _taker_active = False
        _last_taker_rebalance_ts = 0.0
        _pending_taker_order_id: Optional[str] = None
        _skip_replenish_sides: set = set()
        _hot_side_until: dict[str, float] = {"up": 0.0, "down": 0.0}
        _side_fill_times: dict[str, list] = {"up": [], "down": []}  # rolling window timestamps
        _reanchor_avg_blocked_until: dict[str, float] = {"up": 0.0, "down": 0.0}
        _catch_ladder_active: dict[str, bool] = {"up": False, "down": False}
        _session_entry_ts = time.time()

        # Per-side share cap: auto = capital / avg_cost_target (balanced max)
        _max_per_side = cfg.trend_max_shares_per_side
        if _max_per_side <= 0:
            _max_per_side = int(cfg.session_capital_limit / max(cfg.trend_avg_cost_target, 0.30))
        logger.info(
            "Trend caps: max_per_side=%d, balance_ratio=%.1f, worst_lock=%s (threshold=$%.1f)",
            _max_per_side, cfg.trend_balance_ratio,
            "ON" if cfg.trend_worst_lock_enabled else "OFF",
            cfg.trend_worst_lock_threshold,
        )

        try:
            while not self._shutdown:
                phase = self._scheduler.get_current_phase(market)

                if phase in (MarketPhase.CLOSED, MarketPhase.RESOLVED):
                    break

                # WINDING_DOWN: cancel all, wait for resolution
                if (
                    phase == MarketPhase.WINDING_DOWN
                    and state.phase != MarketPhase.WINDING_DOWN
                ):
                    state.phase = MarketPhase.WINDING_DOWN
                    logger.info("Trend WINDING_DOWN — cancelling all orders")
                    await self._client.cancel_and_verify(market.up_token)
                    await self._client.cancel_and_verify(market.down_token)
                    for oid, level in list(state.active_orders.items()):
                        order_engine.on_cancel_release(level, state)
                    state.active_orders.clear()
                    state.order_map.clear()
                    # Fix #59: WINDING_DOWN taker flatten — only if cost-effective.
                    pos = self._tracker.position
                    if pos.share_imbalance >= 0.10 and pos.total_shares >= 10:
                        lagging_side_wd = "up" if pos.up_shares < pos.down_shares else "down"
                        lagging_token_wd = market.up_token if lagging_side_wd == "up" else market.down_token
                        try:
                            book_wd = await self._client.get_order_book(lagging_token_wd)
                            asks_wd = book_wd.get("asks", [])
                            best_ask_wd = min(float(a["price"]) for a in asks_wd) if asks_wd else 1.0
                        except Exception:
                            best_ask_wd = 1.0
                        if best_ask_wd <= 0.80:
                            logger.info(
                                "Trend WINDING_DOWN: taker flatten (imbalance=%.1f%%, deficit=%.0f, ask=%.0fc)",
                                pos.share_imbalance * 100, pos.excess_shares, best_ask_wd * 100,
                            )
                            await self._try_taker_flatten(
                                market, state, fill_monitor, skip_price_cap=True,
                            )
                        else:
                            logger.info(
                                "Trend WINDING_DOWN: skipping taker flatten — ask %.0fc too expensive "
                                "(only %.0fc improvement per share, not worth it)",
                                best_ask_wd * 100, (1.0 - best_ask_wd) * 100,
                            )

                    # Bargain hunting: place cheap limit orders on both sides.
                    # Near close, volatility spikes — cheap fills below avg cost are free EV.
                    # Cap at current avg cost so every fill improves the position.
                    pos = self._tracker.position
                    if pos.total_shares > 0:
                        _bargain_cap_cents = max(1, int(pos.avg_cost_per_share * 100) - 1)
                        # Use fresh WS bids for ladder anchoring
                        _bg_up_bid = up_anchor_bid
                        _bg_down_bid = down_anchor_bid
                        if self._ws_feed is not None and self._ws_feed.is_connected:
                            _wb = self._ws_feed.get_best_bid(market.up_token)
                            if _wb is not None:
                                _bg_up_bid = _wb
                            _wb = self._ws_feed.get_best_bid(market.down_token)
                            if _wb is not None:
                                _bg_down_bid = _wb
                        for _bg_side, _bg_token, _bg_bid in [
                            ("up", market.up_token, _bg_up_bid),
                            ("down", market.down_token, _bg_down_bid),
                        ]:
                            _bg_ladder = build_trend_ladder(
                                _bg_bid, market.tick_size, cfg.shares_per_order,
                                num_levels=cfg.trend_levels_per_side,
                                step_cents=cfg.trend_step_cents,
                                max_buy_price_cents=_bargain_cap_cents,
                            )
                            if _bg_ladder:
                                try:
                                    _bg_placed = await order_engine.place_side_batch(
                                        _bg_ladder, _bg_token, _bg_side,
                                        expiration_ts, state,
                                        check_avg_cost=True,
                                    )
                                    if _bg_placed > 0:
                                        logger.info(
                                            "WINDING_DOWN bargain %s: %d orders (cap=%dc, range=%dc-%dc)",
                                            _bg_side, _bg_placed, _bargain_cap_cents,
                                            _bg_ladder[-1].price_cents, _bg_ladder[0].price_cents,
                                        )
                                except Exception as e:
                                    logger.debug("WINDING_DOWN bargain %s failed: %s", _bg_side, e)

                # Snapshot before drain
                _snapshot_new_orders()

                # Drain fills
                side_fills = drain_fills(
                    fill_queue, self._tracker, state, order_engine,
                    _update_fill_tracking,
                )
                # Clean up stale cancelled orders (prevents headroom bloat)
                cleanup_cancelled_orders(state, max_age=10.0)
                # Track last fill time per side
                now_ts = time.time()
                if side_fills.get("up", 0) > 0:
                    last_side_fill_ts["up"] = now_ts
                    _reanchor_avg_blocked_until["up"] = 0.0  # Fix #53c: fills change avg, unblock
                    _catch_ladder_active["up"] = False  # Fix #60: fills change position, re-evaluate
                if side_fills.get("down", 0) > 0:
                    last_side_fill_ts["down"] = now_ts
                    _reanchor_avg_blocked_until["down"] = 0.0  # Fix #53c: fills change avg, unblock
                    _catch_ladder_active["down"] = False  # Fix #60: fills change position, re-evaluate

                # Force USDC drift update after any fill for accurate PROTECT decisions
                if any(side_fills.values()):
                    try:
                        _usdc_now_fill = await self._client.get_balance()
                        if _usdc_start is not None:
                            _usdc_spent_fill = _usdc_start - _usdc_now_fill
                            _usdc_cost_drift = _usdc_spent_fill - self._tracker.position.total_cost
                            self._usdc_cost_drift = _usdc_cost_drift
                            last_drift_update = now_ts
                    except Exception:
                        pass

                # Hot-side detection: pause replenishment when one side fills too fast
                if now_ts - _session_entry_ts >= 15.0:  # grace period for initial fills
                    for hs_side in ("up", "down"):
                        hs_count = side_fills.get(hs_side, 0)
                        if hs_count <= 0:
                            continue
                        # Record fill timestamps for rolling-window detection
                        _side_fill_times[hs_side].extend([now_ts] * hs_count)
                        # Burst detection (existing): many fills in one drain cycle
                        hs_ladder = up_ladder if hs_side == "up" else down_ladder
                        total_levels = max(1, len(hs_ladder))
                        fill_ratio = hs_count / total_levels
                        if hs_count >= 2 and fill_ratio >= cfg.hot_side_fill_ratio:
                            _hot_side_until[hs_side] = now_ts + cfg.hot_side_cooldown_s
                            logger.warning(
                                "Hot side detected (burst): %s — %d/%d levels filled (%.0f%%), pausing %.0fs",
                                hs_side, hs_count, total_levels,
                                fill_ratio * 100, cfg.hot_side_cooldown_s,
                            )
                    # Rolling-window detection: sustained fill rate over trailing window
                    for hs_side in ("up", "down"):
                        window = cfg.hot_side_rolling_window_s
                        cutoff = now_ts - window
                        _side_fill_times[hs_side] = [t for t in _side_fill_times[hs_side] if t > cutoff]
                        rolling_count = len(_side_fill_times[hs_side])
                        if rolling_count >= cfg.hot_side_rolling_threshold and _hot_side_until[hs_side] <= now_ts:
                            _hot_side_until[hs_side] = now_ts + cfg.hot_side_cooldown_s
                            logger.warning(
                                "Hot side detected (rolling): %s — %d fills in %.0fs window, pausing %.0fs",
                                hs_side, rolling_count, window, cfg.hot_side_cooldown_s,
                            )

                # Kill command check
                killed = await self._alerts.check_kill_command()
                if killed:
                    logger.warning("Trend: kill command received")
                    self._shutdown = True
                    break

                # Fill monitor health
                if state.total_trades > 0 and fill_monitor.last_poll_age > 30.0:
                    logger.warning("Trend: fill monitor stale (%.0fs)", fill_monitor.last_poll_age)

                now = time.time()

                # --- Worst-case profit lock (primary safety) ---
                # When worst_case >= threshold (default $0), the position is guaranteed
                # profitable at resolution. Any further buying can only create imbalance
                # risk. Cancel all and freeze.
                if cfg.trend_worst_lock_enabled and state.phase == MarketPhase.ACTIVE:
                    # Fix #71: Drift-corrected worst for lock decisions.
                    _worst = self._tracker.position.risk_worst_case - _usdc_cost_drift
                    _min_sh = cfg.shares_per_order * 4  # need some position first
                    if (
                        not _worst_locked
                        and self._tracker.position.total_shares >= _min_sh
                        and _worst >= cfg.trend_worst_lock_threshold
                    ):
                        _worst_locked = True
                        pos = self._tracker.position
                        logger.info(
                            "Trend WORST LOCK: worst=$%.2f >= $%.2f — freezing. "
                            "Up=%.1f Down=%.1f avg=$%.3f EV=$%.1f cost=$%.1f",
                            _worst, cfg.trend_worst_lock_threshold,
                            pos.up_shares, pos.down_shares,
                            pos.avg_cost_per_share, pos.expected_profit,
                            pos.total_cost,
                        )
                        try:
                            await self._client.cancel_and_verify(market.up_token)
                            await self._client.cancel_and_verify(market.down_token)
                            for oid, level in list(state.active_orders.items()):
                                order_engine.on_cancel_release(level, state)
                            state.active_orders.clear()
                            state.order_map.clear()
                        except Exception as e:
                            logger.warning("Trend WORST LOCK cancel failed: %s", e)
                    elif (
                        _worst_locked
                        and _worst < cfg.trend_worst_lock_unlock
                    ):
                        _worst_locked = False
                        logger.info(
                            "Trend WORST UNLOCK: worst=$%.2f < $%.2f — resuming",
                            _worst, cfg.trend_worst_lock_unlock,
                        )
                        last_reanchor_ts["up"] = 0.0
                        last_reanchor_ts["down"] = 0.0

                _any_locked = _ev_locked or _worst_locked

                if state.phase == MarketPhase.ACTIVE and not _any_locked:
                    # --- Helper: graduated balance ratio ---
                    def _effective_balance_ratio(total_shares: float) -> float:
                        """Smooth balance ratio: tightens continuously as position grows.

                        Fix #67A: Replace step tiers with continuous decay.
                        Values: 0→1.5x, 50→1.4x, 100→1.33x, 200→1.25x, 500→1.14x, 1000→1.08x
                        """
                        k = 0.5      # max excess at 0 shares (1.0 + 0.5 = 1.5x)
                        scale = 200  # half-life in shares
                        return 1.0 + k / (1.0 + total_shares / scale)

                    # --- Helper: check if side is blocked by caps/balance ---
                    # Pessimistic pending shares: orders placed this cycle that
                    # may fill before the next drain. Reset each loop iteration.
                    _pending_replenish = {"up": 0.0, "down": 0.0}

                    def _side_blocked(side: str) -> bool:
                        pos = self._tracker.position
                        up_s, down_s = pos.up_shares, pos.down_shares
                        # Add pessimistic pending shares from this replenish cycle
                        up_s += _pending_replenish["up"]
                        down_s += _pending_replenish["down"]
                        side_s = up_s if side == "up" else down_s
                        other_s = down_s if side == "up" else up_s
                        # Per-side share cap
                        if side_s >= _max_per_side:
                            return True
                        # Balance ratio: heavy side blocked when ratio > threshold
                        min_s = cfg.shares_per_order * 2
                        ratio_limit = _effective_balance_ratio(up_s + down_s)
                        if side_s > min_s and other_s > 0 and side_s / other_s > ratio_limit:
                            return True
                        if side_s > min_s and other_s == 0 and side_s > cfg.shares_per_order * 4:
                            return True
                        return False

                    # --- Replenish filled levels (per-order balance check) ---
                    replenish_cap = cfg.trend_replenish_cap
                    pos = self._tracker.position
                    _lag_side = "up" if pos.up_shares < pos.down_shares else "down" if pos.down_shares < pos.up_shares else ""
                    _heavy_side = "down" if _lag_side == "up" else "up" if _lag_side == "down" else ""
                    _rebal_target = cfg.trend_rebalance_avg_cost_limit
                    _now_hs = time.time()

                    # Fix #65/70: When worst-case is negative, suppress heavy side.
                    # Every heavy fill adds cost without increasing min(up,down),
                    # directly worsening worst. Even "bargain" fills at 28c cost
                    # $2.80 that drops worst by $2.80.
                    # Fix #74: Taker uses pair-profitability cap (heavy_avg + ask < 1.0)
                    # instead of flat 50c. Allows rebalancing when lagging side is expensive.
                    _taker_active = False
                    if (
                        cfg.trend_taker_rebalance_enabled
                        and _heavy_side
                        and pos.total_shares > 0
                    ):
                        _worst_rep = pos.risk_worst_case - _usdc_cost_drift
                        # Fix #81: Strict less-than prevents taker flag when worst
                        # is exactly at or above tier1 (e.g. worst=+$12, tier1=$0).
                        if _worst_rep < cfg.trend_taker_tier1_worst:
                            _taker_active = True

                    # Fix #68/75: Profit protection mode.
                    # When REAL worst-case >= threshold, freeze ALL order placement.
                    # Fix #75: Freeze BOTH sides, not just heavy. Previously allowed
                    # lagging-side accumulation which destroyed locked profit when
                    # price reversed (e.g. +$44 REAL worst → -$23 in 60s).
                    # Fix #71: Use drift-corrected worst for accurate decisions.
                    # Fix #77: Fully sticky — once PROTECT activates, it stays on for
                    # the entire session.  Previous hysteresis (deactivate at -$2)
                    # caused PROTECT to flicker on/off with market swings, letting the
                    # bot dump orders into volatile markets during "off" moments.
                    # Fix #79A: Use RAW worst (no drift correction) + balance gate.
                    # Drift-corrected worst was based on stale USDC balance which
                    # caused fake profit ($51.6 from stale drift, real was -$42).
                    # Also require reasonable balance (< 2.0x ratio) to prevent
                    # PROTECT activation on lopsided positions.
                    _min_side = min(pos.up_shares, pos.down_shares)
                    _max_side = max(pos.up_shares, pos.down_shares)
                    _balance_ok = _min_side > 0 and (_max_side / _min_side) < 2.0
                    _was_profit = _profit_sticky
                    if not _profit_sticky:
                        _profit_sticky = (
                            pos.total_shares >= cfg.shares_per_order * 15
                            and pos.risk_worst_case >= cfg.trend_profit_protect_threshold  # raw, no drift
                            and _balance_ok
                        )
                    _profit = _profit_sticky

                    # When PROTECT just activated, cancel ALL open orders.
                    # Existing orders (especially expensive ones) can still fill
                    # and destroy the profit we're trying to protect.
                    if _profit and not _was_profit:
                        # Fix #79C: Lock lagging side at activation — prevents side-flipping
                        _protect_lag_side = _lag_side
                        logger.info(
                            "PROTECT activated (raw worst=$%.1f, lag=%s) — cancelling all open orders",
                            pos.risk_worst_case, _protect_lag_side,
                        )
                        await self._client.cancel_and_verify(market.up_token)
                        await self._client.cancel_and_verify(market.down_token)
                        for _oid, _lvl in list(state.active_orders.items()):
                            order_engine.on_cancel_release(_lvl, state)
                        state.active_orders.clear()
                        state.order_map.clear()

                    # Fix #79E: PROTECT replenishment cooldown (5s)
                    # Fix #79G: Normal replenishment cooldown (2s per side)
                    _replenish_cooldown = 5.0 if _profit else 2.0
                    for _rep_side, _rep_ladder, _rep_token in [
                        ("up", up_ladder, market.up_token),
                        ("down", down_ladder, market.down_token),
                    ]:
                        if _rep_side in _skip_replenish_sides or _side_blocked(_rep_side):
                            continue
                        # Fix #80: NO replenishment during PROTECT.
                        # Previously replenished lagging side, but in a trending market
                        # those orders get swept instantly (90 shares in 30s),
                        # turning $4 profit into -$40 loss. PROTECT means PROTECT.
                        if _profit:
                            continue
                        # Fix #70: Full heavy-side suppression when taker active
                        _rep_frozen = (
                            _taker_active and (_rep_side == _heavy_side or not _heavy_side)
                        )
                        if _rep_frozen:
                            continue
                        if _now_hs < _hot_side_until.get(_rep_side, 0.0):
                            continue
                        # Fix #79E/G: Per-side replenishment cooldown
                        if _profit and (_now_hs - _protect_replenish_ts) < _replenish_cooldown:
                            continue
                        if not _profit and (_now_hs - _last_replenish_ts[_rep_side]) < _replenish_cooldown:
                            continue
                        # Fix #79H: Skip replenishment when re-anchor is imminent.
                        # Both drift AND cooldown must be ready — if cooldown hasn't
                        # expired, re-anchor won't fire and replenishment should proceed.
                        _rep_anchor = up_anchor_bid if _rep_side == "up" else down_anchor_bid
                        _ra_cooldown_ok = (_now_hs - last_reanchor_ts[_rep_side]) >= cfg.trend_reanchor_cooldown_s
                        if _ra_cooldown_ok:
                            _ws_bid = (
                                self._ws_feed.get_best_bid(_rep_token)
                                if self._ws_feed and self._ws_feed.is_connected
                                else None
                            )
                            if _ws_bid is not None:
                                _rep_drift = abs(_ws_bid - _rep_anchor)
                                _rep_min_drift = max(cfg.trend_reanchor_drift, _rep_anchor * 0.05)
                                if round(_rep_drift * 100) > round(_rep_min_drift * 100):
                                    continue  # re-anchor imminent — skip replenishment
                        # Get WS best ask — fail-closed: no data = no replenishment
                        _rep_ask_cents = 0
                        if self._ws_feed is not None:
                            _rep_ask = self._ws_feed.get_best_ask(_rep_token)
                            if _rep_ask is not None:
                                _rep_ask_cents = int(round(_rep_ask * 100))
                        if _rep_ask_cents <= 0:
                            continue  # no ask data — skip replenishment
                        # Filter levels at/above ask (would cross spread = taker)
                        _rep_eligible = [
                            lvl for lvl in _rep_ladder
                            if lvl.price_cents < _rep_ask_cents
                        ]
                        # Place replenishment
                        _rep_avg_override = 0 if _profit else (_rebal_target if _rep_side == _lag_side else 0)
                        _rep_placed = await order_engine.replenish_batch(
                            _rep_eligible, _rep_side, _rep_token, expiration_ts, state,
                            max_replenishments=replenish_cap,
                            check_avg_cost=True,
                            avg_cost_target_override=_rep_avg_override,
                        )
                        if _rep_placed > 0:
                            _pending_replenish[_rep_side] += _rep_placed * cfg.shares_per_order
                            if _profit:
                                _protect_replenish_ts = _now_hs
                            _last_replenish_ts[_rep_side] = _now_hs

                    # Snapshot replenishment orders immediately. They can be cancelled
                    # later in this same loop iteration by re-anchors/locks, and we'd
                    # otherwise miss them in end-of-session order-based reconciliation.
                    _snapshot_new_orders()

                    # --- Taker rebalance: cancel-and-recalculate pattern ---
                    # Cancel previous unfilled taker order before recalculating.
                    # Refund budget for unfilled portion to prevent phantom depletion.
                    if _pending_taker_order_id is not None:
                        try:
                            order_info = await self._client.get_order(_pending_taker_order_id)
                            status = (order_info or {}).get("status", "")
                            if status in ("LIVE", "ACTIVE"):
                                # Refund unfilled portion of budget
                                pending_meta = state.order_map.get(_pending_taker_order_id, {})
                                orig_size = float(pending_meta.get("original_size", 0))
                                orig_price = float(pending_meta.get("price", 0))
                                filled = float((order_info or {}).get("size_matched", 0))
                                unfilled_cost = (orig_size - filled) * orig_price
                                if unfilled_cost > 0:
                                    state.taker_notional_used = max(0.0, state.taker_notional_used - unfilled_cost)
                                await self._client.cancel_order(_pending_taker_order_id)
                                logger.info(
                                    "Cancelled stale taker rebalance order %s (refunded $%.2f)",
                                    _pending_taker_order_id, unfilled_cost,
                                )
                            # Terminal state (filled/cancelled) — safe to clear
                            _pending_taker_order_id = None
                        except Exception as e:
                            # Keep pending ID to retry next cycle (BUG 7 fix)
                            logger.warning(
                                "Failed to check pending taker order %s: %s — will retry",
                                _pending_taker_order_id, e,
                            )

                    # --- Taker rebalance ---
                    # Fix #78: If deficit and worst is bad, buy lagging side.
                    # Cheap ask (< 50c) → full deficit in one shot (EV-positive).
                    # Expensive ask (>= 50c) → shares_per_order at a time.
                    if (
                        cfg.trend_taker_rebalance_enabled
                        and phase in (MarketPhase.ACTIVE, MarketPhase.WINDING_DOWN)
                        and not _ev_locked and not _worst_locked
                        and (now - _session_entry_ts) >= cfg.trend_taker_grace_period_s
                    ):
                        _any_hot = _hot_side_until["up"] > now or _hot_side_until["down"] > now

                        pos = self._tracker.position
                        deficit = abs(pos.up_shares - pos.down_shares)
                        _worst = (pos.risk_worst_case - _usdc_cost_drift) if pos.total_shares > 0 else 0.0
                        _has_deficit = deficit >= cfg.shares_per_order

                        # Fix #81: Strict less-than (consistent with _taker_active check)
                        _should_taker = _has_deficit and _worst < cfg.trend_taker_tier1_worst

                        # Graduated taker size: 5 → 7 → 10 by tier
                        _taker_size = 5
                        if _worst < cfg.trend_taker_tier3_worst:
                            _taker_size = 10
                        elif _worst < cfg.trend_taker_tier2_worst:
                            _taker_size = 7

                        if (
                            _should_taker
                            and not _any_hot
                            and (now - _last_taker_rebalance_ts) >= 2.0
                        ):
                            _pending_taker_order_id = await self._try_taker_flatten(
                                market, state, fill_monitor,
                                max_shares_override=_taker_size,
                            )
                            _last_taker_rebalance_ts = now
                            _snapshot_new_orders()

                    # --- Re-anchor check ---
                    check_interval = cfg.trend_reanchor_check_interval_s
                    if check_interval <= 0 or (now - last_reanchor_check) >= check_interval:
                        # Only clear skip flags for sides whose cancelled orders
                        # have been cleaned up. This prevents replenish from piling
                        # new orders onto a side where headroom is exhausted by
                        # pending cancelled entries. The flag persists until cleanup
                        # expires the cancelled orders (~10s), then re-anchor can
                        # successfully place fresh orders.
                        for _s in list(_skip_replenish_sides):
                            _has_pending = any(
                                m.get("cancelled") and m.get("side") == _s
                                for m in state.order_map.values()
                            )
                            if not _has_pending:
                                _skip_replenish_sides.discard(_s)
                        last_reanchor_check = now
                        try:
                            # Use WS cache for instant price reads (no API call)
                            new_up_bid = (
                                self._ws_feed.get_best_bid(market.up_token)
                                if self._ws_feed and self._ws_feed.is_connected
                                else None
                            )
                            new_down_bid = (
                                self._ws_feed.get_best_bid(market.down_token)
                                if self._ws_feed and self._ws_feed.is_connected
                                else None
                            )
                            # Fall back to REST if WS unavailable
                            if new_up_bid is None or new_down_bid is None:
                                fresh_up_book = await self._client.get_order_book(market.up_token)
                                fresh_down_book = await self._client.get_order_book(market.down_token)
                                if new_up_bid is None:
                                    new_up_bid = _get_best_bid_price(fresh_up_book)
                                if new_down_bid is None:
                                    new_down_bid = _get_best_bid_price(fresh_down_book)

                            # Check each side for re-anchor
                            _ra_now = time.time()
                            for side, old_anchor, new_bid, ladder, token_id in [
                                ("up", up_anchor_bid, new_up_bid, up_ladder, market.up_token),
                                ("down", down_anchor_bid, new_down_bid, down_ladder, market.down_token),
                            ]:
                                # Skip re-anchor on blocked side
                                if _side_blocked(side):
                                    continue

                                # Fix #52E: Skip re-anchor on hot side for BOTH sides.
                                # Previously only heavy side was gated, but lagging side
                                # re-anchor → instant fills → re-anchor creates sustained
                                # accumulation loop. Replenishment still gets lagging bypass.
                                if _ra_now < _hot_side_until.get(side, 0.0):
                                    continue

                                # Fix #79B: Freeze ALL re-anchors during PROTECT.
                                # Previously only heavy side was frozen, but lagging-side
                                # re-anchors were causing avalanche fills that destroyed
                                # the locked profit.
                                if _profit:
                                    continue

                                # Fix #65/70: Full heavy-side re-anchor suppression
                                # when taker is active. Every heavy fill (even cheap)
                                # increases total_cost → worsens worst.
                                if _taker_active and side == _heavy_side:
                                    continue

                                # Fix #67B: No upward re-anchor for heavy side.
                                # Don't chase price up — stale orders below bid are cheaper fills.
                                # Lagging side CAN re-anchor up (cheaper than taker, helps balance).
                                if side == _heavy_side and new_bid > old_anchor:
                                    continue

                                drift = abs(new_bid - old_anchor)
                                _base_drift = cfg.trend_reanchor_drift
                                # Relative drift: 5% of anchor price, floored at base drift.
                                # Prevents re-anchor ping-pong from normal 2-3c bid noise
                                # at 50-80c prices (e.g. 77c→79c→77c oscillation).
                                # At 80c: max(0.03, 0.04) = 4c.  At 30c: max(0.03, 0.015) = 3c.
                                # Compare in integer cents to avoid floating-point edge cases
                                # (e.g. abs(0.31-0.33) = 0.02000000000000018 > 0.02).
                                min_drift = max(_base_drift, old_anchor * 0.05)
                                drift_cents = round(drift * 100)
                                min_drift_cents = round(min_drift * 100)
                                if drift_cents <= min_drift_cents:
                                    continue
                                if (now - last_reanchor_ts[side]) < cfg.trend_reanchor_cooldown_s:
                                    continue
                                if (now - last_side_fill_ts[side]) < cfg.trend_fill_grace_s:
                                    continue

                                # Fix #52C: Skip re-anchor when budget too low for useful placement.
                                # Prevents wasted cancel+rebuild cycles when budget is exhausted.
                                _remaining_budget = cfg.session_capital_limit - (
                                    self._tracker.position.total_cost
                                    + state.reserved_notional
                                    + state.cancel_race_buffer
                                )
                                _min_useful_cost = 3 * cfg.shares_per_order * 0.30  # cheapest possible 3 levels
                                if _remaining_budget < _min_useful_cost:
                                    continue

                                # Fix #53c: Skip re-anchor if avg cost churn cooldown active
                                if now < _reanchor_avg_blocked_until.get(side, 0.0):
                                    continue

                                # Save real market bid before any catch-anchor override
                                _real_market_bid = new_bid

                                # Fix #58a: Pre-check avg cost BEFORE cancelling.
                                # If new ladder is entirely too expensive, don't
                                # cancel existing orders just to place nothing.
                                pos_pre = self._tracker.position
                                _is_lagging_pre = (
                                    (side == "up" and pos_pre.up_shares < pos_pre.down_shares) or
                                    (side == "down" and pos_pre.down_shares < pos_pre.up_shares)
                                )
                                _avg_target_pre = cfg.trend_avg_cost_target
                                _current_avg_pre = pos_pre.avg_cost_per_share if pos_pre.avg_cost_per_share > 0 else 0.50
                                _market_too_expensive = False
                                if pos_pre.total_shares > 0 and _avg_target_pre > 0:
                                    _bottom_pre = new_bid - cfg.trend_levels_per_side * cfg.trend_step_cents / 100.0
                                    _bottom_pre = max(_bottom_pre, 0.01)
                                    _proj_avg_pre = (pos_pre.total_cost + _bottom_pre * cfg.shares_per_order) / (pos_pre.total_shares + cfg.shares_per_order)
                                    if _proj_avg_pre > _avg_target_pre and _bottom_pre >= _current_avg_pre:
                                        _market_too_expensive = True
                                        if _is_lagging_pre:
                                            # Fix #60: If catch ladder already active for this side,
                                            # don't cancel+replace with identical orders. Just update
                                            # anchor to current market bid and skip.
                                            if _catch_ladder_active.get(side):
                                                if side == "up":
                                                    up_anchor_bid = _real_market_bid
                                                else:
                                                    down_anchor_bid = _real_market_bid
                                                last_reanchor_ts[side] = now
                                                continue

                                            # Fix #58b: Lagging side too expensive to chase.
                                            # Build "catch" ladder at sensible prices instead.
                                            # Anchor at current avg cost — orders below avg
                                            # always improve position via escape hatch.
                                            _catch_anchor = min(_current_avg_pre, _avg_target_pre)
                                            logger.info(
                                                "Trend re-anchor %s: bid %.0fc too expensive, "
                                                "using catch anchor at %.0fc (avg=%.3f)",
                                                side, new_bid * 100, _catch_anchor * 100, _current_avg_pre,
                                            )
                                            new_bid = _catch_anchor
                                        else:
                                            # Heavy side — skip entirely, keep existing orders
                                            logger.info(
                                                "Trend re-anchor %s: avg cost too high — "
                                                "even bottom %.0fc > avg %.3f, keeping existing orders",
                                                side, _bottom_pre * 100, _current_avg_pre,
                                            )
                                            if side == "up":
                                                up_anchor_bid = new_bid
                                            else:
                                                down_anchor_bid = new_bid
                                            last_reanchor_ts[side] = now
                                            continue

                                # Re-anchor this side
                                logger.info(
                                    "Trend re-anchor %s: bid %.3f→%.3f (drift=%.3f)",
                                    side, old_anchor, new_bid, drift,
                                )

                                # Cancel all orders on this side + verify
                                await self._client.cancel_market_orders(token_id)
                                # Verify and cancel stragglers individually
                                try:
                                    remaining = await self._client.get_open_orders(token_id)
                                    remaining = [
                                        o for o in remaining
                                        if o.get("asset_id") == token_id
                                    ]
                                    if remaining:
                                        logger.warning(
                                            "Re-anchor %s: %d orders survived cancel_market_orders — cancelling individually",
                                            side, len(remaining),
                                        )
                                        for o in remaining:
                                            oid = o.get("id", o.get("order_id", ""))
                                            if oid:
                                                try:
                                                    await self._client.cancel_order(oid)
                                                except Exception:
                                                    pass
                                except Exception as e:
                                    logger.warning("Re-anchor verify cancel failed: %s", e)

                                # Drain fills BEFORE clearing state
                                drain_fills(
                                    fill_queue, self._tracker, state, order_engine,
                                    _update_fill_tracking,
                                )

                                # Release ALL orders on this side — both ladder levels
                                # and replenished orders that aren't in the ladder.
                                cancelled_oids = []
                                ladder_oids = set()
                                for level in ladder:
                                    oid = level.order_id
                                    if oid is not None:
                                        order_engine.on_cancel_release(level, state)
                                        state.active_orders.pop(oid, None)
                                        cancelled_oids.append(oid)
                                        ladder_oids.add(oid)
                                # Also mark any replenished/orphan orders on this side
                                for oid, meta in list(state.order_map.items()):
                                    if meta.get("side") == side and oid not in ladder_oids and "cancel_ts" not in meta:
                                        cancelled_oids.append(oid)
                                        state.active_orders.pop(oid, None)
                                        # Release reserved_notional for non-ladder orders
                                        price = meta.get("price", 0)
                                        orig_size = meta.get("original_size", 0)
                                        if price > 0 and orig_size > 0:
                                            release = orig_size * price
                                            state.reserved_notional = max(0, state.reserved_notional - release)
                                if cancelled_oids:
                                    mark_orders_cancelled(state, cancelled_oids)

                                # Re-check after drain — position may have changed
                                if _side_blocked(side):
                                    logger.info(
                                        "Trend re-anchor %s: blocked after drain (ratio exceeded) — skipping placement",
                                        side,
                                    )
                                    _skip_replenish_sides.add(side)
                                    if side == "up":
                                        up_anchor_bid = new_bid
                                    else:
                                        down_anchor_bid = new_bid
                                    last_reanchor_ts[side] = now
                                    continue

                                # Headroom-based level cap for heavy side
                                pos = self._tracker.position
                                up_s, down_s = pos.up_shares, pos.down_shares
                                side_s = up_s if side == "up" else down_s
                                other_s = down_s if side == "up" else up_s
                                if side_s >= other_s and other_s > 0:
                                    ratio_limit = _effective_balance_ratio(up_s + down_s)
                                    # Pending cancel-race shares (may still fill).
                                    # Only count cancellations 1-3s old as risk.
                                    # < 1s: current re-anchor cycle (being replaced)
                                    # > 3s: almost certainly dead
                                    _now_hr = time.time()
                                    cancelled_same_side = sum(
                                        1 for m in state.order_map.values()
                                        if m.get("cancelled") and m.get("side") == side
                                        and 1.0 < (_now_hr - m.get("cancel_ts", 0)) < 3.0
                                    )
                                    pending_shares = cancelled_same_side * cfg.shares_per_order
                                    effective_side_s = side_s + pending_shares
                                    ratio_headroom = max(0.0, (ratio_limit * other_s) - effective_side_s)
                                    taker_budget = cfg.session_capital_limit * cfg.trend_taker_budget_pct
                                    taker_remaining = taker_budget - state.taker_notional_used
                                    lagging_ask = 1.0 - new_bid
                                    taker_headroom = taker_remaining / max(lagging_ask, 0.01)
                                    max_new_shares = min(ratio_headroom, taker_headroom)
                                    actual_levels = min(
                                        cfg.trend_levels_per_side,
                                        int(max_new_shares / cfg.shares_per_order),
                                    )
                                    if actual_levels < 1:
                                        logger.info(
                                            "Trend re-anchor %s: headroom too small (%d levels, %d pending cancelled) — skipping",
                                            side, actual_levels, cancelled_same_side,
                                        )
                                        _skip_replenish_sides.add(side)
                                        if side == "up":
                                            up_anchor_bid = new_bid
                                        else:
                                            down_anchor_bid = new_bid
                                        last_reanchor_ts[side] = now
                                        continue
                                else:
                                    # Asymmetric ladder: lagging side gets extra levels
                                    # proportional to deficit so it can catch up (Fix #52).
                                    pos_asym = self._tracker.position
                                    _side_shares = pos_asym.up_shares if side == "up" else pos_asym.down_shares
                                    _other_shares = pos_asym.down_shares if side == "up" else pos_asym.up_shares
                                    _deficit = max(0, _other_shares - _side_shares)
                                    _extra = min(
                                        int(_deficit / max(cfg.shares_per_order, 1)),
                                        2,  # Fix #57b: cap at +2 extra levels (was +3)
                                    )
                                    actual_levels = cfg.trend_levels_per_side + _extra

                                # Post-cancel avg cost guard (backup — pre-check should
                                # have caught this, but position may have changed from
                                # drain_fills above).
                                pos = self._tracker.position
                                _is_lagging = (
                                    (side == "up" and pos.up_shares < pos.down_shares) or
                                    (side == "down" and pos.down_shares < pos.up_shares)
                                )
                                _avg_target = cfg.trend_avg_cost_target
                                if pos.total_shares > 0 and _avg_target > 0:
                                    _bottom_price = new_bid - actual_levels * cfg.trend_step_cents / 100.0
                                    _bottom_price = max(_bottom_price, 0.01)
                                    proj_cost = pos.total_cost + _bottom_price * cfg.shares_per_order
                                    proj_shares = pos.total_shares + cfg.shares_per_order
                                    proj_avg = proj_cost / proj_shares
                                    _current_avg = pos.avg_cost_per_share if pos.avg_cost_per_share > 0 else 0.50
                                    if proj_avg > _avg_target and _bottom_price >= _current_avg:
                                        if _is_lagging:
                                            # Fix #58b: Already cancelled — use catch anchor
                                            _catch_anchor = min(_current_avg, _avg_target)
                                            logger.info(
                                                "Trend re-anchor %s: post-cancel, using catch anchor %.0fc",
                                                side, _catch_anchor * 100,
                                            )
                                            new_bid = _catch_anchor
                                        else:
                                            logger.info(
                                                "Trend re-anchor %s: avg cost too high post-cancel — "
                                                "bottom %.0fc projects %.4f > %.3f",
                                                side, _bottom_price * 100, proj_avg, _avg_target,
                                            )
                                            if side == "up":
                                                up_anchor_bid = new_bid
                                            else:
                                                down_anchor_bid = new_bid
                                            last_reanchor_ts[side] = now
                                            continue

                                # Fix #60b: Cap catch ladder at deficit.
                                # Extra shares beyond matching the other side don't
                                # improve worst-case — they just add cost.
                                if new_bid != _real_market_bid:
                                    _pos_catch = self._tracker.position
                                    _catch_side_shares = _pos_catch.up_shares if side == "up" else _pos_catch.down_shares
                                    _catch_other_shares = _pos_catch.down_shares if side == "up" else _pos_catch.up_shares
                                    _catch_deficit = max(0, _catch_other_shares - _catch_side_shares)
                                    _deficit_levels = round(_catch_deficit / max(cfg.shares_per_order, 1))
                                    if _deficit_levels < actual_levels:
                                        actual_levels = max(_deficit_levels, 1)  # at least 1 level

                                # Build new ladder — cap below ask to prevent taker fills
                                # Fail-closed: no ask data = skip re-anchor placement
                                _ra_ask_cents = 0
                                _ra_token = market.up_token if side == "up" else market.down_token
                                if self._ws_feed is not None:
                                    _ra_ask = self._ws_feed.get_best_ask(_ra_token)
                                    if _ra_ask is not None:
                                        _ra_ask_cents = int(round(_ra_ask * 100))
                                if _ra_ask_cents <= 0:
                                    logger.info("Trend re-anchor %s: no ask data — skipping placement", side)
                                    if side == "up":
                                        up_anchor_bid = new_bid
                                    else:
                                        down_anchor_bid = new_bid
                                    last_reanchor_ts[side] = now
                                    continue
                                _ra_price_cap = min(cfg.trend_max_buy_price_cents, _ra_ask_cents - 1) if cfg.trend_max_buy_price_cents > 0 else _ra_ask_cents - 1

                                logger.info(
                                    "Trend re-anchor %s: placing %d levels (base=%d)",
                                    side, actual_levels, cfg.trend_levels_per_side,
                                )
                                new_ladder = build_trend_ladder(
                                    new_bid, market.tick_size, cfg.shares_per_order,
                                    num_levels=actual_levels,
                                    step_cents=cfg.trend_step_cents,
                                    max_buy_price_cents=_ra_price_cap,
                                )

                                if new_ladder:
                                    ladder.clear()
                                    ladder.extend(new_ladder)

                                    # Fix #36a: pass check_avg_cost so each level is
                                    # individually gated by _is_cheap_enough.
                                    _placed = await order_engine.place_side_batch(
                                        new_ladder, token_id, side,
                                        expiration_ts, state,
                                        check_avg_cost=True,
                                        avg_cost_target=_avg_target,
                                    )
                                    _snapshot_new_orders()
                                    if _placed > 0:
                                        # Successful placement — allow replenish again
                                        _skip_replenish_sides.discard(side)
                                    else:
                                        # Fix #53c: All levels rejected by avg cost guard.
                                        # Cooldown 30s to avoid cancel→place(0)→cancel churn.
                                        _reanchor_avg_blocked_until[side] = now + 30.0
                                        logger.info(
                                            "Trend re-anchor %s: 0 orders placed (avg cost) — "
                                            "churn cooldown 30s",
                                            side,
                                        )

                                # Store REAL market bid as anchor (not catch anchor)
                                # so drift is measured from actual market, preventing
                                # catch orders from being immediately re-anchored.
                                if side == "up":
                                    up_anchor_bid = _real_market_bid
                                else:
                                    down_anchor_bid = _real_market_bid
                                last_reanchor_ts[side] = now

                                # Fix #60: Track whether catch ladder is active.
                                # When active, subsequent re-anchors skip cancel+replace
                                # since catch prices are identical (based on avg cost).
                                # Cleared on fills (position changes) or market reversion.
                                if new_bid != _real_market_bid:
                                    _catch_ladder_active[side] = True
                                else:
                                    _catch_ladder_active[side] = False

                        except Exception as e:
                            logger.warning("Trend re-anchor check failed: %s", e)

                # --- EV lock (optional) ---
                if cfg.trend_ev_lock_threshold > 0 and state.phase == MarketPhase.ACTIVE:
                    ev = state.position.expected_profit
                    if not _ev_locked and ev >= cfg.trend_ev_lock_threshold:
                        _ev_locked = True
                        logger.info(
                            "Trend EV LOCK: EV=$%.2f >= $%.2f — freezing position",
                            ev, cfg.trend_ev_lock_threshold,
                        )
                        try:
                            await self._client.cancel_and_verify(market.up_token)
                            await self._client.cancel_and_verify(market.down_token)
                            for oid, level in list(state.active_orders.items()):
                                order_engine.on_cancel_release(level, state)
                            state.active_orders.clear()
                            state.order_map.clear()
                        except Exception as e:
                            logger.warning("Trend EV LOCK cancel failed: %s", e)
                    elif _ev_locked and ev < cfg.trend_ev_lock_threshold * 0.5:
                        _ev_locked = False
                        logger.info(
                            "Trend EV UNLOCK: EV=$%.2f < $%.2f — resuming",
                            ev, cfg.trend_ev_lock_threshold * 0.5,
                        )
                        last_reanchor_ts["up"] = 0.0
                        last_reanchor_ts["down"] = 0.0

                # Snapshot any new orders
                _snapshot_new_orders()

                # Fast USDC drift update (every 0.5s) — keeps PROTECT decisions accurate
                if now - last_drift_update >= 0.5:
                    last_drift_update = now
                    try:
                        _usdc_now_fast = await self._client.get_balance()
                        if _usdc_start is not None:
                            _usdc_spent_fast = _usdc_start - _usdc_now_fast
                            _usdc_cost_drift = _usdc_spent_fast - self._tracker.position.total_cost
                            self._usdc_cost_drift = _usdc_cost_drift
                    except Exception:
                        pass  # heartbeat will catch up

                # Heartbeat
                if now - last_heartbeat >= cfg.heartbeat_interval:
                    pos = self._tracker.position
                    _liq_tag = ""
                    # Polymarket UI often reflects liquidation value of the excess side.
                    # Approximate this by crediting the excess shares at the current best bid.
                    try:
                        if pos.excess_shares >= 1.0 and self._ws_feed is not None:
                            _excess_side = pos.excess_side
                            _excess_token = market.up_token if _excess_side == "up" else market.down_token
                            _excess_bid = self._ws_feed.get_best_bid(_excess_token)
                            if _excess_bid is not None and _excess_bid > 0:
                                _excess_credit = pos.excess_shares * _excess_bid
                                _liq_worst = pos.risk_worst_case + _excess_credit
                                _liq_tag = f" liq=${_liq_worst:.1f}"
                    except Exception:
                        _liq_tag = ""
                    _lock_tag = ""
                    if _worst_locked:
                        _lock_tag = " FROZEN(worst)"
                    elif _ev_locked:
                        _lock_tag = " FROZEN(EV)"
                    elif _profit:
                        _lock_tag = " PROTECT"
                    _ratio = max(pos.up_shares, pos.down_shares) / max(min(pos.up_shares, pos.down_shares), 1)
                    _taker_fees = self._tracker.taker_fee_estimate
                    _rebates = self._tracker.rebate_estimate
                    _net_worst = pos.risk_worst_case + _rebates - _taker_fees
                    _mtm_tag = ""
                    try:
                        if self._ws_feed is not None and self._ws_feed.is_connected:
                            _up_bid = self._ws_feed.get_best_bid(market.up_token)
                            _down_bid = self._ws_feed.get_best_bid(market.down_token)
                            if _up_bid is not None and _down_bid is not None:
                                _mtm = (pos.up_shares * _up_bid) + (pos.down_shares * _down_bid) - pos.total_cost
                                _mtm_tag = f" mtm=${_mtm:.1f}"
                    except Exception:
                        _mtm_tag = ""
                    # Fix #71: Query USDC balance and compute real P&L.
                    _usdc_tag = ""
                    _real_worst_tag = ""
                    try:
                        _usdc_now = await self._client.get_balance()
                        _usdc_spent = _usdc_start - _usdc_now if _usdc_start is not None else 0.0
                        _usdc_cost_drift = _usdc_spent - pos.total_cost
                        self._usdc_cost_drift = _usdc_cost_drift
                        # Real P&L uses actual USDC spent instead of tracker cost.
                        _real_if_up = pos.up_shares - _usdc_spent
                        _real_if_down = pos.down_shares - _usdc_spent
                        _real_worst = min(_real_if_up, _real_if_down)
                        _real_worst_tag = f" REAL: worst=${_real_worst:.1f} up=${_real_if_up:.1f} down=${_real_if_down:.1f} spent=${_usdc_spent:.0f}"
                        _usdc_tag = f" drift=${_usdc_cost_drift:+.1f}"
                    except Exception:
                        pass
                    summary = (
                        f"[TREND] Up={pos.up_shares:.1f}({pos.up_vwap:.2f}) "
                        f"Down={pos.down_shares:.1f}({pos.down_vwap:.2f}) "
                        f"avg=${pos.avg_cost_per_share:.3f} "
                        f"EV=${pos.expected_profit:.1f} "
                        f"worst=${pos.risk_worst_case:.1f} net=${_net_worst:.1f}{_liq_tag}{_mtm_tag} "
                        f"if_up=${pos.risk_pnl_if_up:.1f} if_down=${pos.risk_pnl_if_down:.1f} "
                        f"cost=${pos.total_cost:.1f}/{cfg.session_capital_limit:.0f} "
                        f"trades={state.total_trades} "
                        f"taker_fees=${_taker_fees:.2f} rebates=${_rebates:.2f} "
                        f"bal={_ratio:.1f}x{_lock_tag}{_usdc_tag}{_real_worst_tag}"
                    )
                    logger.info(summary)
                    await self._alerts.heartbeat_status(state.phase.value, summary)
                    last_heartbeat = now
                    last_mini_hb = now

                # Mini-heartbeat: bid/ask snapshot every 5s between full heartbeats
                # Skip when loop is busy (fills this cycle = noise)
                elif now - last_mini_hb >= 5.0 and not any(side_fills.values()):
                    last_mini_hb = now
                    try:
                        _tick_parts = []
                        if self._ws_feed is not None and self._ws_feed.is_connected:
                            _ub = self._ws_feed.get_best_bid(market.up_token)
                            _ua = self._ws_feed.get_best_ask(market.up_token)
                            _db = self._ws_feed.get_best_bid(market.down_token)
                            _da = self._ws_feed.get_best_ask(market.down_token)
                            _ub_c = f"{_ub*100:.0f}" if _ub is not None else "?"
                            _ua_c = f"{_ua*100:.0f}" if _ua is not None else "?"
                            _db_c = f"{_db*100:.0f}" if _db is not None else "?"
                            _da_c = f"{_da*100:.0f}" if _da is not None else "?"
                            _tick_parts.append(f"up={_ub_c}/{_ua_c}c")
                            _tick_parts.append(f"down={_db_c}/{_da_c}c")
                            if _ua is not None and _da is not None:
                                _tick_parts.append(f"spread={_ua + _da:.2f}")
                        _n_orders = len(state.order_map)
                        _tick_parts.append(f"orders={_n_orders}")
                        _flags = []
                        if _profit:
                            _flags.append("PROTECT")
                        if _taker_active:
                            _flags.append("TAKER")
                        if _worst_locked or _ev_locked:
                            _flags.append("FROZEN")
                        if _flags:
                            _tick_parts.append(" ".join(_flags))
                        logger.info("[TICK] %s", " ".join(_tick_parts))
                    except Exception as e:
                        logger.debug("Mini-heartbeat error: %s", e)

                # --- Periodic position reconciliation ---
                # Check real on-chain balance vs tracker.
                # Catches phantom fills, cancel-race drift, missed taker fills.
                # Fix #56: Skip when fills arrived recently — endpoint lags
                # behind on-chain settlement and may return stale/zero values.
                _last_any_fill = max(
                    last_side_fill_ts.get("up", 0.0),
                    last_side_fill_ts.get("down", 0.0),
                )
                _reconcile_settle_s = 30.0  # wait 30s after last fill
                if (
                    now - last_reconcile_ts >= cfg.position_reconcile_interval_s
                    and now - _last_any_fill >= _reconcile_settle_s
                ):
                    last_reconcile_ts = now
                    try:
                        old_pos = self._tracker.position
                        changed = await self._reconcile_live_position(
                            market, state, reason="periodic", force=False,
                        )
                        if changed:
                            new_pos = self._tracker.position
                            logger.warning(
                                "Periodic reconcile corrected position: "
                                "UP %.1f→%.1f DOWN %.1f→%.1f "
                                "cost $%.1f→$%.1f",
                                old_pos.up_shares, new_pos.up_shares,
                                old_pos.down_shares, new_pos.down_shares,
                                old_pos.total_cost, new_pos.total_cost,
                            )
                    except Exception as e:
                        logger.debug("Periodic reconcile failed: %s", e)

                # Save state
                state.session_worst_case_pnl = self._tracker.position.risk_worst_case
                self._state_manager.save_state(state)

                await asyncio.sleep(max(0.2, cfg.monitor_loop_interval_s))

        except Exception as exc:
            logger.exception("Trend session crashed: %s", exc)
        finally:
            stop_event.set()
            try:
                await asyncio.wait_for(monitor_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                monitor_task.cancel()

            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)

            _snapshot_new_orders()
            self._flush_fill_queue(fill_queue)
            await self._reconcile_via_orders(all_placed_orders, state)

            pos = self._tracker.position
            _usdc_end = await self._snapshot_usdc_balance()
            _usdc_delta = None
            if _usdc_start is not None and _usdc_end is not None:
                _usdc_delta = _usdc_start - _usdc_end
                _drift = _usdc_delta - pos.total_cost
                logger.info(
                    "USDC cross-check: start=$%.2f end=$%.2f spent=$%.2f "
                    "tracked_cost=$%.2f drift=$%.2f",
                    _usdc_start, _usdc_end, _usdc_delta,
                    pos.total_cost, _drift,
                )
                # Fix #71: Show real P&L using USDC ground truth.
                try:
                    _real_if_up = pos.up_shares - _usdc_delta
                    _real_if_down = pos.down_shares - _usdc_delta
                    _real_worst = min(_real_if_up, _real_if_down)
                    logger.info(
                        "REAL P&L: worst=$%.2f if_up=$%.2f if_down=$%.2f "
                        "(up=%.1f down=%.1f shares, $%.2f spent)",
                        _real_worst, _real_if_up, _real_if_down,
                        pos.up_shares, pos.down_shares, _usdc_delta,
                    )
                except (TypeError, ValueError):
                    pass
            result = {
                "session_id": session_id,
                "condition_id": market.condition_id,
                "total_trades": state.total_trades,
                "up_shares": pos.up_shares,
                "down_shares": pos.down_shares,
                "combined_vwap": pos.combined_vwap,
                "avg_cost_per_share": pos.avg_cost_per_share,
                "expected_profit": pos.expected_profit,
                "worst_case_pnl": pos.risk_worst_case,
                "pnl_if_up": pos.risk_pnl_if_up,
                "pnl_if_down": pos.risk_pnl_if_down,
                "projected_pnl": self._tracker.report_worst_case,
                "rebate_estimate": self._tracker.rebate_estimate,
                "taker_fees_estimate": self._tracker.taker_fee_estimate,
                "net_profit_estimate": self._tracker.net_profit_estimate,
                "taker_notional_used": state.taker_notional_used,
                "real_worst_case_pnl": (min(pos.up_shares - _usdc_delta, pos.down_shares - _usdc_delta) if isinstance(_usdc_delta, (int, float)) else None),
                "real_pnl_if_up": ((pos.up_shares - _usdc_delta) if isinstance(_usdc_delta, (int, float)) else None),
                "real_pnl_if_down": ((pos.down_shares - _usdc_delta) if isinstance(_usdc_delta, (int, float)) else None),
                "usdc_start": _usdc_start,
                "usdc_end": _usdc_end,
                "usdc_spent": _usdc_delta,
            }
            self._state_manager.save_session_result(result)
            await self._alerts.session_complete(
                fills=state.total_trades,
                combined_vwap=pos.combined_vwap,
                risk_pnl=pos.risk_worst_case,
                report_pnl=self._tracker.report_worst_case,
                up_shares=pos.up_shares,
                down_shares=pos.down_shares,
            )
            return result

    # ------------------------------------------------------------------
    # Hedge-sell strategy
    # ------------------------------------------------------------------

    @staticmethod
    def _best_bid(book: dict) -> float:
        """Extract best bid price from order book. Returns 0.0 if no bids."""
        bids = book.get("bids", [])
        if not bids:
            return 0.0
        return max(float(b.get("price", 0)) for b in bids)

    @staticmethod
    def _best_ask(book: dict) -> float:
        """Extract best ask price from order book. Returns 1.0 if no asks."""
        asks = book.get("asks", [])
        if not asks:
            return 1.0
        return min(float(a.get("price", 1.0)) for a in asks)

    def _calc_mtm(self, pos: PositionState, up_bid: float, down_bid: float) -> float:
        """Mark-to-market profit: what we'd get selling everything at best bids minus cost."""
        return pos.up_shares * up_bid + pos.down_shares * down_bid - pos.total_cost

    async def _run_hedge_sell_session(
        self, market: MarketInfo, tier: EdgeTier
    ) -> Optional[dict]:
        """Hedge-sell strategy: buy hedged via active ladder, sell when mtm >= target.

        Phase 1 (BUYING): Active-style ladder with rebalancing to build hedged position.
        Phase 2 (SELLING): Place limit sell orders at best_bid+1c, re-place on drift,
                           panic taker if mtm drops below min_profit.
        """
        session_id = uuid.uuid4().hex[:12]
        state = BotState(
            phase=MarketPhase.ACTIVE,
            market=market,
            session_id=session_id,
        )

        # Subscribe WS feed to this market's tokens
        if self._ws_feed is not None:
            await self._ws_feed.start([market.up_token, market.down_token])

        await self._reconcile_live_position(
            market, state, reason="hedge_sell_start", force=True,
        )

        # Get orderbooks
        up_book = await self._client.get_order_book(market.up_token)
        down_book = await self._client.get_order_book(market.down_token)
        up_price = _get_effective_price(up_book)
        down_price = _get_effective_price(down_book)
        combined = up_price + down_price

        logger.info(
            "[HEDGE_SELL] Session entry: up=%.3f down=%.3f combined=%.3f",
            up_price, down_price, combined,
        )

        # Build ladders — cap at 6 levels for hedge_sell (tighter than active's 15).
        # Fewer levels = less one-sided exposure before re-anchor can correct.
        max_hs_levels = 6
        half_capital = self._config.session_capital_limit / 2
        up_cost_per = self._config.shares_per_order * up_price
        down_cost_per = self._config.shares_per_order * down_price
        up_levels = min(max_hs_levels, max(2, int(half_capital / up_cost_per))) if up_cost_per > 0 else 2
        down_levels = min(max_hs_levels, max(2, int(half_capital / down_cost_per))) if down_cost_per > 0 else 2

        up_ladder = build_ladder(
            up_price, market.tick_size, self._config.shares_per_order, up_levels,
            other_side_best_ask=down_price, max_combined=self._config.max_combined_vwap,
        )
        down_ladder = build_ladder(
            down_price, market.tick_size, self._config.shares_per_order, down_levels,
            other_side_best_ask=up_price, max_combined=self._config.max_combined_vwap,
        )

        logger.info(
            "[HEDGE_SELL] Ladder: up=%d levels down=%d levels",
            len(up_ladder), len(down_ladder),
        )

        # Engines
        fill_queue: asyncio.Queue = asyncio.Queue()
        fill_monitor = FillMonitor(
            self._client, fill_queue, market.condition_id,
            market.up_token, market.down_token,
        )
        order_engine = OrderEngine(self._client, self._config, fill_monitor)

        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(
            fill_monitor.run(
                interval=max(0.2, self._config.fill_poll_interval_s),
                stop_event=stop_event,
            )
        )

        # Ensure CTF allowances are set (required for SELL orders later)
        allowance_ok = await self._client.ensure_allowances(
            token_ids=[market.up_token, market.down_token]
        )
        if not allowance_ok:
            logger.error("[HEDGE_SELL] Failed to set CTF allowances — SELL orders will fail")

        await self._alerts.market_entry(market, tier, combined)
        expiration_ts = int(market.close_ts)

        # Place initial ladder
        # Fix #42: cancel orphaned orders if placement times out
        try:
            await order_engine.place_ladder(
                up_ladder, down_ladder,
                market.up_token, market.down_token,
                expiration_ts, state,
            )
        except Exception as e:
            logger.error("[HEDGE_SELL] Initial placement failed — cancelling orphaned orders: %s", e)
            await self._cancel_orphaned_orders(market)
            stop_event.set()
            monitor_task.cancel()
            return None

        # State tracking
        last_heartbeat = time.time()
        last_reanchor_check = time.time()
        last_rebalance_cancel = 0.0
        last_soft_rebalance_lagging_side = ""
        last_rebalance_mode = ""
        last_side_fill_ts: dict[str, float] = {"up": 0.0, "down": 0.0}
        last_reanchor_action_ts: dict[str, float] = {"up": 0.0, "down": 0.0}
        up_anchor = up_price
        down_anchor = down_price
        selling_mode = False
        selling_entered_ts = 0.0
        # Track sell order IDs per side
        sell_orders: dict[str, str] = {}  # "up" / "down" -> order_id
        sell_order_prices: dict[str, int] = {}  # "up" / "down" -> price_cents
        last_mtm_check = 0.0
        all_placed_orders: dict[str, dict] = {}

        def _snapshot_new_orders():
            for oid, meta in state.order_map.items():
                if oid not in all_placed_orders:
                    all_placed_orders[oid] = dict(meta)

        def _update_fill_tracking(fill):
            level = state.active_orders.get(fill.order_id)
            if not level:
                return
            level.filled_shares += fill.filled_shares
            if level.filled_shares + 1e-9 >= level.target_shares:
                level.last_fill_at = time.time()
                level.order_id = None
                state.active_orders.pop(fill.order_id, None)
                state.order_map.pop(fill.order_id, None)

        def _drain_fills():
            """Drain fill queue and update tracker."""
            drain_fills(
                fill_queue, self._tracker, state, order_engine,
                _update_fill_tracking,
                last_side_fill_ts=last_side_fill_ts,
            )

        async def _sell_side_at_limit(
            side: str, token_id: str, shares: float, target_price: float
        ) -> str:
            """Place a SELL limit order for a side. Retries on balance lag. Returns order_id or ''."""
            if shares < 1.0:
                return ""
            price = clamp_price(target_price, 0.01)
            price = round_to_tick(price, 0.01)
            for attempt in range(3):
                try:
                    oid = await self._client.place_taker_order(
                        token_id=token_id,
                        price=price,
                        size=round(shares, 2),
                        side=side,
                        expiration_ts=expiration_ts,
                        sell=True,
                    )
                    logger.info(
                        "[HEDGE_SELL] SELL limit %s %.1f @ %.2f -> %s",
                        side, shares, price, oid,
                    )
                    return oid
                except Exception as e:
                    if "not enough balance" in str(e).lower() and attempt < 2:
                        logger.warning(
                            "[HEDGE_SELL] SELL %s balance not settled, retry %d/3 in 5s",
                            side, attempt + 1,
                        )
                        await asyncio.sleep(5.0)
                        continue
                    logger.error("[HEDGE_SELL] SELL limit failed %s: %s", side, e)
                    return ""
            return ""

        async def _taker_sell_side(side: str, token_id: str, shares: float) -> str:
            """Emergency taker sell at best bid. Retries on balance lag."""
            if shares < 1.0:
                return ""
            for attempt in range(3):
                book = await self._client.get_order_book(token_id)
                bid = self._best_bid(book)
                if bid <= 0:
                    logger.warning("[HEDGE_SELL] No bids for %s, can't taker sell", side)
                    return ""
                try:
                    oid = await self._client.place_taker_order(
                        token_id=token_id,
                        price=bid,
                        size=round(shares, 2),
                        side=side,
                        expiration_ts=expiration_ts,
                        sell=True,
                    )
                    logger.info(
                        "[HEDGE_SELL] TAKER SELL %s %.1f @ %.2f -> %s",
                        side, shares, bid, oid,
                    )
                    return oid
                except Exception as e:
                    if "not enough balance" in str(e).lower() and attempt < 2:
                        logger.warning(
                            "[HEDGE_SELL] TAKER SELL %s balance not settled, retry %d/3 in 5s",
                            side, attempt + 1,
                        )
                        await asyncio.sleep(5.0)
                        continue
                    logger.error("[HEDGE_SELL] Taker sell failed %s: %s", side, e)
                    return ""
            return ""

        async def _cancel_sell_orders():
            """Cancel any outstanding sell orders."""
            for side, oid in list(sell_orders.items()):
                if oid:
                    try:
                        await self._client.cancel_order(oid)
                    except Exception:
                        pass
            sell_orders.clear()
            sell_order_prices.clear()

        async def _enter_selling_mode():
            """Cancel all buy orders and start selling."""
            nonlocal selling_mode, selling_entered_ts
            selling_mode = True
            selling_entered_ts = time.time()
            logger.info("[HEDGE_SELL] === ENTERING SELLING MODE ===")

            # Cancel all buy orders
            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)
            for oid, level in list(state.active_orders.items()):
                order_engine.on_cancel_release(level, state)
            state.active_orders.clear()
            state.order_map.clear()

            # Drain any last fills
            _drain_fills()

            # Wait for Polymarket balance index to settle.
            # The exchange rejects SELL orders if token balance isn't indexed yet
            # (~5s delay after fills). Poll until balance appears or timeout.
            pos = self._tracker.position
            if pos.up_shares >= 1.0 or pos.down_shares >= 1.0:
                logger.info("[HEDGE_SELL] Waiting for balance settlement...")
                for attempt in range(12):  # up to 60s
                    await asyncio.sleep(5.0)
                    settled = True
                    for side, token, expected in [
                        ("up", market.up_token, pos.up_shares),
                        ("down", market.down_token, pos.down_shares),
                    ]:
                        if expected < 1.0:
                            continue
                        actual = await self._client.get_position(token)
                        if actual != actual:  # NaN = endpoint unavailable
                            # Fallback: just wait fixed time
                            if attempt >= 2:
                                break
                            settled = False
                            continue
                        if actual < expected * 0.5:
                            settled = False
                            logger.debug(
                                "[HEDGE_SELL] %s balance: %.1f / %.1f (waiting)",
                                side, actual, expected,
                            )
                    if settled:
                        logger.info(
                            "[HEDGE_SELL] Balance settled (attempt %d)", attempt + 1
                        )
                        break
                else:
                    logger.warning("[HEDGE_SELL] Balance settlement timeout — proceeding anyway")

        async def _place_or_update_sells():
            """Place/re-place SELL limit orders at best_bid + 1c for both sides."""
            pos = self._tracker.position
            for side, token, shares in [
                ("up", market.up_token, pos.up_shares),
                ("down", market.down_token, pos.down_shares),
            ]:
                if shares < 1.0:
                    continue
                book = await self._client.get_order_book(token)
                bid = self._best_bid(book)
                if bid <= 0:
                    continue
                target_cents = int(round(bid * 100)) + 1
                target_price = target_cents / 100.0

                existing_oid = sell_orders.get(side, "")
                existing_price = sell_order_prices.get(side, 0)

                # Check if existing order already filled
                if existing_oid:
                    try:
                        order_info = await self._client.get_order(existing_oid)
                        matched = float(order_info.get("size_matched", 0) or 0)
                        orig_size = float(order_info.get("original_size", 0) or 0)
                        status = str(order_info.get("status", "")).upper()
                        if status == "MATCHED" or (orig_size > 0 and matched + 0.5 >= orig_size):
                            logger.info("[HEDGE_SELL] SELL %s fully filled (%s)", side, existing_oid[:12])
                            sell_orders.pop(side, None)
                            sell_order_prices.pop(side, None)
                            continue
                        # Partial fill: update remaining
                        remaining = shares  # use tracker's shares as truth
                    except Exception:
                        remaining = shares
                else:
                    remaining = shares

                # Re-place if price drifted
                drift = abs(target_cents - existing_price)
                if existing_oid and drift < self._config.hedge_sell_redrift_cents:
                    continue  # price hasn't moved enough

                # Cancel old and place new
                if existing_oid:
                    try:
                        await self._client.cancel_order(existing_oid)
                    except Exception:
                        pass

                oid = await _sell_side_at_limit(side, token, remaining, target_price)
                if oid:
                    sell_orders[side] = oid
                    sell_order_prices[side] = target_cents

        def _all_sold() -> bool:
            """Check if all shares have been sold."""
            pos = self._tracker.position
            return pos.up_shares < 1.0 and pos.down_shares < 1.0

        try:
            while not self._shutdown:
                phase = self._scheduler.get_current_phase(market)
                if phase in (MarketPhase.CLOSED, MarketPhase.RESOLVED):
                    break

                _snapshot_new_orders()
                _drain_fills()

                pos = self._tracker.position

                # === SELLING MODE ===
                if selling_mode:
                    now = time.time()

                    if _all_sold():
                        logger.info("[HEDGE_SELL] All shares sold! Session complete.")
                        break

                    # Check mtm
                    if now - last_mtm_check >= self._config.hedge_sell_check_interval_s:
                        last_mtm_check = now
                        up_book_s = await self._client.get_order_book(market.up_token)
                        down_book_s = await self._client.get_order_book(market.down_token)
                        up_bid = self._best_bid(up_book_s)
                        down_bid = self._best_bid(down_book_s)
                        mtm = self._calc_mtm(pos, up_bid, down_bid)

                        # Panic: mtm dropped below floor
                        if mtm < self._config.hedge_sell_min_profit and pos.total_shares >= 2.0:
                            logger.warning(
                                "[HEDGE_SELL] MTM $%.2f < min $%.2f — TAKER DUMP",
                                mtm, self._config.hedge_sell_min_profit,
                            )
                            await _cancel_sell_orders()
                            if pos.up_shares >= 1.0:
                                await _taker_sell_side("up", market.up_token, pos.up_shares)
                            if pos.down_shares >= 1.0:
                                await _taker_sell_side("down", market.down_token, pos.down_shares)
                            await asyncio.sleep(2.0)
                            _drain_fills()
                            break

                        # Timeout: go taker
                        if now - selling_entered_ts >= self._config.hedge_sell_sell_timeout_s:
                            logger.warning(
                                "[HEDGE_SELL] Sell timeout (%.0fs) — TAKER DUMP (mtm=$%.2f)",
                                now - selling_entered_ts, mtm,
                            )
                            await _cancel_sell_orders()
                            pos = self._tracker.position
                            if pos.up_shares >= 1.0:
                                await _taker_sell_side("up", market.up_token, pos.up_shares)
                            if pos.down_shares >= 1.0:
                                await _taker_sell_side("down", market.down_token, pos.down_shares)
                            await asyncio.sleep(2.0)
                            _drain_fills()
                            break

                        # Place/update sell limit orders
                        await _place_or_update_sells()

                    # Heartbeat in selling mode
                    if now - last_heartbeat >= 5.0:
                        up_book_h = await self._client.get_order_book(market.up_token)
                        down_book_h = await self._client.get_order_book(market.down_token)
                        up_bid_h = self._best_bid(up_book_h)
                        down_bid_h = self._best_bid(down_book_h)
                        mtm_h = self._calc_mtm(pos, up_bid_h, down_bid_h)
                        logger.info(
                            "[HEDGE_SELL] SELLING: up=%.0f down=%.0f mtm=$%.2f "
                            "sell_orders=%d elapsed=%.0fs",
                            pos.up_shares, pos.down_shares, mtm_h,
                            len(sell_orders), now - selling_entered_ts,
                        )
                        await self._alerts.heartbeat_status(
                            "selling", f"SELLING mtm=${mtm_h:.2f} up={pos.up_shares:.0f} down={pos.down_shares:.0f}"
                        )
                        last_heartbeat = now

                    await asyncio.sleep(0.5)
                    continue

                # === BUYING MODE ===

                # WINDING_DOWN: if not sold yet, panic sell
                if phase == MarketPhase.WINDING_DOWN and state.phase != MarketPhase.WINDING_DOWN:
                    state.phase = MarketPhase.WINDING_DOWN
                    logger.info("[HEDGE_SELL] WINDING_DOWN — selling at market")
                    await _enter_selling_mode()
                    # Force taker sell immediately
                    pos = self._tracker.position
                    if pos.up_shares >= 1.0:
                        await _taker_sell_side("up", market.up_token, pos.up_shares)
                    if pos.down_shares >= 1.0:
                        await _taker_sell_side("down", market.down_token, pos.down_shares)
                    await asyncio.sleep(2.0)
                    _drain_fills()
                    break

                # MTM check — should we start selling?
                now = time.time()
                if now - last_mtm_check >= self._config.hedge_sell_check_interval_s and pos.total_shares >= 2.0:
                    last_mtm_check = now
                    up_book_m = await self._client.get_order_book(market.up_token)
                    down_book_m = await self._client.get_order_book(market.down_token)
                    up_bid_m = self._best_bid(up_book_m)
                    down_bid_m = self._best_bid(down_book_m)
                    mtm = self._calc_mtm(pos, up_bid_m, down_bid_m)

                    if mtm >= self._config.hedge_sell_profit_target:
                        logger.info(
                            "[HEDGE_SELL] MTM $%.2f >= target $%.2f — START SELLING! "
                            "up=%.0f(bid=%.2f) down=%.0f(bid=%.2f) cost=$%.2f",
                            mtm, self._config.hedge_sell_profit_target,
                            pos.up_shares, up_bid_m, pos.down_shares, down_bid_m,
                            pos.total_cost,
                        )
                        await _enter_selling_mode()
                        continue

                # Risk checks (simplified — no kill escalation, just basic safety)
                verdict = self._risk_engine.check_position(self._tracker.position, state)
                if verdict.action in (RiskAction.KILL_SESSION, RiskAction.KILL_DAY):
                    logger.warning("[HEDGE_SELL] Risk kill: %s", verdict.reason)
                    if verdict.action == RiskAction.KILL_DAY:
                        self._shutdown = True
                    break

                # Replenish / rebalance / re-anchor if ACTIVE
                if state.phase == MarketPhase.ACTIVE and verdict.action in (
                    RiskAction.NONE, RiskAction.STOP_ADDING
                ):
                    pos = self._tracker.position
                    soft_imb = max(0.0, self._config.rebalance_soft_imbalance)
                    imbalance = pos.share_imbalance
                    lagging_side = (
                        "up" if pos.up_shares < pos.down_shares
                        else "down" if pos.down_shares < pos.up_shares
                        else ""
                    )
                    min_total = max(0.0, self._config.rebalance_min_total_shares - 0.5)

                    # Max imbalance gate: when imbalance > 25%, cancel heavy side
                    # and freeze ALL buying until rebalanced.
                    hs_max_imbalance = 0.25
                    buying_frozen = False
                    if (
                        lagging_side
                        and imbalance >= hs_max_imbalance
                        and pos.total_shares >= min_total
                    ):
                        buying_frozen = True
                        heavy_side = "down" if lagging_side == "up" else "up"
                        heavy_token = market.up_token if heavy_side == "up" else market.down_token
                        heavy_open = sum(
                            1 for m in state.order_map.values()
                            if m.get("side") == heavy_side
                        )
                        if (
                            heavy_open > 0
                            and now - last_rebalance_cancel >= self._config.rebalance_cancel_cooldown_s
                        ):
                            logger.info(
                                "[HEDGE_SELL] Imbalance %.0f%% > 25%% — cancelling %s (%d open)",
                                imbalance * 100, heavy_side, heavy_open,
                            )
                            await self._client.cancel_market_orders(heavy_token)
                            side_oids = [
                                oid for oid, m in state.order_map.items()
                                if m.get("side") == heavy_side and not m.get("cancelled")
                            ]
                            for oid in side_oids:
                                lvl = state.active_orders.pop(oid, None)
                                if lvl:
                                    order_engine.on_cancel_release(lvl, state)
                            if side_oids:
                                mark_orders_cancelled(state, side_oids)
                            last_rebalance_cancel = now

                    if not buying_frozen:
                        replenish_up = True
                        replenish_down = True

                        # Soft imbalance: replenish only lagging side
                        if (
                            lagging_side
                            and imbalance >= soft_imb
                            and pos.total_shares >= min_total
                        ):
                            replenish_up = lagging_side == "up"
                            replenish_down = lagging_side == "down"
                            heavy_side = "down" if lagging_side == "up" else "up"
                            heavy_open = sum(
                                1 for m in state.order_map.values()
                                if m.get("side") == heavy_side
                            )

                            # Hard imbalance: cancel heavy side
                            hard_imb = max(soft_imb, self._config.rebalance_hard_imbalance)
                            if (
                                imbalance >= hard_imb
                                and heavy_open > 0
                                and now - last_rebalance_cancel >= self._config.rebalance_cancel_cooldown_s
                            ):
                                heavy_token = market.up_token if heavy_side == "up" else market.down_token
                                await self._client.cancel_market_orders(heavy_token)
                                side_oids = [
                                    oid for oid, m in state.order_map.items()
                                    if m.get("side") == heavy_side and not m.get("cancelled")
                                ]
                                for oid in side_oids:
                                    lvl = state.active_orders.pop(oid, None)
                                    if lvl:
                                        order_engine.on_cancel_release(lvl, state)
                                if side_oids:
                                    mark_orders_cancelled(state, side_oids)
                                last_rebalance_cancel = now

                        if replenish_up:
                            for level in up_ladder:
                                await order_engine.replenish(
                                    level, "up", market.up_token, expiration_ts, state
                                )
                        if replenish_down:
                            for level in down_ladder:
                                await order_engine.replenish(
                                    level, "down", market.down_token, expiration_ts, state
                                )

                    # Re-anchor: when price drifts, cancel stale orders and rebuild ladder.
                    # Allowed even when frozen — re-anchor only helps lagging side when imbalanced.
                    if now - last_reanchor_check >= self._config.reanchor_check_interval_s:
                        last_reanchor_check = now
                        try:
                            pos_ra = self._tracker.position
                            time_left_s = market.close_ts - self._scheduler.adjusted_now()

                            # Skip if too close to expiry
                            if time_left_s < self._config.reanchor_min_time_left_s:
                                pass  # fall through to except
                            else:
                                ra_up_book = await self._client.get_order_book(market.up_token)
                                ra_down_book = await self._client.get_order_book(market.down_token)
                                new_up = _get_effective_price(ra_up_book)
                                new_down = _get_effective_price(ra_down_book)
                                imbal_ra = pos_ra.share_imbalance
                                lagging = (
                                    "up" if pos_ra.up_shares < pos_ra.down_shares
                                    else "down" if pos_ra.down_shares < pos_ra.up_shares
                                    else ""
                                )

                                for side, old_anchor, new_price, token, old_ladder in [
                                    ("up", up_anchor, new_up, market.up_token, up_ladder),
                                    ("down", down_anchor, new_down, market.down_token, down_ladder),
                                ]:
                                    drift = abs(new_price - old_anchor)

                                    # Force re-anchor when side has 0 orders but position exists (fix #25)
                                    side_order_count = sum(
                                        1 for m in state.order_map.values() if m.get("side") == side
                                    )
                                    side_shares = pos_ra.up_shares if side == "up" else pos_ra.down_shares
                                    force_reanchor = side_order_count == 0 and side_shares > 0

                                    if not force_reanchor and drift < self._config.reanchor_drift_threshold:
                                        continue

                                    # Per-side cooldown
                                    if now - last_reanchor_action_ts[side] < self._config.reanchor_min_action_cooldown_s:
                                        continue

                                    # Recent fill grace period
                                    if now - last_side_fill_ts.get(side, 0.0) < self._config.reanchor_side_fill_grace_s:
                                        continue

                                    # Lagging-only when imbalanced (fix #28: force bypasses)
                                    if (
                                        imbal_ra >= self._config.rebalance_soft_imbalance
                                        and lagging
                                        and side != lagging
                                        and not force_reanchor
                                    ):
                                        continue

                                    # Worse-price block: don't chase higher buy prices (fix #23/#27)
                                    if new_price > old_anchor and not force_reanchor:
                                        worse_dev = new_price - old_anchor
                                        emergency_ok = (
                                            lagging == side
                                            and imbal_ra >= self._config.reanchor_worse_price_imbalance_emergency
                                            and pos_ra.combined_vwap < self._config.max_combined_vwap
                                            and worse_dev <= 0.05
                                        )
                                        if not emergency_ok:
                                            logger.info(
                                                "[HEDGE_SELL] Re-anchor %s blocked: worse price %.3f->%.3f",
                                                side, old_anchor, new_price,
                                            )
                                            continue

                                    # Build candidate ladder
                                    other_price = new_down if side == "up" else new_up
                                    rebuilt = build_ladder(
                                        new_price, market.tick_size,
                                        self._config.shares_per_order,
                                        len(old_ladder),
                                        other_side_best_ask=other_price,
                                        max_combined=self._config.max_combined_vwap,
                                    )
                                    if not rebuilt:
                                        continue

                                    # VWAP headroom check: block if full ladder pushes cVWAP past max
                                    if pos_ra.total_shares >= self._config.rebalance_min_total_shares - 0.5:
                                        total_new_cost = sum(
                                            (lv.price_cents / 100.0) * lv.target_shares for lv in rebuilt
                                        )
                                        total_new_shares = sum(lv.target_shares for lv in rebuilt)
                                        if side == "up":
                                            new_side_vwap = (
                                                (pos_ra.up_cost + total_new_cost)
                                                / (pos_ra.up_shares + total_new_shares)
                                            ) if (pos_ra.up_shares + total_new_shares) > 0 else 0.0
                                            other_vwap = pos_ra.down_vwap if pos_ra.down_vwap > 0 else new_side_vwap
                                        else:
                                            new_side_vwap = (
                                                (pos_ra.down_cost + total_new_cost)
                                                / (pos_ra.down_shares + total_new_shares)
                                            ) if (pos_ra.down_shares + total_new_shares) > 0 else 0.0
                                            other_vwap = pos_ra.up_vwap if pos_ra.up_vwap > 0 else new_side_vwap
                                        if new_side_vwap + other_vwap > self._config.max_combined_vwap:
                                            logger.info(
                                                "[HEDGE_SELL] Re-anchor %s blocked: VWAP headroom "
                                                "(projected=%.3f, max=%.3f)",
                                                side, new_side_vwap + other_vwap,
                                                self._config.max_combined_vwap,
                                            )
                                            continue

                                    logger.info(
                                        "[HEDGE_SELL] Re-anchor %s: %.3f -> %.3f (drift=%.3f, %d lvls, force=%s)",
                                        side, old_anchor, new_price, drift, len(rebuilt), force_reanchor,
                                    )

                                    # Cancel stale orders on this side
                                    await self._client.cancel_market_orders(token)
                                    side_oids = [
                                        oid for oid, m in state.order_map.items()
                                        if m.get("side") == side and not m.get("cancelled")
                                    ]
                                    for oid in side_oids:
                                        lvl = state.active_orders.pop(oid, None)
                                        if lvl:
                                            order_engine.on_cancel_release(lvl, state)
                                    if side_oids:
                                        mark_orders_cancelled(state, side_oids)

                                    # Update anchor and ladder
                                    if side == "up":
                                        up_anchor = new_price
                                        up_ladder = rebuilt
                                    else:
                                        down_anchor = new_price
                                        down_ladder = rebuilt
                                    last_reanchor_action_ts[side] = now

                                    # Place new orders
                                    to_place = [
                                        lv for lv in rebuilt
                                        if lv.order_id is None and lv.filled_shares < lv.target_shares
                                    ]
                                    if to_place:
                                        await order_engine.place_side(
                                            to_place, token, side, expiration_ts, state,
                                        )
                        except Exception as e:
                            logger.warning("[HEDGE_SELL] Re-anchor error: %s", e)

                # Taker rebalance — buy lagging side when break-even exceeded.
                # Blocked by imbalance gate (no point buying more when already lopsided).
                if (
                    self._config.taker_rebalance_enabled
                    and state.phase == MarketPhase.ACTIVE
                    and verdict.action in (RiskAction.NONE, RiskAction.STOP_ADDING)
                    and self._tracker.position.share_imbalance < 0.25
                ):
                    pos = self._tracker.position
                    if (
                        pos.hedged_shares > 0
                        and pos.excess_shares >= self._config.shares_per_order
                        and pos.total_shares >= self._config.taker_rebalance_min_total_shares
                        and pos.combined_vwap < 1.0
                    ):
                        excess_vwap = pos.down_vwap if pos.excess_side == "down" else pos.up_vwap
                        max_excess = pos.hedged_profit / excess_vwap if excess_vwap > 0 else 0.0
                        if pos.excess_shares > max_excess:
                            await self._try_taker_flatten(market, state, fill_monitor)

                # Heartbeat
                if now - last_heartbeat >= self._config.heartbeat_interval:
                    pos = self._tracker.position
                    # Quick mtm estimate
                    try:
                        hb_up = await self._client.get_order_book(market.up_token)
                        hb_down = await self._client.get_order_book(market.down_token)
                        hb_mtm = self._calc_mtm(
                            pos, self._best_bid(hb_up), self._best_bid(hb_down)
                        )
                    except Exception:
                        hb_mtm = 0.0
                    summary = (
                        f"[HEDGE_SELL] Up={pos.up_shares:.1f}({pos.up_vwap:.2f}) "
                        f"Down={pos.down_shares:.1f}({pos.down_vwap:.2f}) "
                        f"mtm=${hb_mtm:.1f} target=${self._config.hedge_sell_profit_target:.0f} "
                        f"imbal={pos.share_imbalance*100:.0f}% "
                        f"cost=${pos.total_cost:.1f}/{self._config.session_capital_limit:.0f}"
                    )
                    logger.info(summary)
                    await self._alerts.heartbeat_status(state.phase.value, summary)
                    last_heartbeat = now

                # Kill command
                killed = await self._alerts.check_kill_command()
                if killed:
                    logger.warning("[HEDGE_SELL] Kill command — selling at market")
                    await _enter_selling_mode()
                    pos = self._tracker.position
                    if pos.up_shares >= 1.0:
                        await _taker_sell_side("up", market.up_token, pos.up_shares)
                    if pos.down_shares >= 1.0:
                        await _taker_sell_side("down", market.down_token, pos.down_shares)
                    await asyncio.sleep(2.0)
                    _drain_fills()
                    self._shutdown = True
                    break

                # Save state
                state.session_worst_case_pnl = pos.risk_worst_case
                self._state_manager.save_state(state)

                await asyncio.sleep(max(0.2, self._config.monitor_loop_interval_s))

        finally:
            stop_event.set()
            try:
                await asyncio.wait_for(monitor_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                monitor_task.cancel()

            # Cancel any remaining orders
            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)

            _snapshot_new_orders()

            # Flush stale fills before reconciliation overwrites tracker
            self._flush_fill_queue(fill_queue)

            await self._reconcile_via_orders(all_placed_orders, state)

            pos = self._tracker.position
            # Final mtm
            try:
                fin_up = await self._client.get_order_book(market.up_token)
                fin_down = await self._client.get_order_book(market.down_token)
                final_mtm = self._calc_mtm(
                    pos, self._best_bid(fin_up), self._best_bid(fin_down)
                )
            except Exception:
                final_mtm = 0.0

            result = {
                "session_id": session_id,
                "condition_id": market.condition_id,
                "total_trades": state.total_trades,
                "up_shares": pos.up_shares,
                "down_shares": pos.down_shares,
                "combined_vwap": pos.combined_vwap,
                "worst_case_pnl": pos.risk_worst_case,
                "mtm_profit": final_mtm,
                "projected_pnl": self._tracker.report_worst_case,
                "rebate_estimate": self._tracker.rebate_estimate,
            }
            self._state_manager.save_session_result(result)
            logger.info(
                "[HEDGE_SELL] Session end: up=%.0f down=%.0f mtm=$%.2f worst=$%.2f",
                pos.up_shares, pos.down_shares, final_mtm, pos.risk_worst_case,
            )
            await self._alerts.session_complete(
                fills=state.total_trades,
                combined_vwap=pos.combined_vwap,
                risk_pnl=pos.risk_worst_case,
                report_pnl=self._tracker.report_worst_case,
                up_shares=pos.up_shares,
                down_shares=pos.down_shares,
            )
            return result

    async def _try_taker_flatten(
        self,
        market: MarketInfo,
        state: BotState,
        fill_monitor: FillMonitor,
        max_notional_override: float = 0,
        max_shares_override: int = 0,
        skip_price_cap: bool = False,
        profitable_only: bool = False,
    ) -> Optional[str]:
        """EV-aware taker rebalancing — buy lagging side at best_ask.

        Economics-driven sizing:
        - ask < 50c (EV-positive): buy aggressively, each share profits in expectation
        - ask >= 50c (EV-negative): scale down, only buy what risk reduction justifies
        - Logs full economics: cost, EV delta, worst-case before/after

        Returns order_id if placed, None otherwise.
        """
        pos = self._tracker.position
        cfg = self._config
        if pos.total_shares <= 0:
            return None

        lagging_side = "up" if pos.up_shares < pos.down_shares else "down"
        deficit = abs(pos.up_shares - pos.down_shares)
        if deficit < 5:
            return None

        lagging_token = market.up_token if lagging_side == "up" else market.down_token
        book = await self._client.get_order_book(lagging_token)
        asks = book.get("asks", [])
        if not asks:
            logger.warning("Taker rebalance: no asks for %s", lagging_side)
            return None
        best_ask = min(float(a["price"]) for a in asks)

        # Fix #81: Simplified taker price cap.
        # Every lagging share bought at price P improves worst by (1 - P).
        # When worst < 0, ANY buy under 95c helps — skip pair check entirely.
        # When worst >= 0, use pair profitability as a sanity check.
        # Min ask threshold: don't taker-buy below 63c — market is still close
        # to 50/50 and maker orders will likely fill naturally. Only chase when
        # the market has moved significantly (ask > 63c).
        # WINDING_DOWN bypasses via skip_price_cap.
        if not skip_price_cap:
            _worst_for_cap = pos.risk_worst_case - getattr(self, "_usdc_cost_drift", 0.0)
            if best_ask < 0.60:
                return None
            if best_ask > 0.95:
                now_t = time.time()
                if now_t - getattr(self, "_taker_price_cap_log_ts", 0) >= 30:
                    logger.info(
                        "Taker skip: %s ask %.0fc > 95c hard cap [up=%.0f down=%.0f worst=$%.1f]",
                        lagging_side, best_ask * 100, pos.up_shares, pos.down_shares, _worst_for_cap,
                    )
                    self._taker_price_cap_log_ts = now_t
                return None
            # When worst >= 0 (profitable), apply pair check to avoid wasteful buys.
            # When worst < 0 (losing), skip pair check — rebalancing at any price < 95c
            # improves worst-case and is better than sitting idle.
            if _worst_for_cap >= 0:
                heavy_shares = pos.down_shares if lagging_side == "up" else pos.up_shares
                heavy_cost = pos.down_cost if lagging_side == "up" else pos.up_cost
                heavy_avg = heavy_cost / heavy_shares if heavy_shares > 0 else 0.50
                pair_cost = heavy_avg + best_ask
                if pair_cost > 1.0:
                    now_t = time.time()
                    if now_t - getattr(self, "_taker_price_cap_log_ts", 0) >= 30:
                        logger.info(
                            "Taker skip: %s ask %.0fc pair=%.2f (heavy_avg=%.2f + ask=%.2f > $1.00) [up=%.0f down=%.0f worst=$%.1f]",
                            lagging_side, best_ask * 100, pair_cost, heavy_avg, best_ask,
                            pos.up_shares, pos.down_shares, _worst_for_cap,
                        )
                        self._taker_price_cap_log_ts = now_t
                    return None

        # --- EV-aware sizing ---
        # EV per share bought at best_ask (assuming 50/50 outcome):
        #   If lagging wins: share worth $1, cost = ask → profit = 1 - ask
        #   If heavy wins:   share worth $0, cost = ask → loss = -ask
        #   EV = (1-ask)/2 - ask/2 = 0.5 - ask
        ev_per_share = 0.50 - best_ask
        is_ev_positive = ev_per_share > 0

        # Fix #74: With pair-profitability cap, taker buys may be above 50c (EV-negative
        # per share) but profitable as a pair. No worst-case override needed.
        _worst_override = False

        # Start with full deficit as target
        shares = float(min(deficit, max_shares_override) if max_shares_override > 0 else deficit)

        # --- Dollar budget ---
        # EV-positive: allow spending up to half of remaining capital (rebalance pays for itself).
        if not max_notional_override:
            total_committed = pos.total_cost + state.reserved_notional + state.cancel_race_buffer
            remaining_capital = max(0, cfg.session_capital_limit - total_committed)
            max_taker = max(
                cfg.session_capital_limit * cfg.trend_taker_budget_pct,
                remaining_capital * 0.50,
            )
        else:
            max_taker = max_notional_override or (cfg.session_capital_limit * cfg.taker_flatten_max_notional_pct)

        remaining_budget = max_taker - state.taker_notional_used
        if remaining_budget <= 0:
            now_t = time.time()
            if now_t - getattr(self, "_taker_budget_log_ts", 0) >= 30:
                logger.info("Taker rebalance: budget exhausted ($%.1f used of $%.1f)", state.taker_notional_used, max_taker)
                self._taker_budget_log_ts = now_t
            return None
        budget_max_shares = remaining_budget / best_ask if best_ask > 0 else 0
        shares = min(shares, budget_max_shares)

        # --- Avg cost guard: compute max shares that keep avg ≤ target ---
        # Only applies when ask > target (cheap buys always help avg).
        # Fix #53a: Also skip guard when ask < current avg (always lowers avg).
        # Fix #81: When worst < 0, skip avg guard — getting balanced is more
        # important than keeping avg cheap. A balanced position at 53c avg has
        # far better worst-case than an imbalanced one at 49c avg.
        _worst_for_avg = pos.risk_worst_case - getattr(self, "_usdc_cost_drift", 0.0)
        if not skip_price_cap and _worst_for_avg >= 0:
            target = cfg.trend_rebalance_avg_cost_limit if cfg.strategy == "trend" else cfg.passive_avg_cost_target
            current_avg = pos.avg_cost_per_share if pos.total_shares > 0 else 0.0
            if target > 0 and pos.total_shares > 0 and best_ask > target and best_ask >= current_avg:
                avg_budget = target * pos.total_shares - pos.total_cost
                if avg_budget <= 0:
                    now = time.time()
                    if now - getattr(self, "_taker_avg_cap_log_ts", 0) >= 30:
                        logger.info(
                            "Taker rebalance: avg %.4f >= target %.3f — skipping",
                            pos.avg_cost_per_share, target,
                        )
                        self._taker_avg_cap_log_ts = now
                    return None
                avg_cost_max = avg_budget / (best_ask - target)
                if avg_cost_max < shares:
                    now = time.time()
                    if now - getattr(self, "_taker_avg_cap_log_ts", 0) >= 30:
                        logger.info(
                            "Taker rebalance: avg cost caps %s @ %.2fc to %d shares (wanted %d)",
                            lagging_side, best_ask * 100, int(avg_cost_max), int(shares),
                        )
                        self._taker_avg_cap_log_ts = now
                    shares = avg_cost_max

        # --- Floor checks ---
        shares = int(shares)
        if shares < 5:
            return None
        if shares * best_ask < 1.0:
            return None

        # --- Log economics before placing ---
        cost = shares * best_ask
        lagging_s = pos.up_shares if lagging_side == "up" else pos.down_shares
        heavy_s = pos.down_shares if lagging_side == "up" else pos.up_shares
        old_worst = min(pos.up_shares, pos.down_shares) - pos.total_cost
        new_min_s = min(lagging_s + shares, heavy_s)
        new_worst = new_min_s - (pos.total_cost + cost)
        ev_delta = shares * ev_per_share
        logger.info(
            "Taker rebalance: %s %d shares @ %.2fc, cost=$%.1f, "
            "EV_delta=$%.1f (%s), worst: $%.1f→$%.1f, deficit: %d→%d "
            "[spent $%.0f/%.0f, up=%.0f down=%.0f]",
            lagging_side, shares, best_ask * 100, cost,
            ev_delta, "profitable",
            old_worst, new_worst,
            int(deficit), max(0, int(deficit - shares)),
            pos.total_cost, self._config.session_capital_limit,
            pos.up_shares, pos.down_shares,
        )

        try:
            expiration_ts = int(market.close_ts)
            order_id = await self._client.place_taker_order(
                token_id=lagging_token,
                price=best_ask,
                size=round(shares, 2),
                side=lagging_side,
                expiration_ts=expiration_ts,
            )
            state.taker_notional_used += shares * best_ask
            fill_monitor.register_taker_order(
                order_id, price=best_ask,
                size=round(shares, 2), token_id=lagging_token,
            )
            state.order_map[order_id] = {
                "side": lagging_side, "price": best_ask,
                "token_id": lagging_token, "original_size": round(shares, 2),
                "taker": True,
            }
            try:
                _bal = await self._client.get_balance()
                logger.info("USDC after taker: $%.2f", _bal)
            except Exception:
                pass
            return order_id
        except Exception as e:
            logger.error("Taker rebalance failed: %s", e)
            return None

    async def _handle_risk_verdict(
        self,
        verdict,
        order_engine: OrderEngine,
        market: MarketInfo,
        state: BotState,
    ) -> None:
        """Act on a risk verdict."""
        if verdict.action == RiskAction.NONE:
            self._last_risk_action = RiskAction.NONE
            self._last_risk_reason = ""
            return
        if verdict.action == RiskAction.STOP_ADDING:
            if self._last_risk_action != RiskAction.STOP_ADDING:
                logger.warning("Risk: stop adding - %s", verdict.reason)
            self._last_risk_action = RiskAction.STOP_ADDING
            self._last_risk_reason = verdict.reason
        elif verdict.action == RiskAction.CANCEL_HEAVY_SIDE:
            heavy_side = self._tracker.position.excess_side
            heavy_token = (
                market.up_token if heavy_side == "up" else market.down_token
            )
            await self._client.cancel_market_orders(heavy_token)
            # Fix #9: Clean up local state so budget is released
            side_oids = [
                oid for oid, meta in state.order_map.items()
                if meta.get("side") == heavy_side and not meta.get("cancelled")
            ]
            for oid in side_oids:
                level = state.active_orders.pop(oid, None)
                if level:
                    order_engine.on_cancel_release(level, state)
            if side_oids:
                mark_orders_cancelled(state, side_oids)
            logger.warning(
                "Risk: cancelled heavy side %s (%d orders released) - %s",
                heavy_side, len(side_oids), verdict.reason,
            )
            self._last_risk_action = RiskAction.CANCEL_HEAVY_SIDE
            self._last_risk_reason = verdict.reason
        elif verdict.action == RiskAction.CANCEL_ALL:
            now = time.time()
            cooldown_s = max(2.0, self._config.monitor_loop_interval_s * 5.0)
            same_reason = (
                self._last_risk_action == RiskAction.CANCEL_ALL
                and self._last_risk_reason == verdict.reason
            )
            if same_reason and (now - self._last_cancel_all_ts) < cooldown_s:
                return

            self._last_cancel_all_ts = now
            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)

            # Keep local state aligned after global cancel to avoid stale orders.
            released = 0
            cancelled_oids = []
            for oid, level in list(state.active_orders.items()):
                order_engine.on_cancel_release(level, state)
                cancelled_oids.append(oid)
                released += 1
            state.active_orders.clear()
            if cancelled_oids:
                mark_orders_cancelled(state, cancelled_oids)

            logger.error(
                "Risk: cancel_all - %s (released %d local orders)",
                verdict.reason,
                released,
            )
            if not same_reason:
                await self._alerts.error(
                    f"Risk action: {verdict.action.value} - {verdict.reason}"
                )
            self._last_risk_action = RiskAction.CANCEL_ALL
            self._last_risk_reason = verdict.reason
        elif verdict.action in (
            RiskAction.KILL_SESSION,
            RiskAction.KILL_DAY,
        ):
            await self._client.cancel_and_verify(market.up_token)
            await self._client.cancel_and_verify(market.down_token)
            logger.error(
                "Risk: %s - %s", verdict.action.value, verdict.reason
            )
            await self._alerts.error(
                f"Risk action: {verdict.action.value} - {verdict.reason}"
            )
            self._last_risk_action = verdict.action
            self._last_risk_reason = verdict.reason

    async def shutdown(self) -> None:
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown = True
