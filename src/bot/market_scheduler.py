"""Market discovery, time sync, and phase determination.

Architecture:
- sync_time(): aligns local clock with exchange server time
- get_current_phase(): determines MarketPhase from adjusted timestamps
- discover_next_market(): queries Gamma API for upcoming BTC 15-minute markets
- check_pre_entry_gate(): validates edge before entering a market
"""

from __future__ import annotations

import json
import logging
import time
from typing import List, Optional, Tuple, Union

import httpx

from src.bot.types import BotConfig, EdgeTier, MarketInfo, MarketPhase
from src.bot.math_engine import estimate_initial_edge, estimate_ladder_edge
from src.bot.order_engine import build_ladder
from src.bot.client import ExchangeClient

logger = logging.getLogger(__name__)

GAMMA_API_URL = "https://gamma-api.polymarket.com"


def _parse_json_field(value: Union[str, list]) -> list:
    """Parse a field that might be a JSON string or already a list."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


def _iso_to_ts(iso_str: str) -> float:
    """Parse ISO 8601 datetime string to Unix timestamp."""
    from datetime import datetime, timezone
    # Handle both 'Z' suffix and '+00:00'
    iso_str = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_str)
    return dt.timestamp()


def _parse_market_from_gamma(raw: dict) -> Optional[MarketInfo]:
    """Parse a Gamma API market response into MarketInfo.

    Returns None if required fields are missing or parsing fails.
    """
    try:
        condition_id = raw.get("conditionId") or raw.get("condition_id", "")
        if not condition_id:
            return None

        outcomes = _parse_json_field(raw.get("outcomes", []))
        tokens = _parse_json_field(raw.get("clobTokenIds", []))

        if len(outcomes) < 2 or len(tokens) < 2:
            return None

        # Map outcomes to Up/Down tokens
        up_token = None
        down_token = None
        for i, outcome in enumerate(outcomes):
            name = outcome.lower() if isinstance(outcome, str) else ""
            if name in ("up", "yes"):
                up_token = tokens[i]
            elif name in ("down", "no"):
                down_token = tokens[i]

        if not up_token or not down_token:
            return None

        # eventStartTime is the actual window start; startDate is event creation
        start_iso = raw.get("eventStartTime") or raw.get("startDate", "")
        end_iso = raw.get("endDate", "")

        if not start_iso or not end_iso:
            return None

        open_ts = _iso_to_ts(start_iso)
        close_ts = _iso_to_ts(end_iso)

        return MarketInfo(
            condition_id=condition_id,
            up_token=up_token,
            down_token=down_token,
            open_ts=open_ts,
            close_ts=close_ts,
            tick_size=0.01,
            min_order_size=1.0,
        )
    except Exception as e:
        logger.warning("Failed to parse market from Gamma: %s", e)
        return None


def _get_effective_price(book: dict, default: float = 0.50) -> float:
    """Get effective entry price from order book for penny-ladder strategy.

    Uses the ACTUAL market price — best bid tells us where the market values
    this token. We place our ladder below the best bid to get filled.

    If best_ask is reasonable (< 0.90), use that (tighter reference).
    Otherwise use best_bid. Fallback to 0.50 only for truly empty books.
    """
    asks = book.get("asks", [])
    if asks:
        best_ask = min(float(a.get("price", 1.0)) for a in asks)
        if best_ask < 0.90:
            return best_ask

    bids = book.get("bids", [])
    if bids:
        best_bid = max(float(b.get("price", 0)) for b in bids)
        if best_bid > 0.01:
            return best_bid

    return default


def _get_best_ask(book: dict) -> Optional[float]:
    """Return best ask price, or None if no asks."""
    asks = book.get("asks", []) or []
    if not asks:
        return None
    try:
        return min(float(a.get("price", 1.0)) for a in asks)
    except Exception:
        return None


def _get_best_bid_price(book: dict, default: float = 0.50) -> float:
    """Return best bid price for passive ladder anchoring.

    For passive mode, we anchor to the best bid — this ensures all ladder
    levels sit AT or BELOW the current market, acting as true maker orders.
    Anchoring to the ask (like _get_effective_price) places levels above
    the bid that fill instantly at expensive prices.
    """
    bids = book.get("bids", [])
    if bids:
        best_bid = max(float(b.get("price", 0)) for b in bids)
        if best_bid > 0.01:
            return best_bid
    # Fallback to ask-based if no bids
    return _get_effective_price(book, default)


class MarketScheduler:
    """Handles market discovery, time synchronization, and phase transitions."""

    def __init__(self, client: ExchangeClient, config: BotConfig):
        self._client = client
        self._config = config
        self._time_offset: float = 0.0
        self._last_discovered_cid: str = ""  # dedup discovery logs

    async def sync_time(self) -> float:
        """Synchronize local clock with exchange server time.

        Returns the computed offset (server - local).
        """
        server_time = await self._client.get_server_time()
        self._time_offset = server_time - time.time()
        logger.info("Time sync: offset=%.3fs", self._time_offset)
        return self._time_offset

    def adjusted_now(self) -> float:
        """Return current time adjusted by server offset."""
        return time.time() + self._time_offset

    def get_current_phase(self, market: MarketInfo) -> MarketPhase:
        """Determine the current market phase based on adjusted time."""
        now = self.adjusted_now()

        if now < market.open_ts:
            return MarketPhase.DISCOVERED
        if now < market.open_ts + self._config.entry_delay_s:
            return MarketPhase.PRE_ENTRY
        if now < market.close_ts - self._config.winding_down_buffer_s:
            return MarketPhase.ACTIVE
        if now < market.close_ts:
            return MarketPhase.WINDING_DOWN
        return MarketPhase.CLOSED

    async def discover_next_market(self) -> Optional[MarketInfo]:
        """Discover the next BTC 15-minute market.

        Primary path:
        - slug-based /events lookup for current/next 15-minute slots.
        Fallback path:
        - scan active /markets and filter BTC up/down style questions.
        """
        now = self.adjusted_now()
        now_int = int(now)
        current_slot = (now_int // 900) * 900

        # Try current, next, and next+1 slots (15-minute = 900s)
        slots_to_try = [current_slot, current_slot + 900, current_slot + 1800]

        candidates: List[MarketInfo] = []

        try:
            async with httpx.AsyncClient(timeout=30.0) as http:
                for slot in slots_to_try:
                    slug = f"btc-updown-15m-{slot}"
                    try:
                        resp = await http.get(
                            f"{GAMMA_API_URL}/events",
                            params={"slug": slug},
                        )
                        resp.raise_for_status()
                        events = resp.json()
                    except Exception as e:
                        logger.debug("Slug %s fetch failed: %s", slug, e)
                        continue

                    if not events or not events[0].get("markets"):
                        continue

                    raw = events[0]["markets"][0]
                    info = _parse_market_from_gamma(raw)
                    if info and info.close_ts > now:
                        # Use slot timestamp as open_ts (more precise than startDate)
                        info.open_ts = float(slot)
                        info.close_ts = float(slot + 900)
                        candidates.append(info)
                        logger.debug("Found market: %s (slot %d)", slug, slot)
        except Exception as e:
            logger.error("Gamma API request failed: %s", e)
            return None

        if not candidates:
            # Fallback: scan active markets (Gamma sometimes lags slug indexing).
            try:
                async with httpx.AsyncClient(timeout=30.0) as http:
                    resp = await http.get(
                        f"{GAMMA_API_URL}/markets",
                        params={
                            "limit": 200,
                            "active": "true",
                            "closed": "false",
                            "order": "endDate",
                            "ascending": "true",
                        },
                    )
                    resp.raise_for_status()
                    markets = resp.json()

                for raw in markets:
                    question = str(raw.get("question", "")).lower()
                    slug = str(raw.get("slug", "")).lower()
                    is_btc = ("bitcoin" in question) or ("btc" in question) or ("bitcoin" in slug) or ("btc" in slug)
                    is_updown_style = (
                        ("up or down" in question)
                        or ("updown" in slug)
                        or ("15m" in slug)
                        or ("15 min" in question)
                    )
                    if not (is_btc and is_updown_style):
                        continue

                    info = _parse_market_from_gamma(raw)
                    if info and info.close_ts > now:
                        candidates.append(info)
            except Exception as e:
                logger.error("Gamma markets fallback failed: %s", e)
                return None

            if candidates:
                logger.info("Discovery fallback selected %d BTC candidates", len(candidates))
            else:
                return None

        # Prefer active markets with enough time left, then upcoming
        active = [
            m for m in candidates
            if m.open_ts <= now < m.close_ts - self._config.winding_down_buffer_s
        ]
        if active:
            best = max(active, key=lambda m: m.close_ts)
        else:
            # No active market — pick the soonest upcoming
            upcoming = [m for m in candidates if m.open_ts > now]
            if upcoming:
                best = min(upcoming, key=lambda m: m.open_ts)
            else:
                best = max(candidates, key=lambda m: m.close_ts)

        # Enrich with tick_size and min_order_size from CLOB
        try:
            book = await self._client.get_order_book(best.up_token)
            best.tick_size = float(book.get("tick_size", 0.01))
            best.min_order_size = float(book.get("min_order_size", 1.0))
        except Exception as e:
            logger.warning("Could not fetch order book for enrichment: %s", e)

        # Only log when discovering a new market, not the same one repeatedly
        if best.condition_id != self._last_discovered_cid:
            logger.info(
                "Discovered market: condition=%s, open=%d, close=%d, tick=%.2f",
                best.condition_id[:16], int(best.open_ts), int(best.close_ts), best.tick_size,
            )
            self._last_discovered_cid = best.condition_id
        return best

    async def check_pre_entry_gate(
        self, market: MarketInfo
    ) -> Tuple[bool, EdgeTier]:
        """Check if market has sufficient edge for entry.

        Uses effective reference prices (book-aware) to estimate the fillable ladder.
        Raw best asks are logged for diagnostics but are not a hard blocker.

        Returns (should_enter, tier).
        """
        up_book = await self._client.get_order_book(market.up_token)
        down_book = await self._client.get_order_book(market.down_token)

        if (not up_book.get("asks") and not up_book.get("bids")) or (
            not down_book.get("asks") and not down_book.get("bids")
        ):
            logger.warning("Pre-entry gate: empty book on at least one side")
            return (False, EdgeTier.NEGATIVE)

        up_ref = _get_effective_price(up_book)
        down_ref = _get_effective_price(down_book)
        ref_tier = estimate_initial_edge(up_ref, down_ref)

        # Estimate edge using the same ladder builder we will place with.
        up_ladder = build_ladder(
            best_ask=up_ref,
            tick_size=market.tick_size,
            shares_per_order=self._config.shares_per_order,
            num_levels=15,
        )
        down_ladder = build_ladder(
            best_ask=down_ref,
            tick_size=market.tick_size,
            shares_per_order=self._config.shares_per_order,
            num_levels=15,
        )

        if len(up_ladder) < 3 or len(down_ladder) < 3:
            logger.info(
                "Pre-entry gate: ladder too shallow up=%d down=%d (ref up=%.3f down=%.3f)",
                len(up_ladder), len(down_ladder), up_ref, down_ref,
            )
            return (False, EdgeTier.NEGATIVE)

        ladder_edge = estimate_ladder_edge(up_ladder, down_ladder)
        expected_combined = 1.0 - ladder_edge

        # Imbalance resilience check: reject if edge can't survive N excess fills.
        # hedged_profit from top-5 matched pairs must exceed cost of N excess fills.
        if self._config.entry_min_excess_tolerance > 0 and ladder_edge > 0:
            top_n = min(5, len(up_ladder), len(down_ladder))
            shares = self._config.shares_per_order
            hedged_profit_est = top_n * shares * ladder_edge
            # Excess cost: use deepest (cheapest) level — excess fills typically
            # come from the tail of the ladder where only one side gets filled.
            min_side_price = min(
                up_ladder[-1].price_cents / 100.0 if up_ladder else 1.0,
                down_ladder[-1].price_cents / 100.0 if down_ladder else 1.0,
            )
            one_fill_cost = shares * min_side_price
            survivable = (
                hedged_profit_est / one_fill_cost if one_fill_cost > 0 else 0.0
            )
            if survivable < self._config.entry_min_excess_tolerance - 0.05:
                logger.info(
                    "Pre-entry gate: reject — edge too thin for imbalance "
                    "(survivable=%.1f fills, need %d, edge=%.3f)",
                    survivable,
                    self._config.entry_min_excess_tolerance,
                    ladder_edge,
                )
                return (False, EdgeTier.NEGATIVE)

        if expected_combined >= self._config.entry_max_expected_combined:
            up_best_ask = _get_best_ask(up_book)
            down_best_ask = _get_best_ask(down_book)
            ask_combined = (
                (up_best_ask + down_best_ask)
                if up_best_ask is not None and down_best_ask is not None
                else float("nan")
            )
            logger.info(
                "Pre-entry gate: reject expected_combined=%.3f (max=%.3f) "
                "| ref=%.3f/%.3f asks=%.3f",
                expected_combined,
                self._config.entry_max_expected_combined,
                up_ref,
                down_ref,
                ask_combined,
            )
            return (False, EdgeTier.NEGATIVE)

        up_best_ask = _get_best_ask(up_book)
        down_best_ask = _get_best_ask(down_book)
        ask_combined = (
            (up_best_ask + down_best_ask)
            if up_best_ask is not None and down_best_ask is not None
            else float("nan")
        )
        logger.info(
            "Pre-entry gate: refs %.3f/%.3f tier=%s | ladder expected=%.3f edge=%.3f | asks=%.3f",
            up_ref, down_ref, ref_tier.value, expected_combined, ladder_edge, ask_combined,
        )

        return (True, ref_tier)
