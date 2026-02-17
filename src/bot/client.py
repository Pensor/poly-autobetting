"""Exchange adapter: abstract protocol + Polymarket + Paper implementations.

Architecture:
- Reads: httpx (async-native)
- Writes: py-clob-client via asyncio.run_in_executor() (sync SDK)
- Thread safety: ThreadPoolExecutor(max_workers=1) serializes all SDK calls
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import httpx

from src.bot.types import FillEvent
from src.bot.math_engine import clamp_price, round_to_tick

if __import__("typing").TYPE_CHECKING:
    from src.bot.ws_book_feed import WSBookFeed

logger = logging.getLogger(__name__)


class ExchangeClient(ABC):
    """Abstract exchange interface."""

    @abstractmethod
    async def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
    ) -> str:
        """Place a limit order. Returns order_id."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        ...

    @abstractmethod
    async def cancel_orders(self, order_ids: List[str]) -> bool:
        """Cancel multiple orders by ID in a single API call."""
        ...

    @abstractmethod
    async def cancel_all(self) -> bool:
        ...

    @abstractmethod
    async def cancel_market_orders(self, asset_id: str) -> bool:
        ...

    @abstractmethod
    async def get_trades(self, asset_id: str, after_ts: int, maker_address: str = "") -> List[FillEvent]:
        ...

    @abstractmethod
    async def get_open_orders(self, identifier: str) -> List[Dict]:
        ...

    @abstractmethod
    async def get_order_book(self, token_id: str) -> Dict:
        ...

    @abstractmethod
    async def get_balance(self) -> float:
        ...

    @abstractmethod
    async def get_server_time(self) -> float:
        ...

    @abstractmethod
    async def get_order(self, order_id: str) -> Dict:
        """Get a single order by ID (any status). Returns empty dict on failure."""
        ...

    @abstractmethod
    async def get_min_order_size(self, token_id: str) -> float:
        ...

    @abstractmethod
    async def place_taker_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
        *,
        sell: bool = False,
    ) -> str:
        """Place a GTC limit order WITHOUT post_only (taker-eligible). Returns order_id.

        When sell=True, places a SELL order (to dispose of shares you own).
        """
        ...

    @abstractmethod
    async def get_position(self, token_id: str) -> float:
        """Get actual position size for a token from the exchange. Returns share count."""
        ...

    async def place_orders_batch(
        self,
        orders: List[Dict],
    ) -> List[str]:
        """Place multiple limit BUY orders in a single API call.

        Each dict: {token_id, price, size, side, expiration_ts}.
        Returns list of order_ids (empty string for failed entries).
        Max 15 orders per call (Polymarket limit).
        """
        ...

    async def ensure_allowances(self, token_ids: list[str] | None = None) -> bool:
        """Ensure exchange allowances are set (COLLATERAL + CONDITIONAL).
        Must be called before placing SELL orders. Returns True on success."""
        return True

    async def cancel_and_verify(self, asset_id: str, max_retries: int = 5) -> bool:
        """Cancel all orders for asset and verify none remain."""
        for attempt in range(max_retries):
            await self.cancel_market_orders(asset_id)
            await asyncio.sleep(1.0)
            open_orders = await self.get_open_orders(asset_id)
            if not open_orders:
                logger.info("cancel_and_verify: all orders cancelled (attempt %d)", attempt + 1)
                return True
            logger.warning(
                "cancel_and_verify: %d orders remain (attempt %d/%d)",
                len(open_orders), attempt + 1, max_retries,
            )
            # Try individual cancels for remaining
            for order in open_orders:
                order_id = order.get("id", order.get("order_id", ""))
                if order_id:
                    await self.cancel_order(order_id)
        # Last resort
        await self.cancel_all()
        return False


async def _retry_with_backoff(coro_fn, max_retries: int = 3) -> Any:
    """Exponential backoff with jitter: 1s+-0.5, 2s+-1, 4s+-2."""
    def _is_non_retryable(exc: Exception) -> bool:
        msg = str(exc).lower()
        # Validation/contract errors won't succeed on retry.
        if (
            "status_code=400" in msg
            or "invalid expiration value" in msg
            or "invalid amount" in msg
            or "bad request" in msg
        ):
            return True
        # Fix #42: ReadTimeout means the request was likely received by the
        # server — retrying would place DUPLICATE orders.  Treat as fatal.
        if "readtimeout" in msg or "read operation timed out" in msg:
            return True
        return False

    last_error = None
    for attempt in range(max_retries):
        try:
            return await coro_fn()
        except Exception as e:
            last_error = e
            if _is_non_retryable(e):
                raise
            if attempt < max_retries - 1:
                base_delay = 2 ** attempt
                jitter = base_delay * 0.5 * (random.random() * 2 - 1)
                delay = max(0.1, base_delay + jitter)
                logger.warning("Retry %d/%d after %.1fs: %s", attempt + 1, max_retries, delay, e)
                await asyncio.sleep(delay)
    raise last_error


class PolymarketClient(ExchangeClient):
    """Real Polymarket exchange client.

    Uses py-clob-client (sync) for writes, httpx (async) for reads.
    All SDK calls serialized through ThreadPoolExecutor(max_workers=1).
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        private_key: str,
        funder: str = "",
        clob_url: str = "https://clob.polymarket.com",
        chain_id: int = 137,
        ws_feed: "WSBookFeed | None" = None,
    ):
        self._clob_url = clob_url
        self._http = httpx.AsyncClient(base_url=clob_url, timeout=15.0)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._sdk_client = None
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._private_key = private_key
        self._funder = funder  # Smart wallet address (Gnosis Safe)
        self._chain_id = chain_id
        self._ws_feed = ws_feed

    def _get_sdk_client(self):
        """Lazy-init the synchronous SDK client."""
        if self._sdk_client is None:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            funder_kwarg = {}
            if self._funder:
                funder_kwarg["funder"] = self._funder
            self._sdk_client = ClobClient(
                self._clob_url,
                chain_id=self._chain_id,
                key=self._private_key,
                signature_type=2,  # Polymarket Gnosis Safe
                **funder_kwarg,
            )
            # Use provided creds if available, otherwise derive from private key
            if self._api_key and self._api_secret and self._passphrase:
                creds = ApiCreds(
                    api_key=self._api_key,
                    api_secret=self._api_secret,
                    api_passphrase=self._passphrase,
                )
            else:
                creds = self._sdk_client.derive_api_key()
                logger.info("Derived API credentials from private key")
            self._sdk_client.set_api_creds(creds)
        return self._sdk_client

    async def _run_sdk(self, fn, *args, **kwargs):
        """Run a synchronous SDK call in the executor with timeout."""
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs)),
            timeout=10.0,
        )

    async def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
    ) -> str:
        """Place a GTD limit buy order."""
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import OrderArgs, OrderType

        tick_size = 0.01  # default, should be passed from MarketInfo
        price = clamp_price(price, tick_size)
        price = round_to_tick(price, tick_size)
        size = round(size, 2)

        async def _place():
            client = self._get_sdk_client()
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=BUY,
                expiration=expiration_ts,
            )
            signed_order = await self._run_sdk(client.create_order, order_args)
            result = await self._run_sdk(client.post_order, signed_order, OrderType.GTD)
            order_id = result.get("orderID", result.get("id", ""))
            if not order_id:
                raise ValueError(f"API returned no order_id: {result}")
            logger.info("Placed order %s: %s %.2f @ %.2f", order_id, side, size, price)
            return order_id

        return await _retry_with_backoff(_place)

    async def cancel_order(self, order_id: str) -> bool:
        async def _cancel():
            client = self._get_sdk_client()
            result = await self._run_sdk(client.cancel, order_id)
            return bool(result)
        try:
            return await _retry_with_backoff(_cancel)
        except Exception as e:
            logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    async def cancel_orders(self, order_ids: List[str]) -> bool:
        if not order_ids:
            return True
        async def _cancel():
            client = self._get_sdk_client()
            result = await self._run_sdk(client.cancel_orders, order_ids)
            return bool(result)
        try:
            return await _retry_with_backoff(_cancel)
        except Exception as e:
            logger.error("Failed to cancel_orders (%d ids): %s", len(order_ids), e)
            return False

    async def cancel_all(self) -> bool:
        async def _cancel():
            client = self._get_sdk_client()
            result = await self._run_sdk(client.cancel_all)
            return bool(result)
        try:
            return await _retry_with_backoff(_cancel)
        except Exception as e:
            logger.error("Failed to cancel all: %s", e)
            return False

    async def cancel_market_orders(self, asset_id: str) -> bool:
        async def _cancel():
            client = self._get_sdk_client()
            result = await self._run_sdk(client.cancel_market_orders, asset_id)
            return bool(result)
        try:
            return await _retry_with_backoff(_cancel)
        except Exception as e:
            logger.error("Failed to cancel market orders for %s: %s", asset_id, e)
            return False

    async def get_trades(self, asset_id: str, after_ts: int, maker_address: str = "") -> List[FillEvent]:
        """Fetch trades via SDK (requires L2 auth). Handles pagination."""
        from py_clob_client.clob_types import TradeParams

        def _fetch():
            client = self._get_sdk_client()
            params = TradeParams(asset_id=asset_id, after=int(after_ts))
            if maker_address:
                # Use lowercase address for API-side filtering consistency.
                params.maker_address = maker_address.strip().lower()
            return client.get_trades(params=params)

        try:
            raw_trades = await self._run_sdk(_fetch)
        except Exception as e:
            logger.warning("get_trades failed: %s", e)
            return []

        all_fills: List[FillEvent] = []
        maker_lower = maker_address.lower() if maker_address else ""
        for t in raw_trades:
            # Client-side maker filter:
            # - If payload contains maker/owner, enforce exact match.
            # - If payload omits address fields, keep row and trust API-side filtering.
            #   The SDK `get_trades` endpoint is user-authenticated and often omits maker fields.
            if maker_lower:
                trade_maker = (t.get("maker_address", "") or "").lower()
                # Do not reject by top-level owner mismatch: owner can reflect
                # taker/counterparty on some payload variants.
                if trade_maker:
                    if trade_maker != maker_lower:
                        continue
                else:
                    maker_orders = t.get("maker_orders") or []
                    if isinstance(maker_orders, list) and maker_orders:
                        matched_nested = False
                        for mo in maker_orders:
                            mo_maker = (
                                mo.get("maker_address", "") or mo.get("owner", "") or ""
                            ).lower()
                            if mo_maker == maker_lower:
                                matched_nested = True
                                break
                        if not matched_nested:
                            continue

            order_id = t.get("order_id", "")
            if not order_id:
                maker_orders = t.get("maker_orders") or []
                if isinstance(maker_orders, list):
                    picked = None
                    if maker_lower:
                        for mo in maker_orders:
                            mo_maker = (mo.get("maker_address", "") or mo.get("owner", "") or "").lower()
                            if mo_maker == maker_lower:
                                picked = mo
                                break
                    if not picked and maker_orders:
                        picked = maker_orders[0]
                    if picked:
                        order_id = picked.get("order_id", picked.get("id", ""))

            trade_side = str(t.get("side", "")).upper()
            sign = -1.0 if trade_side == "SELL" else 1.0
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            fill = FillEvent(
                trade_id=t.get("id", t.get("trade_id", "")),
                order_id=order_id,
                side=t.get("asset_id", ""),  # will be mapped by caller
                price=price,
                filled_shares=sign * size,
                usdc_cost=sign * price * size,
                trader_side=t.get("trader_side", t.get("side", "UNKNOWN")),
                timestamp=int(t.get("match_time", t.get("timestamp", 0))),
            )
            all_fills.append(fill)

        logger.debug("get_trades: %d raw, %d after filter", len(raw_trades), len(all_fills))
        return all_fills

    async def get_open_orders(self, identifier: str) -> List[Dict]:
        """Get open orders via SDK (requires L2 auth).

        Identifier can be asset_id (preferred) or market/condition_id.
        We query by inferred identifier type first:
        - condition id (`0x...`) -> market
        - otherwise -> asset_id
        """
        from py_clob_client.clob_types import OpenOrderParams

        def _fetch_asset():
            client = self._get_sdk_client()
            params = OpenOrderParams(asset_id=identifier)
            return client.get_orders(params)

        def _fetch_market():
            client = self._get_sdk_client()
            params = OpenOrderParams(market=identifier)
            return client.get_orders(params)

        try:
            looks_like_market = isinstance(identifier, str) and identifier.startswith("0x")
            first = _fetch_market if looks_like_market else _fetch_asset
            second = _fetch_asset if looks_like_market else _fetch_market

            orders = await self._run_sdk(first)
            if orders:
                return orders
            # If first query by market/asset_id returned empty, that's definitive.
            # Skip the redundant second query to halve API calls.
            return []
        except Exception as e:
            logger.warning("get_open_orders failed for %s: %s", identifier, e)
            return []

    async def get_order(self, order_id: str) -> Dict:
        """Get a single order by ID via SDK (any status: LIVE, MATCHED, CANCELLED)."""
        def _fetch():
            client = self._get_sdk_client()
            return client.get_order(order_id)
        try:
            result = await self._run_sdk(_fetch)
            return result if isinstance(result, dict) else {}
        except Exception as e:
            logger.warning("get_order failed for %s: %s", order_id, e)
            return {}

    async def get_order_book(self, token_id: str) -> Dict:
        # Check WS cache first for faster response
        if self._ws_feed is not None:
            ws_book = self._ws_feed.get_book(token_id)
            if ws_book is not None:
                logger.debug("WS book cache hit for %s", token_id[:12])
                return ws_book

        async def _fetch():
            resp = await self._http.get("/book", params={"token_id": token_id})
            resp.raise_for_status()
            return resp.json()
        return await _retry_with_backoff(_fetch)

    async def get_balance(self) -> float:
        async def _balance():
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_sdk_client()
            # signature_type=2 for Polymarket smart wallet proxy
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=2,
            )
            result = await self._run_sdk(client.get_balance_allowance, params)
            if isinstance(result, dict):
                # Balance is in USDC atomic units (6 decimals)
                raw = float(result.get("balance", 0))
                return raw / 1e6
            return 0.0
        return await _balance()

    async def get_server_time(self) -> float:
        resp = await self._http.get("/time")
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return float(data.get("time", time.time()))
        return float(data)

    async def get_min_order_size(self, token_id: str) -> float:
        book = await self.get_order_book(token_id)
        return float(book.get("min_order_size", 1.0))

    async def place_orders_batch(self, orders: List[Dict]) -> List[str]:
        """Place multiple GTD limit BUY orders in one API call via SDK post_orders()."""
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import OrderArgs, OrderType

        if not orders:
            return []

        tick_size = 0.01

        async def _place():
            client = self._get_sdk_client()

            # Sign all orders in a single executor call (CPU-bound crypto)
            def _sign_all():
                result = []
                for o in orders:
                    price = clamp_price(o["price"], tick_size)
                    price = round_to_tick(price, tick_size)
                    size = round(o["size"], 2)
                    order_args = OrderArgs(
                        token_id=o["token_id"],
                        price=price,
                        size=size,
                        side=BUY,
                        expiration=o["expiration_ts"],
                    )
                    signed_order = client.create_order(order_args)
                    result.append(signed_order)
                return result

            # Increase timeout for batch signing (14 orders)
            loop = asyncio.get_event_loop()
            signed_orders = await asyncio.wait_for(
                loop.run_in_executor(self._executor, _sign_all),
                timeout=30.0,
            )

            # Build batch args
            from py_clob_client.clob_types import PostOrdersArgs
            batch_args = [
                PostOrdersArgs(order=so, orderType=OrderType.GTD)
                for so in signed_orders
            ]

            # Single API call for all orders
            result = await self._run_sdk(client.post_orders, batch_args)

            # Parse order IDs from response
            order_ids: List[str] = []
            if isinstance(result, list):
                for r in result:
                    if isinstance(r, dict):
                        oid = r.get("orderID", r.get("id", ""))
                        order_ids.append(oid)
                    else:
                        order_ids.append("")
            elif isinstance(result, dict):
                # Single wrapper response
                oid = result.get("orderID", result.get("id", ""))
                order_ids.append(oid)
            else:
                logger.warning("Unexpected batch response type: %s", type(result))

            # Pad to match input length if API returned fewer
            while len(order_ids) < len(orders):
                order_ids.append("")

            placed = sum(1 for oid in order_ids if oid)
            logger.info("Batch placed %d/%d orders", placed, len(orders))
            return order_ids

        return await _retry_with_backoff(_place)

    async def place_taker_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
        *,
        sell: bool = False,
    ) -> str:
        """Place a GTC limit order without post_only (taker-eligible)."""
        from py_clob_client.order_builder.constants import BUY, SELL
        from py_clob_client.clob_types import OrderArgs, OrderType

        tick_size = 0.01
        price = clamp_price(price, tick_size)
        price = round_to_tick(price, tick_size)
        size = round(size, 2)
        sdk_side = SELL if sell else BUY

        async def _place():
            client = self._get_sdk_client()
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=sdk_side,
            )
            signed_order = await self._run_sdk(client.create_order, order_args)
            result = await self._run_sdk(client.post_order, signed_order, OrderType.GTC)
            order_id = result.get("orderID", result.get("id", ""))
            trade_type = "SELL" if sell else "BUY"
            logger.info("Placed taker %s %s: %s %.2f @ %.2f", trade_type, order_id, side, size, price)
            return order_id

        return await _retry_with_backoff(_place)

    async def get_position(self, token_id: str) -> float:
        """Get position size (conditional token balance) from exchange.

        Uses the SDK's get_balance_allowance with AssetType.CONDITIONAL,
        which queries the on-chain token balance. The old /data/positions
        endpoint was non-existent (always 404).

        Fix #54: Both USDC (COLLATERAL) and conditional tokens return
        atomic units (6 decimals). Must divide by 1e6 to get share count.
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_sdk_client()
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=2,
            )
            result = await self._run_sdk(client.get_balance_allowance, params)
            if isinstance(result, dict):
                raw = float(result.get("balance", 0))
                shares = raw / 1e6
                logger.debug("get_position raw balance for %s...: %s (%.4f shares)", token_id[:12], raw, shares)
                return shares
            return 0.0
        except Exception as e:
            logger.debug("get_position failed for %s: %s", token_id, e)
            return float("nan")

    async def ensure_allowances(self, token_ids: list[str] | None = None) -> bool:
        """Ensure both COLLATERAL (USDC) and CONDITIONAL (CTF) allowances are set.

        Calls the Polymarket server-side endpoint that triggers on-chain
        setApprovalForAll / approve for the exchange contracts.
        Must be called before placing SELL orders.
        """
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

        # COLLATERAL (ERC-20 USDC) — no token_id needed
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2)
            result = await self._run_sdk(
                self._get_sdk_client().update_balance_allowance, params
            )
            logger.info("Allowance update (COLLATERAL): %s", result)
        except Exception as e:
            logger.error("Failed to update allowance (COLLATERAL): %s", e)
            return False

        # CONDITIONAL (ERC-1155 CTF) — requires a valid token_id
        for token_id in (token_ids or []):
            try:
                params = BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                    signature_type=2,
                )
                result = await self._run_sdk(
                    self._get_sdk_client().update_balance_allowance, params
                )
                logger.info("Allowance update (CONDITIONAL %s...): %s", token_id[:12], result)
            except Exception as e:
                logger.error("Failed to update allowance (CONDITIONAL %s...): %s", token_id[:12], e)
                return False
        return True

    async def close(self):
        await self._http.aclose()
        self._executor.shutdown(wait=False)


class PaperClient(ExchangeClient):
    """Paper trading client: fake writes, real reads via httpx.

    All write methods return fake UUIDs. No real orders are placed.
    Read methods proxy to the real CLOB API.
    """

    def __init__(self, clob_url: str = "https://clob.polymarket.com"):
        self._http = httpx.AsyncClient(base_url=clob_url, timeout=15.0)
        self._fake_orders: Dict[str, Dict] = {}
        self._fake_balance: float = 10000.0

    async def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
    ) -> str:
        order_id = f"paper-{uuid.uuid4().hex[:12]}"
        self._fake_orders[order_id] = {
            "id": order_id,
            "asset_id": token_id,
            "token_id": token_id,
            "price": price,
            "original_size": size,
            "size": size,
            "size_matched": 0.0,
            "side": side,
            "expiration_ts": expiration_ts,
            "status": "LIVE",
        }
        self._fake_balance -= price * size
        logger.info("PAPER: placed %s %.2f @ %.2f -> %s", side, size, price, order_id)
        return order_id

    async def cancel_order(self, order_id: str) -> bool:
        if order_id in self._fake_orders:
            order = self._fake_orders[order_id]
            if order["status"] == "LIVE":
                self._fake_balance += order["price"] * order["size"]
                order["status"] = "CANCELLED"
            return True
        return False

    async def cancel_orders(self, order_ids: List[str]) -> bool:
        for oid in order_ids:
            await self.cancel_order(oid)
        return True

    async def cancel_all(self) -> bool:
        for oid, order in list(self._fake_orders.items()):
            if order["status"] == "LIVE":
                self._fake_balance += order["price"] * order["size"]
                order["status"] = "CANCELLED"
        return True

    async def cancel_market_orders(self, asset_id: str) -> bool:
        for oid, order in list(self._fake_orders.items()):
            if order["token_id"] == asset_id and order["status"] == "LIVE":
                self._fake_balance += order["price"] * order["size"]
                order["status"] = "CANCELLED"
        return True

    async def get_trades(self, asset_id: str, after_ts: int, maker_address: str = "") -> List[FillEvent]:
        # Paper client has no real fills
        return []

    async def get_open_orders(self, identifier: str) -> List[Dict]:
        # Paper-mode approximation:
        # - identifier="0x..." (condition/market id): return all LIVE orders.
        # - otherwise treat identifier as token/asset id and filter strictly.
        live_orders = [
            o for o in self._fake_orders.values()
            if str(o.get("status", "")).upper() == "LIVE"
        ]
        if not identifier:
            return live_orders
        id_s = str(identifier)
        if id_s.startswith("0x"):
            return live_orders
        if not id_s.isdigit():
            return live_orders
        return [
            o for o in live_orders
            if o.get("token_id") == id_s or o.get("asset_id") == id_s
        ]

    async def get_order(self, order_id: str) -> Dict:
        return self._fake_orders.get(order_id, {})

    async def get_order_book(self, token_id: str) -> Dict:
        resp = await self._http.get("/book", params={"token_id": token_id})
        resp.raise_for_status()
        return resp.json()

    async def get_balance(self) -> float:
        return self._fake_balance

    async def get_server_time(self) -> float:
        return time.time()

    async def get_min_order_size(self, token_id: str) -> float:
        book = await self.get_order_book(token_id)
        return float(book.get("min_order_size", 1.0))

    async def place_orders_batch(self, orders: List[Dict]) -> List[str]:
        """Paper batch: place each order individually."""
        ids = []
        for o in orders:
            oid = await self.place_limit_order(
                token_id=o["token_id"],
                price=o["price"],
                size=o["size"],
                side=o.get("side", "buy"),
                expiration_ts=o["expiration_ts"],
            )
            ids.append(oid)
        return ids

    async def place_taker_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        expiration_ts: int,
        *,
        sell: bool = False,
    ) -> str:
        order_id = f"paper-taker-{uuid.uuid4().hex[:12]}"
        self._fake_orders[order_id] = {
            "id": order_id,
            "asset_id": token_id,
            "token_id": token_id,
            "price": price,
            "original_size": size,
            "size": size,
            "size_matched": size,  # taker fills immediately in paper
            "side": side,
            "expiration_ts": expiration_ts,
            "status": "MATCHED",
        }
        if sell:
            self._fake_balance += price * size
        else:
            self._fake_balance -= price * size
        trade_type = "SELL" if sell else "BUY"
        logger.info("PAPER TAKER %s: placed %s %.2f @ %.2f -> %s", trade_type, side, size, price, order_id)
        return order_id

    async def get_position(self, token_id: str) -> float:
        """Paper client: sum filled orders for this token."""
        total = 0.0
        for o in self._fake_orders.values():
            if o["token_id"] == token_id and o.get("status") == "MATCHED":
                total += float(o.get("size_matched", 0))
        return total

    async def close(self):
        await self._http.aclose()
