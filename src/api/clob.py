"""Polymarket CLOB API client."""

import httpx
import json
from typing import Optional
from datetime import datetime

CLOB_API_URL = "https://clob.polymarket.com"


async def get_orderbook(token_id: str) -> dict:
    """Get orderbook for a specific token.

    Returns bids and asks with prices and sizes.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/book",
            params={"token_id": token_id},
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


async def get_price(token_id: str) -> dict:
    """Get current price for a token."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/price",
            params={"token_id": token_id},
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


async def get_prices(token_ids: list[str]) -> dict:
    """Get prices for multiple tokens at once."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/prices",
            params={"token_ids": ",".join(token_ids)},
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


async def get_midpoint(token_id: str) -> Optional[float]:
    """Get midpoint price for a token."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/midpoint",
            params={"token_id": token_id},
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()
        mid = data.get("mid")
        return float(mid) if mid else None


async def get_spread(token_id: str) -> dict:
    """Get bid-ask spread for a token."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/spread",
            params={"token_id": token_id},
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


async def get_last_trade_price(token_id: str) -> Optional[float]:
    """Get last trade price for a token."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{CLOB_API_URL}/last-trade-price",
            params={"token_id": token_id},
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()
        price = data.get("price")
        return float(price) if price else None


async def get_trades(
    token_id: str,
    limit: int = 100,
    before: Optional[str] = None,
    after: Optional[str] = None,
) -> list[dict]:
    """Get recent trades for a token.

    Note: This endpoint may require authentication for full access.
    """
    async with httpx.AsyncClient() as client:
        params = {
            "token_id": token_id,
            "limit": limit,
        }
        if before:
            params["before"] = before
        if after:
            params["after"] = after

        response = await client.get(
            f"{CLOB_API_URL}/trades",
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


def analyze_orderbook(book: dict) -> dict:
    """Analyze orderbook for trading opportunities.

    Returns metrics like spread, depth, and imbalance.
    """
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    if not bids or not asks:
        return {
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "mid": None,
        }

    best_bid = float(bids[0]["price"])
    best_ask = float(asks[0]["price"])
    spread = best_ask - best_bid
    mid = (best_bid + best_ask) / 2

    # Calculate depth at top 5 levels
    bid_depth = sum(float(b["size"]) for b in bids[:5])
    ask_depth = sum(float(a["size"]) for a in asks[:5])

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "mid": mid,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "imbalance": (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0,
    }
