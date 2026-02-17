"""Gamma API client for fetching Polymarket markets."""

import httpx
import json
from typing import Optional, Union
from datetime import datetime, timezone

GAMMA_API_URL = "https://gamma-api.polymarket.com"


def _parse_json_field(value: Union[str, list]) -> list:
    """Parse a field that might be a JSON string or already a list."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


async def get_btc_15min_markets(limit: int = 20) -> list[dict]:
    """Fetch active 15-minute BTC prediction markets.

    Returns markets sorted by end_date (soonest first).
    """
    async with httpx.AsyncClient() as client:
        # Search for 15-minute BTC markets
        params = {
            "limit": limit,
            "active": "true",
            "closed": "false",
            "order": "endDate",
            "ascending": "true",
        }

        response = await client.get(
            f"{GAMMA_API_URL}/markets",
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        markets = response.json()

        # Filter for 15-minute BTC markets
        btc_15min = []
        for market in markets:
            question = market.get("question", "").lower()
            # Look for patterns like "Bitcoin above $X at Y:ZZ"
            if ("bitcoin" in question or "btc" in question) and (
                "15 min" in question or
                ":00" in question or ":15" in question or
                ":30" in question or ":45" in question
            ):
                btc_15min.append(market)

        return btc_15min


async def get_market_by_id(market_id: str) -> Optional[dict]:
    """Fetch a specific market by ID."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{GAMMA_API_URL}/markets/{market_id}",
            timeout=30.0
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()


async def get_market_by_condition_id(condition_id: str) -> Optional[dict]:
    """Fetch market by condition ID."""
    async with httpx.AsyncClient() as client:
        params = {"condition_id": condition_id}
        response = await client.get(
            f"{GAMMA_API_URL}/markets",
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        markets = response.json()
        return markets[0] if markets else None


async def search_markets(query: str, limit: int = 10) -> list[dict]:
    """Search markets by query string."""
    async with httpx.AsyncClient() as client:
        params = {
            "limit": limit,
            "_q": query,
            "active": "true",
        }
        response = await client.get(
            f"{GAMMA_API_URL}/markets",
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()


def parse_market_outcomes(market: dict) -> dict:
    """Parse market outcomes (Up/Down or Yes/No).

    Returns dict with outcome token IDs and current prices.
    Note: API returns these fields as JSON strings, not arrays.
    """
    outcomes = _parse_json_field(market.get("outcomes", []))
    outcome_prices = _parse_json_field(market.get("outcomePrices", []))
    tokens = _parse_json_field(market.get("clobTokenIds", []))

    result = {}
    for i, outcome in enumerate(outcomes):
        price = None
        if i < len(outcome_prices):
            try:
                price = float(outcome_prices[i])
            except (ValueError, TypeError):
                pass

        result[outcome] = {
            "token_id": tokens[i] if i < len(tokens) else None,
            "price": price,
        }

    return result
