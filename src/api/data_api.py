"""Polymarket Data API client for user activity and trades."""

import httpx
from typing import Optional
from datetime import datetime

DATA_API_URL = "https://data-api.polymarket.com"


async def get_user_trades(
    address: str,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Get user's trade history.

    Returns list of trades with:
    - proxyWallet
    - side (BUY/SELL)
    - asset (token ID)
    - conditionId
    - size
    - price
    - fee
    - timestamp
    - transactionHash
    """
    async with httpx.AsyncClient() as client:
        params = {
            "user": address,
            "limit": limit,
            "offset": offset,
        }

        response = await client.get(
            f"{DATA_API_URL}/trades",
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


async def get_user_activity(
    address: str,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Get user's activity feed.

    Returns list of activities with type:
    - TRADE
    - DEPOSIT
    - WITHDRAWAL
    - etc.
    """
    async with httpx.AsyncClient() as client:
        params = {
            "user": address,
            "limit": limit,
            "offset": offset,
        }

        response = await client.get(
            f"{DATA_API_URL}/activity",
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


async def get_user_positions(address: str) -> list[dict]:
    """Get user's current positions.

    Returns list of positions with:
    - proxyWallet
    - asset (token ID)
    - conditionId
    - size
    - avgPrice
    """
    async with httpx.AsyncClient() as client:
        params = {"user": address}

        response = await client.get(
            f"{DATA_API_URL}/positions",
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


def parse_trade(trade: dict) -> dict:
    """Parse a trade into a more usable format."""
    timestamp = trade.get("timestamp", 0)
    if isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp)
    else:
        dt = None

    return {
        "timestamp": dt,
        "side": trade.get("side"),
        "token_id": trade.get("asset"),
        "condition_id": trade.get("conditionId"),
        "size": float(trade.get("size", 0)),
        "price": float(trade.get("price", 0)),
        "fee": float(trade.get("fee", 0)),
        "tx_hash": trade.get("transactionHash"),
        "outcome": trade.get("outcome"),
        "maker": trade.get("maker", False),
    }


async def get_all_user_trades(
    address: str,
    max_trades: int = 1000,
) -> list[dict]:
    """Get all user trades with pagination."""
    all_trades = []
    offset = 0
    batch_size = 100

    while len(all_trades) < max_trades:
        trades = await get_user_trades(address, limit=batch_size, offset=offset)

        if not trades:
            break

        all_trades.extend(trades)
        offset += batch_size

        if len(trades) < batch_size:
            break

    return all_trades[:max_trades]
