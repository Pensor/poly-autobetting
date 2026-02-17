"""Polygonscan API client for on-chain data."""

import httpx
from typing import Optional
from datetime import datetime

# Use V2 API
POLYGONSCAN_API_URL = "https://api.polygonscan.com/v2/api"

# Conditional Tokens Framework contract
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Chain ID for Polygon
POLYGON_CHAIN_ID = 137


async def get_token_transfers(
    address: str,
    api_key: str = "",
    contract: str = CTF_CONTRACT,
    start_block: int = 0,
    end_block: int = 99999999,
    page: int = 1,
    offset: int = 100,
    sort: str = "desc"
) -> list[dict]:
    """Get ERC-1155 token transfers for an address.

    This fetches conditional token transfers which represent
    Polymarket position changes.
    """
    async with httpx.AsyncClient() as client:
        params = {
            "chainid": POLYGON_CHAIN_ID,
            "module": "account",
            "action": "token1155tx",
            "address": address,
            "contractaddress": contract,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": sort,
        }

        if api_key:
            params["apikey"] = api_key

        response = await client.get(
            POLYGONSCAN_API_URL,
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1":
            # No results or error
            return []

        return data.get("result", [])


async def get_normal_transactions(
    address: str,
    api_key: str = "",
    start_block: int = 0,
    end_block: int = 99999999,
    page: int = 1,
    offset: int = 100,
    sort: str = "desc"
) -> list[dict]:
    """Get normal transactions for an address."""
    async with httpx.AsyncClient() as client:
        params = {
            "chainid": POLYGON_CHAIN_ID,
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": sort,
        }

        if api_key:
            params["apikey"] = api_key

        response = await client.get(
            POLYGONSCAN_API_URL,
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "1":
            return []

        return data.get("result", [])


def parse_transfer(transfer: dict) -> dict:
    """Parse a token transfer into a more usable format."""
    return {
        "hash": transfer.get("hash"),
        "block": int(transfer.get("blockNumber", 0)),
        "timestamp": datetime.fromtimestamp(int(transfer.get("timeStamp", 0))),
        "from": transfer.get("from", "").lower(),
        "to": transfer.get("to", "").lower(),
        "token_id": transfer.get("tokenID"),
        "value": int(transfer.get("tokenValue", 0)),
        "token_name": transfer.get("tokenName"),
    }


async def get_latest_block() -> int:
    """Get the latest block number."""
    async with httpx.AsyncClient() as client:
        params = {
            "chainid": POLYGON_CHAIN_ID,
            "module": "proxy",
            "action": "eth_blockNumber",
        }
        response = await client.get(
            POLYGONSCAN_API_URL,
            params=params,
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()
        return int(data.get("result", "0x0"), 16)
