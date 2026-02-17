"""Configuration for Polymarket tracker."""

import os
from dotenv import load_dotenv

load_dotenv()

# Target trader
TARGET_TRADER_ADDRESS = os.getenv(
    "TARGET_TRADER_ADDRESS",
    "0x818f214c7f3e479cce1d964d53fe3db7297558cb"
)

# API Keys
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY", "")
POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
POLYMARKET_PASSPHRASE = os.getenv("POLYMARKET_PASSPHRASE", "")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Polymarket endpoints
POLYMARKET_CLOB_URL = "https://clob.polymarket.com"
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Polygonscan
POLYGONSCAN_API_URL = "https://api.polygonscan.com/api"

# Conditional Tokens contract on Polygon
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Fee structure
TAKER_FEE_RATE = 0.025  # 2.5% base
MAKER_REBATE_RATE = 0.20  # 20% of taker fees

def calculate_taker_fee(shares: float, price: float) -> float:
    """Calculate taker fee for a trade.

    Formula: fee = shares * fee_rate * price * (1 - price)
    Where fee_rate caps at 2% effective
    """
    p = price
    fee_factor = p * (1 - p)  # Max at p=0.5 (0.25)
    fee = shares * 0.02 * fee_factor * 4  # Normalized to ~2% max
    return fee

def calculate_maker_rebate(shares: float, price: float) -> float:
    """Calculate maker rebate (20% of what taker would pay)."""
    taker_fee = calculate_taker_fee(shares, price)
    return taker_fee * MAKER_REBATE_RATE
