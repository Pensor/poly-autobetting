"""Helpers for robust trader activity watching from Data API activity feed."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable


def _to_float(value: Any) -> float:
    """Best-effort float conversion."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_unix_ts(value: Any) -> int:
    """Convert timestamp field to unix seconds."""
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return 0
    if ts > 10_000_000_000:
        # Milliseconds input.
        ts //= 1000
    return ts


def make_trade_key(activity: dict[str, Any]) -> str:
    """Build a stable key for one activity record.

    Data API activity does not always provide a unique id, so we combine
    transaction hash + trade attributes.
    """
    tx_hash = str(activity.get("transactionHash") or "")
    asset = str(activity.get("asset") or "")
    side = str(activity.get("side") or "")
    timestamp = _to_unix_ts(activity.get("timestamp"))
    size = _to_float(activity.get("size"))
    price = _to_float(activity.get("price"))
    outcome = str(activity.get("outcome") or "")
    slug = str(activity.get("slug") or activity.get("eventSlug") or "")
    return (
        f"{tx_hash}|{asset}|{side}|{timestamp}|"
        f"{size:.8f}|{price:.8f}|{outcome}|{slug}"
    )


def normalize_trade_activity(activity: dict[str, Any]) -> dict[str, Any]:
    """Normalize one TRADE activity row for downstream analysis."""
    ts = _to_unix_ts(activity.get("timestamp"))
    size = _to_float(activity.get("size"))
    price = _to_float(activity.get("price"))
    usdc_size = _to_float(activity.get("usdcSize"))
    return {
        "event_key": make_trade_key(activity),
        "timestamp": ts,
        "datetime": datetime.fromtimestamp(ts).isoformat() if ts > 0 else None,
        "transaction_hash": activity.get("transactionHash", ""),
        "condition_id": activity.get("conditionId", ""),
        "asset": activity.get("asset", ""),
        "outcome": activity.get("outcome", ""),
        "side": activity.get("side", ""),
        "price": price,
        "size": size,
        "usdc_size": usdc_size,
        "notional": usdc_size if usdc_size > 0 else size * price,
        "title": activity.get("title", ""),
        "slug": activity.get("slug", activity.get("eventSlug", "")),
        "type": activity.get("type", ""),
        "raw": activity,
    }


@dataclass
class SeenTradeWindow:
    """Bounded dedupe window for trade keys."""

    max_size: int = 50000
    _keys: set[str] = field(default_factory=set)
    _order: deque[str] = field(default_factory=deque)

    def __contains__(self, key: str) -> bool:
        return key in self._keys

    def __len__(self) -> int:
        return len(self._keys)

    def add(self, key: str) -> bool:
        """Add key, evict oldest keys if capacity reached."""
        if not key:
            return False
        if key in self._keys:
            return False
        self._keys.add(key)
        self._order.append(key)
        while len(self._order) > self.max_size:
            old_key = self._order.popleft()
            self._keys.discard(old_key)
        return True

    def update(self, keys: Iterable[str]) -> int:
        """Add many keys. Returns number of newly inserted keys."""
        added = 0
        for key in keys:
            if self.add(key):
                added += 1
        return added


def seed_trade_window(activities: Iterable[dict[str, Any]], window: SeenTradeWindow) -> int:
    """Seed dedupe window with existing TRADE events."""
    keys: list[str] = []
    for activity in activities:
        if activity.get("type") != "TRADE":
            continue
        keys.append(make_trade_key(activity))
    return window.update(keys)


def select_new_trade_activities(
    activities: Iterable[dict[str, Any]],
    window: SeenTradeWindow,
) -> list[dict[str, Any]]:
    """Return unseen TRADE activities, sorted by timestamp."""
    new_events: list[dict[str, Any]] = []
    in_batch: set[str] = set()

    for activity in activities:
        if activity.get("type") != "TRADE":
            continue
        normalized = normalize_trade_activity(activity)
        key = normalized["event_key"]
        if key in in_batch or key in window:
            continue
        in_batch.add(key)
        new_events.append(normalized)

    new_events.sort(key=lambda x: (x["timestamp"], x["event_key"]))
    window.update(event["event_key"] for event in new_events)
    return new_events
