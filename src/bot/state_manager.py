"""State manager: atomic persistence, file lock, crash recovery.

Uses atomic write (write .tmp -> os.replace) for crash safety.
File lock (fcntl.flock) prevents double-start.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Optional

from src.bot.types import (
    BotConfig, BotState, MarketInfo, MarketPhase, PositionState,
)

logger = logging.getLogger(__name__)


class StateManager:
    """Manages bot state persistence and crash recovery."""

    def __init__(self, state_dir: str = "state"):
        self._state_dir = Path(state_dir).resolve()
        self._state_file = self._state_dir / "bot_state.json"
        self._backup_file = self._state_dir / "bot_state.backup.json"
        self._lock_file = self._state_dir / "bot.lock"
        self._results_dir = Path("results").resolve()
        self._lock_fd = None

        # Create directories
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._results_dir.mkdir(parents=True, exist_ok=True)

        # Clean up any .tmp files from previous crash
        for tmp in self._state_dir.glob("*.tmp"):
            tmp.unlink()
            logger.info("Cleaned up stale tmp file: %s", tmp)

    def acquire_lock(self) -> bool:
        """Acquire file lock to prevent double-start. Returns False if already locked."""
        try:
            self._lock_fd = open(self._lock_file, "w")
            fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_fd.write(str(os.getpid()))
            self._lock_fd.flush()
            logger.info("Lock acquired (pid=%d)", os.getpid())
            return True
        except (IOError, OSError):
            logger.error("Lock already held -- another instance running?")
            if self._lock_fd:
                self._lock_fd.close()
                self._lock_fd = None
            return False

    def release_lock(self) -> None:
        """Release file lock."""
        if self._lock_fd:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
                self._lock_fd.close()
            except Exception:
                pass
            self._lock_fd = None
            logger.info("Lock released")

    def save_state(self, state: BotState) -> None:
        """Atomically save state to disk."""
        data = self._serialize_state(state)
        data["_saved_at"] = time.time()

        tmp_file = self._state_file.with_suffix(".tmp")

        # Backup current state first
        if self._state_file.exists():
            shutil.copy2(self._state_file, self._backup_file)

        # Atomic write: .tmp -> fsync -> rename
        with open(tmp_file, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

        try:
            os.replace(tmp_file, self._state_file)
        except FileNotFoundError:
            # Rare race: tmp file not yet visible after fsync.
            # Retry once after a brief pause.
            time.sleep(0.05)
            try:
                os.replace(tmp_file, self._state_file)
            except FileNotFoundError:
                logger.warning("save_state: tmp file vanished after fsync — skipping this save")
                return

    def load_state(self) -> Optional[BotState]:
        """Load state from disk. Falls back to backup if main is corrupt."""
        # Try main file
        state = self._try_load(self._state_file)
        if state:
            return state

        # Try backup
        logger.warning("Main state file failed, trying backup")
        state = self._try_load(self._backup_file)
        if state:
            return state

        logger.info("No saved state found -- fresh start")
        return None

    def save_session_result(self, result: dict) -> None:
        """Append session result to results/sessions.jsonl."""
        result["_saved_at"] = time.time()
        results_file = self._results_dir / "sessions.jsonl"
        with open(results_file, "a") as f:
            f.write(json.dumps(result) + "\n")

    def _serialize_state(self, state: BotState) -> dict:
        """Convert BotState to JSON-safe dict."""
        data = {
            "phase": state.phase.value,
            "daily_pnl": state.daily_pnl,
            "hourly_pnl": state.hourly_pnl,
            "session_worst_case_pnl": state.session_worst_case_pnl,
            "reserved_notional": state.reserved_notional,
            "total_trades": state.total_trades,
            "session_id": state.session_id,
            "position": {
                "up_shares": state.position.up_shares,
                "down_shares": state.position.down_shares,
                "up_cost": state.position.up_cost,
                "down_cost": state.position.down_cost,
            },
        }
        if state.market:
            data["market"] = {
                "condition_id": state.market.condition_id,
                "up_token": state.market.up_token,
                "down_token": state.market.down_token,
                "open_ts": state.market.open_ts,
                "close_ts": state.market.close_ts,
                "tick_size": state.market.tick_size,
                "min_order_size": state.market.min_order_size,
            }
        # Note: active_orders and order_map are NOT persisted
        # (they're rebuilt via reconciliation on recovery)
        return data

    def _deserialize_state(self, data: dict) -> BotState:
        """Reconstruct BotState from dict."""
        pos_data = data.get("position", {})
        position = PositionState(
            up_shares=pos_data.get("up_shares", 0.0),
            down_shares=pos_data.get("down_shares", 0.0),
            up_cost=pos_data.get("up_cost", 0.0),
            down_cost=pos_data.get("down_cost", 0.0),
        )

        market = None
        if "market" in data and data["market"]:
            m = data["market"]
            market = MarketInfo(
                condition_id=m["condition_id"],
                up_token=m["up_token"],
                down_token=m["down_token"],
                open_ts=m["open_ts"],
                close_ts=m["close_ts"],
                tick_size=m["tick_size"],
                min_order_size=m.get("min_order_size", 1.0),
            )

        return BotState(
            phase=MarketPhase(data.get("phase", "waiting")),
            market=market,
            position=position,
            daily_pnl=data.get("daily_pnl", 0.0),
            hourly_pnl=data.get("hourly_pnl", 0.0),
            session_worst_case_pnl=data.get("session_worst_case_pnl", 0.0),
            reserved_notional=data.get("reserved_notional", 0.0),
            total_trades=data.get("total_trades", 0),
            session_id=data.get("session_id", ""),
        )

    def _try_load(self, path: Path) -> Optional[BotState]:
        """Try to load and deserialize state from a file."""
        if not path.exists():
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            return self._deserialize_state(data)
        except Exception as e:
            logger.error("Failed to load %s: %s", path, e)
            return None
