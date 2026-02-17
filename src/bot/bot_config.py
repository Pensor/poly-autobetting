"""Bot configuration loader.

Loads bot parameters from environment variables with sensible defaults.
Supports CLI overrides for tiny test mode (--shares 1 --capital 5).
Does NOT modify existing src/config.py.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

from src.bot.types import BotConfig


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_bot_config(overrides: dict | None = None) -> BotConfig:
    """Load bot configuration from environment with optional overrides.

    Environment variables are prefixed with BOT_ (e.g., BOT_SESSION_CAPITAL_LIMIT).
    CLI overrides take precedence over env vars.

    Args:
        overrides: Dict of field_name -> value to override defaults/env.
                   Typical usage: {"session_capital_limit": 5, "shares_per_order": 1}

    Returns:
        BotConfig with all parameters resolved.
    """
    load_dotenv()

    config = BotConfig(
        session_capital_limit=float(os.getenv("BOT_SESSION_CAPITAL_LIMIT", "300.0")),
        shares_per_order=float(os.getenv("BOT_SHARES_PER_ORDER", "10.0")),
        order_delay_ms=int(os.getenv("BOT_ORDER_DELAY_MS", "300")),
        max_replenishments=int(os.getenv("BOT_MAX_REPLENISHMENTS", "3")),
        entry_delay_s=float(os.getenv("BOT_ENTRY_DELAY_S", "0.0")),
        max_trades=int(os.getenv("BOT_MAX_TRADES", "350")),
        winding_down_buffer_s=int(os.getenv("BOT_WINDING_DOWN_BUFFER_S", "15")),
        heartbeat_interval=int(os.getenv("BOT_HEARTBEAT_INTERVAL", "15")),
        daily_loss_limit=float(os.getenv("BOT_DAILY_LOSS_LIMIT", "200.0")),
        hourly_loss_limit=float(os.getenv("BOT_HOURLY_LOSS_LIMIT", "100.0")),
        worst_case_pnl_limit=float(os.getenv("BOT_WORST_CASE_PNL_LIMIT", "-75.0")),
        unhedged_exposure_limit=float(os.getenv("BOT_UNHEDGED_EXPOSURE_LIMIT", "100.0")),
        imbalance_threshold=float(os.getenv("BOT_IMBALANCE_THRESHOLD", "0.15")),
        max_daily_markets=int(os.getenv("BOT_MAX_DAILY_MARKETS", "50")),
        max_run_sessions=int(os.getenv("BOT_MAX_RUN_SESSIONS", "0")),
        max_run_minutes=float(os.getenv("BOT_MAX_RUN_MINUTES", "0.0")),
        no_edge_cooldown_s=float(os.getenv("BOT_NO_EDGE_COOLDOWN_S", "20.0")),
        fill_poll_interval_s=float(os.getenv("BOT_FILL_POLL_INTERVAL_S", "1.0")),
        monitor_loop_interval_s=float(os.getenv("BOT_MONITOR_LOOP_INTERVAL_S", "2.0")),
        reanchor_check_interval_s=float(os.getenv("BOT_REANCHOR_CHECK_INTERVAL_S", "30.0")),
        reanchor_drift_threshold=float(os.getenv("BOT_REANCHOR_DRIFT_THRESHOLD", "0.02")),
        reanchor_min_time_left_s=float(os.getenv("BOT_REANCHOR_MIN_TIME_LEFT_S", "90.0")),
        reanchor_min_action_cooldown_s=float(os.getenv("BOT_REANCHOR_MIN_ACTION_COOLDOWN_S", "4.0")),
        reanchor_side_fill_grace_s=float(os.getenv("BOT_REANCHOR_SIDE_FILL_GRACE_S", "2.0")),
        reanchor_out_of_range_ratio=float(os.getenv("BOT_REANCHOR_OUT_OF_RANGE_RATIO", "0.35")),
        reanchor_min_edge_gain=float(os.getenv("BOT_REANCHOR_MIN_EDGE_GAIN", "0.003")),
        reanchor_worse_price_imbalance_emergency=float(os.getenv("BOT_REANCHOR_WORSE_PRICE_IMBALANCE_EMERGENCY", "0.35")),
        entry_max_expected_combined=float(os.getenv("BOT_ENTRY_MAX_EXPECTED_COMBINED", "1.005")),
        rebalance_soft_imbalance=float(os.getenv("BOT_REBALANCE_SOFT_IMBALANCE", "0.08")),
        rebalance_hard_imbalance=float(os.getenv("BOT_REBALANCE_HARD_IMBALANCE", "0.15")),
        rebalance_min_total_shares=float(os.getenv("BOT_REBALANCE_MIN_TOTAL_SHARES", "20.0")),
        rebalance_cancel_cooldown_s=float(os.getenv("BOT_REBALANCE_CANCEL_COOLDOWN_S", "5.0")),
        max_combined_vwap=float(os.getenv("BOT_MAX_COMBINED_VWAP", "0.99")),
        vwap_stop_adding_threshold=float(os.getenv("BOT_VWAP_STOP_ADDING_THRESHOLD", "0.98")),
        taker_flatten_imbalance=float(os.getenv("BOT_TAKER_FLATTEN_IMBALANCE", "0.15")),
        taker_flatten_max_notional_pct=float(os.getenv("BOT_TAKER_FLATTEN_MAX_NOTIONAL_PCT", "0.20")),
        per_side_cost_cap_pct=float(os.getenv("BOT_PER_SIDE_COST_CAP_PCT", "0.60")),
        rebalance_max_levels=int(os.getenv("BOT_REBALANCE_MAX_LEVELS", "5")),
        hot_side_fill_ratio=float(os.getenv("BOT_HOT_SIDE_FILL_RATIO", "0.50")),
        hot_side_cooldown_s=float(os.getenv("BOT_HOT_SIDE_COOLDOWN_S", "10.0")),
        cancel_all_noop_escalation=int(os.getenv("BOT_CANCEL_ALL_NOOP_ESCALATION", "3")),
        smart_cancel_enabled=_env_bool("BOT_SMART_CANCEL_ENABLED", False),
        position_reconcile_interval_s=float(os.getenv("BOT_POSITION_RECONCILE_INTERVAL_S", "5.0")),
        position_reconcile_tolerance_shares=float(
            os.getenv("BOT_POSITION_RECONCILE_TOLERANCE_SHARES", "0.1")
        ),
        position_reconcile_tolerance_cost=float(
            os.getenv("BOT_POSITION_RECONCILE_TOLERANCE_COST", "0.25")
        ),
        taker_rebalance_enabled=_env_bool("BOT_TAKER_REBALANCE_ENABLED", False),
        taker_rebalance_cooldown_s=float(os.getenv("BOT_TAKER_REBALANCE_COOLDOWN_S", "10.0")),
        taker_rebalance_min_total_shares=float(
            os.getenv("BOT_TAKER_REBALANCE_MIN_TOTAL_SHARES", "10.0")
        ),
        entry_min_excess_tolerance=int(os.getenv("BOT_ENTRY_MIN_EXCESS_TOLERANCE", "1")),
        strategy=os.getenv("BOT_STRATEGY", "active"),
        passive_step_cents=int(os.getenv("BOT_PASSIVE_STEP_CENTS", "1")),
        passive_floor_cents=int(os.getenv("BOT_PASSIVE_FLOOR_CENTS", "2")),
        passive_expand_interval_s=float(os.getenv("BOT_PASSIVE_EXPAND_INTERVAL_S", "1.0")),
        passive_replenish_cap=int(os.getenv("BOT_PASSIVE_REPLENISH_CAP", "999")),
        passive_max_depth_cents=int(os.getenv("BOT_PASSIVE_MAX_DEPTH_CENTS", "0")),
        passive_expansion_enabled=_env_bool("BOT_PASSIVE_EXPANSION_ENABLED", True),
        passive_expand_max_cents=int(os.getenv("BOT_PASSIVE_EXPAND_MAX_CENTS", "95")),
        passive_expand_vwap_cap=float(os.getenv("BOT_PASSIVE_EXPAND_VWAP_CAP", "0.970")),
        passive_profit_lock_threshold=float(
            os.getenv("BOT_PASSIVE_PROFIT_LOCK_THRESHOLD", "50.0")
        ),
        passive_profit_lock_unlock_buffer=float(
            os.getenv("BOT_PASSIVE_PROFIT_LOCK_UNLOCK_BUFFER", "-3.0")
        ),
        passive_profit_lock_min_shares=float(
            os.getenv("BOT_PASSIVE_PROFIT_LOCK_MIN_SHARES", "50.0")
        ),
        passive_max_buy_price_cents=int(
            os.getenv("BOT_PASSIVE_MAX_BUY_PRICE_CENTS", "50")
        ),
        passive_avg_cost_target=float(
            os.getenv("BOT_PASSIVE_AVG_COST_TARGET", "0.498")
        ),
        passive_imbalance_cap=float(
            os.getenv("BOT_PASSIVE_IMBALANCE_CAP", "0.60")
        ),
        passive_one_sided_abort_s=float(
            os.getenv("BOT_PASSIVE_ONE_SIDED_ABORT_S", "5.0")
        ),
        passive_one_sided_min_fills=int(
            os.getenv("BOT_PASSIVE_ONE_SIDED_MIN_FILLS", "3")
        ),
        trend_levels_per_side=int(os.getenv("BOT_TREND_LEVELS_PER_SIDE", "7")),
        trend_step_cents=int(os.getenv("BOT_TREND_STEP_CENTS", "1")),
        trend_max_buy_price_cents=int(os.getenv("BOT_TREND_MAX_BUY_PRICE_CENTS", "58")),
        trend_avg_cost_target=float(os.getenv("BOT_TREND_AVG_COST_TARGET", "0.498")),
        trend_reanchor_drift=float(os.getenv("BOT_TREND_REANCHOR_DRIFT", "0.03")),
        trend_reanchor_cooldown_s=float(os.getenv("BOT_TREND_REANCHOR_COOLDOWN_S", "5.0")),
        trend_reanchor_check_interval_s=float(os.getenv("BOT_TREND_REANCHOR_CHECK_INTERVAL_S", "1.0")),
        trend_fill_grace_s=float(os.getenv("BOT_TREND_FILL_GRACE_S", "1.5")),
        trend_replenish_cap=int(os.getenv("BOT_TREND_REPLENISH_CAP", "999")),
        trend_ev_lock_threshold=float(os.getenv("BOT_TREND_EV_LOCK_THRESHOLD", "0.0")),
        trend_order_delay_ms=int(os.getenv("BOT_TREND_ORDER_DELAY_MS", "80")),
        trend_max_shares_per_side=int(os.getenv("BOT_TREND_MAX_SHARES_PER_SIDE", "0")),
        trend_balance_ratio=float(os.getenv("BOT_TREND_BALANCE_RATIO", "1.5")),
        trend_worst_lock_enabled=_env_bool("BOT_TREND_WORST_LOCK_ENABLED", False),
        trend_worst_lock_threshold=float(os.getenv("BOT_TREND_WORST_LOCK_THRESHOLD", "5.0")),
        trend_worst_lock_unlock=float(os.getenv("BOT_TREND_WORST_LOCK_UNLOCK", "-5.0")),
        trend_taker_entry_enabled=_env_bool("BOT_TREND_TAKER_ENTRY_ENABLED", False),
        trend_taker_entry_orders=int(os.getenv("BOT_TREND_TAKER_ENTRY_ORDERS", "3")),
        trend_taker_entry_shares=float(os.getenv("BOT_TREND_TAKER_ENTRY_SHARES", "0.0")),
        trend_taker_rebalance_enabled=_env_bool("BOT_TREND_TAKER_REBALANCE_ENABLED", False),
        trend_taker_rebalance_deficit=int(os.getenv("BOT_TREND_TAKER_REBALANCE_DEFICIT", "10")),
        trend_taker_rebalance_shares=int(os.getenv("BOT_TREND_TAKER_REBALANCE_SHARES", "0")),
        trend_taker_rebalance_cooldown_s=float(os.getenv("BOT_TREND_TAKER_REBALANCE_COOLDOWN_S", "5.0")),
        trend_taker_budget_pct=float(os.getenv("BOT_TREND_TAKER_BUDGET_PCT", "0.25")),
        hot_side_rolling_window_s=float(os.getenv("BOT_HOT_SIDE_ROLLING_WINDOW_S", "15.0")),
        hot_side_rolling_threshold=int(os.getenv("BOT_HOT_SIDE_ROLLING_THRESHOLD", "8")),
        trend_profit_protect_threshold=float(os.getenv("BOT_TREND_PROFIT_PROTECT_THRESHOLD", "20.0")),
        trend_taker_pair_premium_per_dollar=float(os.getenv("BOT_TREND_TAKER_PAIR_PREMIUM_PER_DOLLAR", "0.005")),
    )

    # Apply CLI overrides
    if overrides:
        for key, value in overrides.items():
            if hasattr(config, key):
                # Convert to the correct type based on default
                default_val = getattr(config, key)
                setattr(config, key, type(default_val)(value))

    # Clamp: imbalance-based thresholds must be ≤ 0.50 (share_imbalance = excess/total ≤ 0.50)
    config.reanchor_worse_price_imbalance_emergency = min(
        config.reanchor_worse_price_imbalance_emergency, 0.50
    )

    return config


# Default values for auto-scaling detection.
_DEFAULT_WORST_CASE_PNL_LIMIT = -100.0
_DEFAULT_UNHEDGED_EXPOSURE_LIMIT = 100.0
_DEFAULT_DAILY_LOSS_LIMIT = 200.0
_DEFAULT_BASE_CAPITAL = 300.0


def auto_scale_thresholds(config: BotConfig) -> BotConfig:
    """Auto-scale risk thresholds when session_capital_limit differs from $300.

    Only scales values that are still at their defaults (don't override explicit settings).
    """
    ratio = config.session_capital_limit / _DEFAULT_BASE_CAPITAL
    if ratio == 1.0:
        return config
    if config.worst_case_pnl_limit == _DEFAULT_WORST_CASE_PNL_LIMIT:
        config.worst_case_pnl_limit = round(_DEFAULT_WORST_CASE_PNL_LIMIT * ratio, 2)
    if config.unhedged_exposure_limit == _DEFAULT_UNHEDGED_EXPOSURE_LIMIT:
        config.unhedged_exposure_limit = round(_DEFAULT_UNHEDGED_EXPOSURE_LIMIT * ratio, 2)
    if config.daily_loss_limit == _DEFAULT_DAILY_LOSS_LIMIT:
        config.daily_loss_limit = round(_DEFAULT_DAILY_LOSS_LIMIT * ratio, 2)
    return config
