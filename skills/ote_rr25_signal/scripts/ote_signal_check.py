#!/usr/bin/env python3
"""
OTE RR25 Signal Checker - 50K Challenge Edition
RR ≥ 2.5 (fixed) | Risk ≤ $80 | $100 daily kill-switch | MNQ only

正確的 OTE 50K Challenge 計算方式：
1. Entry = Zone 內實際成交價 (0.705-0.79)
2. Risk = Entry - Swing Low (LONG) 或 Swing High - Entry (SHORT)
3. Target = Entry + (2.5 × Risk) — 固定 RR 2.5
4. Partial @ fib -0.5 extension
5. Runner @ fib -1.0 extension
"""
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

WORKSPACE = Path(os.environ.get("WORKSPACE", "/home/node/.openclaw/workspace"))
ARTIFACTS = WORKSPACE / "artifacts"
PNL_FILE = ARTIFACTS / "daily_pnl.json"
SIGNAL_LOG = ARTIFACTS / "ote_signals.json"
TEST_LOG = ARTIFACTS / "ote_test_log.json"

TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8475453959")

# === CONSTANTS ===
KILL_SWITCH = 90
MAX_SIGNALS_PER_DAY = 2
MIN_RR = 2.5
MAX_RISK_USD = 80
OTE_ZONE_LOW = 0.705
OTE_ZONE_HIGH = 0.79
FIB_705 = 0.705
FIB_79 = 0.79

# MNQ contract specs: $2 per tick, 0.25 tick = $0.50/tick
PRICE_PER_TICK = 2.0
TICK_SIZE = 0.25


def load_pnl():
    if not PNL_FILE.exists():
        return {"date": str(date.today()), "total_pnl": 0, "signal_count": 0, "signals": []}
    try:
        with open(PNL_FILE) as f:
            data = json.load(f)
        if data.get("date") != str(date.today()):
            return {"date": str(date.today()), "total_pnl": 0, "signal_count": 0, "signals": []}
        return data
    except Exception:
        return {"date": str(date.today()), "total_pnl": 0, "signal_count": 0, "signals": []}


def save_pnl(data):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with open(PNL_FILE, "w") as f:
        json.dump(data, f, indent=2)


def log_signal(signal, is_test=False):
    target = TEST_LOG if is_test else SIGNAL_LOG
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    logs = []
    if target.exists():
        try:
            with open(target) as f:
                logs = json.load(f)
        except Exception:
            pass
    logs.append(signal)
    with open(target, "w") as f:
        json.dump(logs, f, indent=2)


def check_kill_switch(pnl_data):
    return pnl_data.get("total_pnl", 0) <= -KILL_SWITCH


def check_signal_limit(pnl_data):
    return pnl_data.get("signal_count", 0) >= MAX_SIGNALS_PER_DAY


def infer_daily_bias(close, swing_low, swing_high):
    """Infer daily bias from price position relative to recent range."""
    # Simple heuristic: if close is in upper 50% of swing range, bullish
    if swing_high == swing_low:
        return "bullish" if close > swing_low else "bearish"
    midpoint = (swing_low + swing_high) / 2
    return "bullish" if close > midpoint else "bearish"


def in_ote_zone(close, swing_low, swing_high):
    """Check if close is within 0.705-0.79 zone of the fib range."""
    if swing_high == swing_low:
        return False
    range_size = swing_high - swing_low
    zone_low = swing_low + range_size * FIB_705
    zone_high = swing_low + range_size * FIB_79
    return zone_low <= close <= zone_high


def calc_fib_extension(swing_low, swing_high, direction, extension_mult):
    """Calculate fib extension target from swing range.
    extension_mult: -0.5 for partial, -1.0 for runner
    """
    range_size = swing_high - swing_low
    if direction == "LONG":
        return swing_low - (range_size * abs(extension_mult))
    else:
        return swing_high + (range_size * abs(extension_mult))


def check_ote_signal(market_data):
    """
    Returns signal dict if valid, else None.

    market_data keys:
      - symbol: "MNQ"
      - close: current price
      - swing_low: recent swing low value
      - swing_high: recent swing high value
      - time: timestamp string
      - daily_bias (optional): "bullish"/"bearish", inferred if missing
      - ote_zone_hit (optional): bool, inferred from close position if missing
      - ltf_rejection (optional): bool, defaults to False
      - mss_confirmed (optional): bool, defaults to False
      - kill_zone (optional): bool, defaults to False
    """
    pnl = load_pnl()

    # Kill switch
    if check_kill_switch(pnl):
        return None

    # Signal count limit
    if check_signal_limit(pnl):
        return None

    # Required fields
    if "close" not in market_data or "swing_low" not in market_data or "swing_high" not in market_data:
        return None

    close = market_data["close"]
    swing_low = market_data["swing_low"]
    swing_high = market_data["swing_high"]

    # Infer daily bias if not provided
    daily_bias = market_data.get("daily_bias")
    if not daily_bias:
        daily_bias = infer_daily_bias(close, swing_low, swing_high)

    if daily_bias not in ["bullish", "bearish"]:
        return None

    # Map bullish/bearish to LONG/SHORT for internal logic
    trade_direction = "LONG" if daily_bias == "bullish" else "SHORT"

    # Infer OTE zone hit if not provided
    ote_zone_hit = market_data.get("ote_zone_hit")
    if ote_zone_hit is None:
        ote_zone_hit = in_ote_zone(close, swing_low, swing_high)

    if not ote_zone_hit:
        return None

    # Optional conditions (default to False for stricter validation)
    ltf_rejection = market_data.get("ltf_rejection", False)
    mss_confirmed = market_data.get("mss_confirmed", False)
    kill_zone = market_data.get("kill_zone", False)

    # Entry = close (zone内实际成交价)
    entry = close

    # Calculate SL based on trade direction
    if trade_direction == "LONG":
        sl = swing_low
        risk = entry - sl
    else:
        sl = swing_high
        risk = sl - entry

    if risk <= 0:
        return None

    # Risk in USD (MNQ: $2 per tick, 0.25 price = 1 tick)
    risk_ticks = risk / TICK_SIZE
    risk_usd = risk_ticks * PRICE_PER_TICK

    if risk_usd > MAX_RISK_USD:
        return None

    # === TARGET = Entry + (2.5 × Risk) — 固定 RR 2.5 ===
    if trade_direction == "LONG":
        target_rr25 = entry + (2.5 * risk)
    else:
        target_rr25 = entry - (2.5 * risk)

    # === FIB EXTENSIONS FOR PARTIAL/RUNNER ===
    # Partial @ -0.5 extension
    partial_target = calc_fib_extension(swing_low, swing_high, trade_direction, -0.5)

    # Runner @ -1.0 extension (or use RR2.5 target, whichever is better)
    runner_target = calc_fib_extension(swing_low, swing_high, trade_direction, -1.0)

    # === CALCULATE RR FOR EACH TARGET ===
    if trade_direction == "LONG":
        rr_rr25 = (target_rr25 - entry) / risk if risk > 0 else 0
        rr_partial = (partial_target - entry) / risk if risk > 0 else 0
        rr_runner = (runner_target - entry) / risk if risk > 0 else 0
    else:
        rr_rr25 = (entry - target_rr25) / risk if risk > 0 else 0
        rr_partial = (entry - partial_target) / risk if risk > 0 else 0
        rr_runner = (entry - runner_target) / risk if risk > 0 else 0

    # Use RR2.5 target (fixed) as main target
    rr = rr_rr25

    # Validate RR ≥ 2.5
    if rr < MIN_RR:
        return None

    # === SIZE ===
    # Risk per contract: $2/tick × ticks
    # Max risk $80 → contracts = 80 / risk_usd_per_contract
    contracts = min(2, max(1, int(MAX_RISK_USD / risk_usd))) if risk_usd > 0 else 1

    # === FORMAT SIGNAL ===
    signal = {
        "type": "OTE_RR25",
        "direction": trade_direction,
        "entry": round(entry, 2),
        "sl": round(sl, 2),
        "target1": round(partial_target, 2),   # Partial @ -0.5
        "target2": round(target_rr25, 2),      # RR2.5 main target
        "runner_target": round(runner_target, 2),  # -1.0 extension
        "rr": round(rr, 2),
        "risk_usd": round(risk_usd, 2),
        "size": contracts,
        "swing_low": round(swing_low, 2),
        "swing_high": round(swing_high, 2),
        "ote_zone_hit": ote_zone_hit,
        "ltf_rejection": ltf_rejection,
        "mss_confirmed": mss_confirmed,
        "kill_zone": kill_zone,
        "time": market_data.get("time", datetime.now().isoformat()),
        "note": f"Entry {round(entry,2)} | SL {round(sl,2)} | Risk ${round(risk_usd,2)} | RR {round(rr,2)} | Zone {ote_zone_hit} | Reject {ltf_rejection} | MSS {mss_confirmed}"
    }

    # Update P&L tracking
    pnl["signal_count"] = pnl.get("signal_count", 0) + 1
    pnl.setdefault("signals", []).append({
        "time": signal["time"],
        "direction": signal["direction"],
        "entry": signal["entry"],
        "rr": signal["rr"],
        "risk_usd": signal["risk_usd"]
    })
    save_pnl(pnl)

    log_signal(signal, is_test=TEST_MODE)

    return signal


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OTE Signal Checker CLI")
    parser.add_argument("--close", type=float, required=True, help="Entry/close price")
    parser.add_argument("--swing-low", type=float, required=True, help="Swing low value")
    parser.add_argument("--swing-high", type=float, required=True, help="Swing high value")
    parser.add_argument("--bias", choices=["bullish", "bearish"], help="Daily bias (auto-inferred if not provided)")
    parser.add_argument("--in-zone", action="store_true", default=False, help="Price is in OTE zone")
    parser.add_argument("--reject", action="store_true", default=False, help="LTF rejection confirmed")
    parser.add_argument("--mss", action="store_true", default=False, help="MSS confirmed")
    parser.add_argument("--kill-zone", action="store_true", default=False, help="In kill zone")
    parser.add_argument("--time", default=None, help="Timestamp")
    args = parser.parse_args()

    data = {
        "close": args.close,
        "swing_low": args.swing_low,
        "swing_high": args.swing_high,
        "daily_bias": args.bias,
        "ote_zone_hit": args.in_zone,
        "ltf_rejection": args.reject,
        "mss_confirmed": args.mss,
        "kill_zone": args.kill_zone,
        "time": args.time or datetime.now().isoformat(),
    }

    result = check_ote_signal(data)
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("NO SIGNAL (conditions not met)")
        sys.exit(1)