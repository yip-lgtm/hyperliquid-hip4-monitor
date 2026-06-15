#!/usr/bin/env python3
"""
HIP-4 Monitor Long-Running WS Daemon
Keeps WebSocket connected 24/7, logs events to JSONL.
Usage: python3 daemon.py [--workspace /path/to/workspace]
"""
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Add workspace to path
WORKSPACE = Path("/home/node/.openclaw/workspace")
sys.path.insert(0, str(WORKSPACE))

from skills.hyperliquid_hip4_monitor.monitor import (
    HIP4Monitor, WebSocketPriceFeed,
    get_validator_l1_votes, get_all_mids,
    get_outcome_prices, find_arb_signals,
    fetch_polymarket_markets, POLYMARKET_GAMMA_URL,
)

STATE_FILE = WORKSPACE / "hip4_daemon_state.json"
EVENTS_FILE = WORKSPACE / "hip4_events.jsonl"


def log_event(event_type: str, payload: dict):
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "event": event_type, "payload": payload}
    try:
        with open(EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[Log error] {e}")


def run_daemon(poll_interval: int = 180, arb_threshold: float = 0.05):
    print(f"Starting HIP-4 WS Daemon — poll every {poll_interval}s")
    print(f"Events file: {EVENTS_FILE}")
    print(f"State file: {STATE_FILE}")

    ws_feed = WebSocketPriceFeed()
    ws_feed.start()
    print("WebSocket connected, warming up 5s...")
    import time; time.sleep(5)

    monitor = HIP4Monitor(arb_threshold=arb_threshold)
    last_new_markets = []
    last_arb_count = 0

    while True:
        try:
            import time
            time.sleep(poll_interval)

            result = monitor.execute(ws_enabled=False)  # WS already warm via ws_feed

            new_markets = result.get("new_markets", [])
            newly_settled = result.get("newly_settled", [])
            arb_signals = result.get("arb_signals", [])

            # Log new markets
            for m in new_markets:
                log_event("new_canonical_market_detected", {"market": m})

            # Log new settlements
            for m in newly_settled:
                log_event("newly_settled_market", {"market": m})

            # Log arb signals
            for a in arb_signals:
                log_event("arb_opportunity", {
                    "asset": a["hl_market"],
                    "diff": a["diff"],
                    "direction": a["direction"],
                    "hl_prob": a["hl_prob"],
                    "pm_prob": a["pm_prob"],
                    "pm_question": a["pm_question"],
                })

            # Report
            if new_markets or newly_settled or arb_signals:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"NEW: {len(new_markets)} | SETTLED: {len(newly_settled)} | ARB: {len(arb_signals)}")
                print(monitor.get_summary())
            else:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] No events, holding steady")

        except Exception as e:
            print(f"[Error] {e}")
            import time; time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default="/home/node/.openclaw/workspace")
    parser.add_argument("--poll-interval", type=int, default=180)
    parser.add_argument("--arb-threshold", type=float, default=0.05)
    args = parser.parse_args()

    run_daemon(poll_interval=args.poll_interval, arb_threshold=args.arb_threshold)
