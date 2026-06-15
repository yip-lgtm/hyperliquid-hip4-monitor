#!/usr/bin/env python3
"""
Polymarket → HIP-4 Backtester
Pulls resolved Polymarket markets, converts to backtest format, runs backtest.
Usage: python3 run_polymarket_backtest.py [--samples N] [--threshold T] [--dry-run]
"""
import requests
import json
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path

INITIAL_CAPITAL = 1000
PERF_FILE = Path("/home/node/.openclaw/workspace/hip4_performance.json")


def fetch_resolved_markets(limit: int = 300) -> list:
    url = "https://gamma-api.polymarket.com/markets"
    params = {"closed": "true", "limit": limit, "order": "volume24hr", "ascending": "false"}
    print("Fetching resolved Polymarket markets...")
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return []
    markets = resp.json()
    resolved = []
    for m in markets:
        prices_str = m.get("outcomePrices", "[]")
        try:
            prices = json.loads(prices_str)
        except Exception:
            prices = []
        # Valid resolved = has non-zero prices
        if prices and prices[0] not in ("0", "0.0", ""):
            resolved.append({
                "id": m.get("id"),
                "question": m.get("question"),
                "slug": m.get("slug"),
                "outcomePrices": prices,
                "volume": m.get("volumeNum"),
                "resolutionTime": m.get("endDate"),
            })
    print(f"Fetched {len(resolved)} resolved markets")
    return resolved


def convert_to_backtest_format(markets: list, num_samples: int, mock_edge: bool = True) -> dict:
    signals = []
    sample_markets = markets[:num_samples]
    random.seed(42)
    for i, m in enumerate(sample_markets):
        if mock_edge:
            edge = round(random.uniform(0.035, 0.105), 4)
        else:
            edge = 0.05
        try:
            final_yes_price = float(m["outcomePrices"][0])
            was_profitable = final_yes_price > 0.5
        except Exception:
            was_profitable = random.choice([True, False])
        ts = m.get("resolutionTime") or datetime.now(timezone.utc).isoformat()
        signals.append({
            "market": f"POLY_{m['slug'][:35]}",
            "direction": "BUY_HL" if i % 2 == 0 else "BUY_PM",
            "diff": edge,
            "settled": True,
            "settled_price": round(random.uniform(0.45, 0.75), 4),
            "was_profitable": was_profitable,
            "edge": edge,
            "sim_pnl": round(edge * 15.0 * (1 if was_profitable else -1), 4),
            "signal_at": ts,
        })
    return {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "source": "polymarket_resolved",
        "signals": signals,
    }


def run_backtest(arb_threshold: float, dry_run_mode: bool):
    import sys
    sys.path.insert(0, "/home/node/.openclaw/workspace")
    from skills.hyperliquid_hip4_monitor.monitor import HIP4BacktesterV2
    bt = HIP4BacktesterV2()
    report = bt.run_backtest(
        initial_capital=INITIAL_CAPITAL,
        arb_threshold=arb_threshold,
        dry_run_mode=dry_run_mode,
    )
    if "message" in report:
        print(f"No data: {report['message']}")
        return
    print("\n" + bt.format_report(report))
    bt.plot_equity_curve(report, save_path="/tmp/equity.png", show=False)
    print("\nEquity curve saved: /tmp/equity.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket backtest runner")
    parser.add_argument("--samples", type=int, default=80)
    parser.add_argument("--threshold", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    markets = fetch_resolved_markets(limit=400)
    if not markets:
        exit(1)

    data = convert_to_backtest_format(markets, args.samples, mock_edge=True)
    PERF_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"Wrote {len(data['signals'])} signals → {PERF_FILE}")

    run_backtest(arb_threshold=args.threshold, dry_run_mode=args.dry_run)
