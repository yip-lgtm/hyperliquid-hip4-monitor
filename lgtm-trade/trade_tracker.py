#!/usr/bin/env python3
"""
BTC 5m Markov Bot - Trade Tracker
Automatically tracks signals, logs outcomes, computes stats.
Run after each cycle or on-demand.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

TRACKER_FILE = "/home/node/.openclaw/workspace/lgtm-trade/trade_tracker.json"
STATE_FILE = "/home/node/.openclaw/workspace/btc_5m_state_v3.json"

def load_tracker():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            return json.load(f)
    return {
        "trades": [],
        "stats": {
            "total": 0, "wins": 0, "losses": 0,
            "pnl": 0.0, "pending": 0
        },
        "iteration": 1,
        "config": {}
    }

def save_tracker(t):
    with open(TRACKER_FILE, 'w') as f:
        json.dump(t, f, indent=2)

def check_outcome(window_ts):
    """
    Fetch actual market outcome from OKX candle.
    window_ts = start of 5min window
    Returns 'UP', 'DOWN', or None
    """
    try:
        import urllib.request
        from datetime import datetime, timezone
        
        # Fetch recent OKX 5m candles
        url = 'https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=50'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        
        candles = data.get('data', [])
        for c in candles:
            ts_ms = int(c[0])
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            # window_ts is the START of the 5min candle
            candle_start = int(dt.strftime('%m%H%M'))  # HHMM format
            window_start = int(datetime.fromtimestamp(window_ts, tz=timezone.utc).strftime('%m%H%M'))
            
            # Match by minute (5min window = same HHMM as start)
            from datetime import timedelta
            window_dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
            candle_dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
            
            if abs((candle_dt - window_dt).total_seconds()) < 300:
                o, cl = float(c[1]), float(c[4])
                return 'UP' if cl > o else 'DOWN'
    except Exception as e:
        print(f"Outcome check error: {e}")
    return None

def update_tracker():
    t = load_tracker()
    
    if not os.path.exists(STATE_FILE):
        print("No state file found")
        return t
    
    with open(STATE_FILE) as f:
        state = json.load(f)
    
    history = state.get('history', [])
    signals = [h for h in history if h.get('signal')]
    
    print(f"\n{'='*60}")
    print(f"BTC 5m Markov Trade Tracker")
    print(f"{'='*60}")
    print(f"Iteration: {t.get('iteration', 1)}")
    print(f"State cycles: {state.get('cycle_count', 0)}")
    print(f"Tracked trades: {len(t['trades'])}")
    print(f"Pending (unresolved): {t['stats'].get('pending', 0)}")
    print()
    
    # Check for new signals
    tracked_cycles = {tr['cycle'] for tr in t['trades']}
    new_signals = [s for s in signals if s['cycle'] not in tracked_cycles]
    
    if new_signals:
        print(f"New signals to track: {len(new_signals)}")
        for sig in new_signals:
            cycle = sig['cycle']
            direction = sig['direction']
            p = sig['prob_continue']
            edge = sig['edge']
            window_ts = sig.get('window_ts') or (int((datetime.now(timezone.utc).timestamp() // 300) * 300) + 300)
            
            outcome = check_outcome(window_ts)
            
            if outcome:
                won = outcome == direction
                trade = {
                    'cycle': cycle,
                    'direction': direction,
                    'prob': p,
                    'edge': round(edge, 3),
                    'outcome': outcome,
                    'won': won,
                    'pnl': round(edge * 5, 2) if won else round(-(1 - edge - 0.5) * 5, 2),  # rough pnl
                    'time': sig.get('time', ''),
                    'resolved': True
                }
                t['trades'].append(trade)
                print(f"  Cycle {cycle}: {direction} → {outcome} {'✅' if won else '❌'}")
            else:
                # Pending
                trade = {
                    'cycle': cycle,
                    'direction': direction,
                    'prob': p,
                    'edge': round(edge, 3),
                    'outcome': None,
                    'won': None,
                    'pnl': 0.0,
                    'time': sig.get('time', ''),
                    'resolved': False
                }
                t['trades'].append(trade)
                print(f"  Cycle {cycle}: {direction} (p̂={p:.3f}) → PENDING")
    
    # Compute stats
    resolved = [tr for tr in t['trades'] if tr.get('resolved')]
    pending = [tr for tr in t['trades'] if not tr.get('resolved')]
    
    wins = sum(1 for tr in resolved if tr['won'])
    losses = sum(1 for tr in resolved if not tr['won'])
    total = wins + losses
    
    wins_pnl = sum(tr['pnl'] for tr in resolved if tr['won'])
    losses_pnl = sum(abs(tr['pnl']) for tr in resolved if not tr['won'])
    total_pnl = sum(tr['pnl'] for tr in resolved)
    
    wr = wins / total * 100 if total > 0 else 0
    pf = wins / max(losses, 1)
    rr = wins_pnl / losses_pnl if losses_pnl > 0 else 0
    
    t['stats'] = {
        'total': total,
        'wins': wins,
        'losses': losses,
        'pending': len(pending),
        'pnl': round(total_pnl, 2),
        'win_rate': round(wr, 1),
        'profit_factor': round(pf, 2),
        'avg_win': round(wins_pnl / wins, 2) if wins > 0 else 0,
        'avg_loss': round(losses_pnl / losses, 2) if losses > 0 else 0,
        'rr': round(rr, 2) if rr > 0 else 0
    }
    t['iteration'] = t.get('iteration', 1) + 1
    
    save_tracker(t)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"STATISTICS (All Iterations)")
    print(f"{'='*60}")
    print(f"Total resolved: {total} ({wins}W / {losses}L)")
    print(f"Pending: {len(pending)}")
    print(f"Win Rate: {wr:.1f}%")
    print(f"P&L: ${total_pnl:.2f}")
    print(f"Profit Factor: {pf:.2f}")
    print(f"Avg Win: ${wins_pnl/wins:.2f}" if wins > 0 else "Avg Win: $0")
    print(f"Avg Loss: ${losses_pnl/losses:.2f}" if losses > 0 else "Avg Loss: $0")
    print(f"RR: {rr:.2f}:1" if rr > 0 else "RR: N/A")
    print(f"\nRecent trades:")
    for tr in resolved[-5:]:
        emoji = "✅" if tr['won'] else "❌"
        print(f"  Cycle {tr['cycle']}: {tr['direction']} → {tr['outcome']} {emoji} (pnl=${tr['pnl']:.2f})")
    
    return t

if __name__ == '__main__':
    update_tracker()
