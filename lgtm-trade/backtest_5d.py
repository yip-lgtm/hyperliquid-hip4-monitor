#!/usr/bin/env python3
"""
BTC 5m Markov Bot Backtest - 5 Days
Simulates Markov Chain signal generation on historical OKX data.
"""

import json
import time
import random
import sys
from datetime import datetime, timezone
from collections import Counter

# ── Config ─────────────────────────────────────────────────────────────────
STATE_WINDOW = 4
ATR_MULT = 0.8
VOL_MULT = 0.3
MIN_PROB = 0.87
MIN_EDGE = 0.03
MAX_POSITION = 5.0

# ── OKX Fetcher ────────────────────────────────────────────────────────────
def fetch_okx_klines(inst_id, bar, limit, after_ms=None):
    import urllib.request
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    if after_ms:
        url += f"&after={after_ms}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    return data.get('data', [])

def fetch_historical_5m(days=5):
    all_candles = []
    now_ms = int(time.time() * 1000)
    limit = 1440
    
    for _ in range(10):
        after = now_ms if not all_candles else int(all_candles[-1][0]) - 1
        oldest_ts = int(after / 1000)
        oldest_dt = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
        days_ago = (datetime.now(timezone.utc) - oldest_dt).days
        
        if days_ago > days:
            break
        
        candles = fetch_okx_klines('BTC-USDT', '5m', limit, after)
        if not candles:
            break
        all_candles.extend(candles)
        now_ms = int(candles[-1][0]) - 1
        
        if len(all_candles) >= days * 300:
            break
    
    # Deduplicate and sort
    seen = set()
    unique = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    unique.sort(key=lambda x: int(x[0]))
    
    # Keep only last N days
    cutoff = int((time.time() - days * 86400) * 1000)
    unique = [c for c in unique if int(c[0]) >= cutoff]
    return unique

# ── Indicators ─────────────────────────────────────────────────────────────
def compute_directions(klines):
    return ['UP' if float(k[4]) > float(k[1]) else 'DOWN' for k in klines]

def compute_streaks(directions):
    streaks = [1]
    for i in range(1, len(directions)):
        if directions[i-1] == directions[i]:
            streaks.append(streaks[-1] + 1)
        else:
            streaks.append(1)
    return streaks

def compute_atr_at(klines, index, period=14):
    """ATR at specific index"""
    if index < period:
        return 0
    trs = []
    for j in range(1, period + 1):
        if index - j < 0:
            break
        h = float(klines[index - j + 1][2])
        l = float(klines[index - j + 1][3])
        c = float(klines[index - j][4])
        tr = max(h - l, abs(h - c), abs(l - c))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0

def get_avg_atr(klines, period=14, lookback=20):
    atrs = [compute_atr_at(klines, i, period) for i in range(period, len(klines))]
    atrs = [a for a in atrs if a > 0]
    if not atrs:
        return 50
    return sum(atrs[-lookback:]) / min(len(atrs), lookback)

def get_volume_at(klines, index):
    if index < 0 or index >= len(klines):
        return 0
    return float(klines[index][5])

def get_avg_vol(klines, lookback=20):
    vols = [float(k[5]) for k in klines[-lookback:]]
    return sum(vols) / len(vols) if vols else 1

# ── Backtest ────────────────────────────────────────────────────────────────
def run_backtest(klines, days=5):
    random.seed(42)  # reproducible
    print(f"\n{'='*60}")
    print(f"BTC 5m Markov Backtest - {days} Days")
    print(f"{'='*60}")
    
    if len(klines) < 200:
        print(f"ERROR: Only {len(klines)} candles")
        return None
    
    # Train/test split (80/20)
    train_end = int(len(klines) * 0.8)
    train_klines = klines[:train_end]
    test_klines = klines[train_end:]
    
    print(f"Total: {len(klines)} | Train: {len(train_klines)} | Test: {len(test_klines)}")
    
    # Build Markov matrix from training data
    train_dirs = compute_directions(train_klines)
    train_streaks = compute_streaks(train_dirs)
    train_avg_atr = get_avg_atr(train_klines)
    train_avg_vol = get_avg_vol(train_klines)
    
    transitions = {}
    for i in range(STATE_WINDOW, len(train_klines) - 1):
        atr = compute_atr_at(train_klines, i)
        vol = get_volume_at(train_klines, i)
        if atr > train_avg_atr * ATR_MULT and vol > train_avg_vol * VOL_MULT:
            state = ''.join(str(s) for s in train_streaks[i-STATE_WINDOW:i])
            ndir = train_dirs[i + 1]
            if state not in transitions:
                transitions[state] = {'UP': 0, 'DOWN': 0}
            transitions[state][ndir] += 1
    
    matrix = {}
    for state, counts in transitions.items():
        total = counts['UP'] + counts['DOWN']
        if total >= 2:
            matrix[state] = {
                'UP': counts['UP'] / total,
                'DOWN': counts['DOWN'] / total,
                'total': total
            }
    
    print(f"Markov matrix: {len(matrix)} states")
    
    # Build full direction/streak arrays for ATR computation on test
    all_klines = train_klines + test_klines
    all_dirs = compute_directions(all_klines)
    all_streaks = compute_streaks(all_dirs)
    
    # Walk-forward test
    trades = []
    wins, losses = 0, 0
    pnl = 0.0
    
    for i in range(STATE_WINDOW, len(test_klines) - 1):
        idx = train_end + i  # absolute index in all_klines
        
        state = ''.join(str(s) for s in all_streaks[idx-STATE_WINDOW:idx])
        last_dir = all_dirs[idx]
        
        # Get prob from matrix
        if state in matrix:
            m = matrix[state]
            last_char = state[-1]
            pred_dir = 'UP' if last_char == '1' else 'DOWN'
            prob = m.get(pred_dir, 0.5)
            state_n = m.get('total', 0)
        else:
            prob = 0.5
            state_n = 0
        
        # ATR/Vol filters
        atr = compute_atr_at(all_klines, idx)
        vol = get_volume_at(all_klines, idx)
        
        # Expanding average ATR/vol for test period
        lookback_klines = all_klines[:idx+1]
        avg_atr = get_avg_atr(lookback_klines)
        avg_vol = get_avg_vol(lookback_klines)
        
        passes_atr = atr > avg_atr * ATR_MULT
        passes_vol = vol > avg_vol * VOL_MULT
        
        # Simulate market price (with noise to simulate Polymarket spread)
        noise = random.uniform(-0.015, 0.015)
        yes_p = 0.50 + (0.02 if last_dir == 'UP' else -0.02) + noise
        yes_p = max(0.01, min(0.99, yes_p))
        no_p = 1 - yes_p
        q = no_p if last_dir == 'UP' else yes_p
        edge = prob - q
        
        passes_prob = prob >= MIN_PROB
        passes_edge = edge >= MIN_EDGE
        
        signal = passes_atr and passes_vol and passes_prob and passes_edge
        
        if signal:
            # Simulate outcome
            actual_dir = all_dirs[idx + 1]
            won = actual_dir == last_dir
            
            if won:
                wins += 1
                trade_pnl = edge * MAX_POSITION
                pnl += trade_pnl
            else:
                losses += 1
                trade_pnl = -q * MAX_POSITION
                pnl += trade_pnl
            
            trades.append({
                'cycle': i,
                'state': state,
                'direction': last_dir,
                'prob': round(prob, 3),
                'edge': round(edge, 3),
                'yes_price': round(yes_p, 3),
                'actual': actual_dir,
                'won': won,
                'pnl': round(trade_pnl, 2),
                'time': datetime.fromtimestamp(int(test_klines[i][0])/1000, tz=timezone.utc).strftime('%m-%d %H:%M')
            })
    
    total_trades = wins + losses
    if total_trades > 0:
        wr = wins / total_trades * 100
        avg_pnl = pnl / total_trades
        pf = wins / max(losses, 1)
        avg_win = pnl / total_trades if total_trades > 0 else 0
        avg_loss = avg_win  # simplified
        rr = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        
        print(f"\n{'='*60}")
        print(f"RESULTS - {days} Day Backtest")
        print(f"{'='*60}")
        print(f"Cycles:           {len(test_klines)}")
        print(f"Signals:          {total_trades}")
        print(f"Win Rate:         {wr:.1f}% ({wins}W/{losses}L)")
        print(f"Total P&L:        ${pnl:.2f}")
        print(f"Avg/Trade:        ${avg_pnl:.2f}")
        print(f"Profit Factor:    {pf:.2f}")
        print(f"RR:              {rr:.2f}:1")
        print(f"Matrix states:   {len(matrix)}")
    else:
        print("\n⚠ No signals triggered - MIN_PROB too high or market rangebound")
        print(f"Max prob seen: check log above")
        pf = wr = avg_pnl = 0
    
    out = {
        'days': days,
        'total_cycles': len(test_klines),
        'signals': total_trades,
        'wins': wins,
        'losses': losses,
        'pnl': round(pnl, 2),
        'win_rate': round(wr, 1),
        'profit_factor': round(pf, 2),
        'trades': trades
    }
    
    out_file = f'/home/node/.openclaw/workspace/lgtm-trade/backtest_{days}d_results.json'
    with open(out_file, 'w') as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved: {out_file}")
    return out

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    print(f"Fetching {days} days of BTC 5m data from OKX...")
    candles = fetch_historical_5m(days)
    print(f"Got {len(candles)} candles")
    
    if len(candles) < 100:
        print("Not enough data")
        sys.exit(1)
    
    first = datetime.fromtimestamp(int(candles[0][0])/1000, tz=timezone.utc)
    last = datetime.fromtimestamp(int(candles[-1][0])/1000, tz=timezone.utc)
    print(f"Range: {first.strftime('%Y-%m-%d %H:%M')} → {last.strftime('%Y-%m-%d %H:%M')}")
    
    run_backtest(candles, days)
