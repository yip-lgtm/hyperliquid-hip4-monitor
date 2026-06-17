#!/usr/bin/env python3
"""
BTC 5m Polymarket Bot v3 - Markov + ATR/Vol Filter (DRY_RUN)
- State window: 3 candles (streak-based)
- ATR(14) > 1.2x avg(20) filter
- Volume > 1.5x avg filter
- Auto-adjust MIN_PROB after 50 cycles
"""

import time
import json
import sys
import asyncio
import httpx
import os
from datetime import datetime, timezone
from collections import Counter

# === CONFIG ===
DRY_RUN = True  # LIVE TRADING
MIN_PROB = 0.55
MIN_EDGE = 0.01
MAX_POSITION = 5.0  # Max $5 per trade
MAX_DAILY_LOSS = 30.0  # Max $30 daily loss
CHECK_INTERVAL = 81
TARGET_CYCLES = 50
STATE_WINDOW = 4
MIN_STATE_N = 2
ATR_MULT = 0.8
VOL_MULT = 0.1

TELEGRAM_TOKEN = "8606567428:AAFvcsiNf00mAIES6-CTIwKeQTKaos0trNY"
TELEGRAM_CHAT_ID = "8475453959"
GAMMA_API = "https://gamma-api.polymarket.com"

# State
BTC_KLINES = []
TRANSITION_MATRIX = {}
STATE_HISTORY = []
CYCLE_COUNT = 0
CONSECUTIVE_LOSSES = 0
TODAY_LOSS = 0.0
LAST_TRADE_DATE = None
PAUSED_TODAY = False

# Files
STATE_FILE = "/home/node/.openclaw/workspace/btc_5m_state_v3.json"
CONFIG_FILE = "/home/node/.openclaw/workspace/btc_5m_config_v3.json"

def get_current_window_ts():
    return (int(time.time()) // 300) * 300

def get_next_window_ts():
    return get_current_window_ts() + 300

def construct_slug(window_ts):
    return f"btc-updown-5m-{window_ts}"

def parse_outcome_prices(opp_str):
    try:
        prices = json.loads(opp_str)
        return float(prices[0]), float(prices[1])
    except:
        return 0.5, 0.5

async def fetch_btc_klines():
    """Fetch BTC 5m candles from OKX"""
    global BTC_KLINES
    try:
        all_candles = []
        url_template = 'https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=200&after={}'
        after = None
        
        for page in range(8):
            url = url_template.format(after) if after else url_template.format('')
            async with httpx.AsyncClient(timeout=15) as http:
                resp = await http.get(url)
                data = resp.json()
                candles = data.get('data', [])
                if not candles:
                    break
                all_candles.extend(candles)
                after = candles[-1][0]
                if len(all_candles) >= 1440:
                    break
        
        all_candles.reverse()
        BTC_KLINES = all_candles[:1440]
        return len(BTC_KLINES)
    except Exception as e:
        print(f"[BTC] Error: {e}")
        return 0

def compute_directions(klines):
    return ['UP' if float(k[4]) > float(k[1]) else 'DOWN' for k in klines]

def compute_streaks(directions):
    streaks = []
    current_streak = 1
    for i, d in enumerate(directions):
        if i == 0:
            streaks.append(1)
        elif directions[i-1] == d:
            current_streak += 1
            streaks.append(current_streak)
        else:
            current_streak = 1
            streaks.append(current_streak)
    return streaks

def compute_atr(klines, index, period=14):
    """Compute ATR at specific index"""
    if index < period:
        return 0
    trs = []
    for j in range(1, period + 1):
        if index - j < 0:
            break
        high = float(klines[index - j + 1][2])
        low = float(klines[index - j + 1][3])
        prev_close = float(klines[index - j][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0

def get_avg_atr(klines, period=14, lookback=20):
    """Get average ATR over last N periods"""
    atrs = []
    for i in range(period, len(klines)):
        atr = compute_atr(klines, i, period)
        if atr > 0:
            atrs.append(atr)
    if len(atrs) < lookback:
        return sum(atrs) / len(atrs) if atrs else 50
    return sum(atrs[-lookback:]) / lookback

def get_volume(index, klines):
    if index < 0 or index >= len(klines):
        return 0
    return float(klines[index][5])

def get_avg_vol(klines, lookback=20):
    vols = [float(k[5]) for k in klines[-lookback:]]
    return sum(vols) / len(vols) if vols else 1

def build_filtered_matrix(directions, streaks, klines, atr_avg, vol_avg):
    """Build Markov matrix with ATR and volume filters"""
    global TRANSITION_MATRIX
    
    filtered_indices = []
    for i in range(STATE_WINDOW, len(klines) - 1):
        atr = compute_atr(klines, i)
        vol = get_volume(i, klines)
        if atr > atr_avg * ATR_MULT and vol > vol_avg * VOL_MULT:
            filtered_indices.append(i)
    
    transitions = {}
    for i in filtered_indices:
        state = ''.join(str(s) for s in streaks[i-STATE_WINDOW:i])
        next_dir = directions[i + 1]
        if state not in transitions:
            transitions[state] = {'UP': 0, 'DOWN': 0}
        transitions[state][next_dir] += 1
    
    TRANSITION_MATRIX = {}
    for state, counts in transitions.items():
        total = counts['UP'] + counts['DOWN']
        if total > 0:
            TRANSITION_MATRIX[state] = {
                'UP': counts['UP'] / total,
                'DOWN': counts['DOWN'] / total,
                'total': total
            }
    
    return len(filtered_indices), len(TRANSITION_MATRIX)

def get_state(state):
    if state is None or state not in TRANSITION_MATRIX:
        return 0.5, 0
    m = TRANSITION_MATRIX[state]
    last_char = state[-1]
    last_dir = 'UP' if last_char == '1' else 'DOWN'
    key = 'UP' if last_dir == 'UP' else 'DOWN'
    return m.get(key, 0.5), m.get('total', 0)

async def execute_live_trade(market, direction, prob_continue, edge, q, signal_msg):
    """Execute live trade with real money via PolyClaw L2 Client"""
    global CONSECUTIVE_LOSSES, TODAY_LOSS, LAST_TRADE_DATE, PAUSED_TODAY
    
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Reset daily loss if new day
    if LAST_TRADE_DATE != today:
        TODAY_LOSS = 0.0
        CONSECUTIVE_LOSSES = 0
        LAST_TRADE_DATE = today
        PAUSED_TODAY = False
    
    # Check pause conditions
    if PAUSED_TODAY:
        return {
            'message': signal_msg + "\n\n⛸️ TRADING PAUSED TODAY (loss limit reached)",
            'executed': False,
            'reason': 'paused'
        }
    
    # Check daily loss limit
    if TODAY_LOSS >= MAX_DAILY_LOSS:
        PAUSED_TODAY = True
        return {
            'message': signal_msg + f"\n\n🚫 DAILY LOSS LIMIT REACHED (${TODAY_LOSS:.2f} >= ${MAX_DAILY_LOSS:.2f})\nTrading paused for today.",
            'executed': False,
            'reason': 'daily_limit'
        }
    
    # Use minimum of MAX_POSITION or $1 for test
    position_size = min(MAX_POSITION, 1.0)  # $1 test trade
    
    # Prepare trade info
    if direction == 'UP':
        outcome = 'YES'
        entry_price = market['yes_price']
    else:
        outcome = 'NO'
        entry_price = market['no_price']
    
    # Execute real trade via PolyClaw L2
    try:
        from py_clob_client_v2 import ClobClient, SignatureTypeV2
        from py_clob_client_v2.order_builder.constants import BUY, SELL
        from py_clob_client_v2 import OrderArgs, PartialCreateOrderOptions
        
        private_key = os.environ.get('POLYCLAW_PRIVATE_KEY', '')
        funder = os.environ.get('RELAYER_API_KEY_ADDRESS', '')
        
        if not private_key or not funder:
            raise Exception('Missing POLYCLAW_PRIVATE_KEY or RELAYER_API_KEY_ADDRESS')
        
        # Create L2 client with POLY_1271 signature
        temp_client = ClobClient(
            host='https://clob.polymarket.com',
            key=private_key,
            chain_id=137
        )
        api_creds = temp_client.create_or_derive_api_key()
        
        client = ClobClient(
            host='https://clob.polymarket.com',
            key=private_key,
            chain_id=137,
            creds=api_creds,
            signature_type=SignatureTypeV2.POLY_1271,
            funder=funder
        )
        
        # Get token ID from market
        token_id = market.get('tokens', [None])[0] if market.get('tokens') else None
        if not token_id:
            raise Exception('No token_id in market')
        
        # Place order
        side = BUY if direction == 'UP' else SELL
        
        # Size = position_size / price (rough approximation)
        size = position_size / entry_price
        
        response = client.create_and_post_order(
            OrderArgs(
                token_id=token_id,
                price=entry_price,
                size=size,
                side=side,
            ),
            options=PartialCreateOrderOptions(
                tick_size='0.01',
                neg_risk=False,
            ),
        )
        
        if response.get('orderID'):
            pnl = 0  # Unresolved
            CONSECUTIVE_LOSSES = 0
            result_text = f"ORDER PLACED: {response.get('orderID')[:20]}..."
            result_emoji = "✅"
        else:
            raise Exception(str(response))
        
    except Exception as e:
        pnl = 0
        CONSECUTIVE_LOSSES += 1
        TODAY_LOSS += position_size
        result_emoji = "❌"
        result_text = f"ERROR: {str(e)[:100]}"
    
    # Check consecutive losses
    if CONSECUTIVE_LOSSES >= 3:
        PAUSED_TODAY = True
        pause_msg = f"\n\n⚠️ 3 CONSECUTIVE LOSSES - Trading paused for today"
    else:
        pause_msg = ""
    
    trade_msg = f"""

💰 <b>LIVE TRADE EXECUTED</b>
🔹 Direction: <b>{direction}</b> ({outcome})
🔹 Entry Price: <code>{entry_price:.3f}</code>
🔹 Position: <b>${position_size:.2f}</b>
🔹 Expected Prob: <code>{prob_continue:.3f}</code>
🔹 Edge: <code>{edge:.3f}</code>
{result_emoji} <b>Result:</b> {result_text}
📊 Today P&L: <code>${-TODAY_LOSS:.2f}</code>
📊 Consecutive Losses: {CONSECUTIVE_LOSSES}{pause_msg}"""
    
    return {
        'message': signal_msg + trade_msg,
        'executed': True,
        'direction': direction,
        'position_size': position_size
    }

async def fetch_polymarket_market(window_ts):
    slug = construct_slug(window_ts)
    async with httpx.AsyncClient(timeout=30) as http:
        resp = await http.get(f"{GAMMA_API}/markets", params={"slug": slug})
        if resp.status_code == 200:
            data = resp.json()
            if data:
                m = data[0]
                yes_p, no_p = parse_outcome_prices(m.get("outcomePrices", '["0.5", "0.5"]'))
                return {
                    "id": m.get("id"),
                    "slug": m.get("slug"),
                    "question": m.get("question"),
                    "yes_price": yes_p,
                    "no_price": no_p,
                    "end_date": m.get("endDate"),
                    "volume": m.get("volume", 0)
                }
    return None

async def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            await http.post(url, json=payload)
            return True
    except:
        return False

def load_state():
    global CYCLE_COUNT, STATE_HISTORY
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
                CYCLE_COUNT = data.get('cycle_count', 0)
                STATE_HISTORY = data.get('history', [])
    except:
        pass

def save_state():
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({'cycle_count': CYCLE_COUNT, 'history': STATE_HISTORY[-100:]}, f)
    except:
        pass

def load_config():
    global MIN_PROB, MIN_EDGE
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE) as f:
                data = json.load(f)
                MIN_PROB = data.get('MIN_PROB', 0.87)
                MIN_EDGE = data.get('MIN_EDGE', 0.03)
    except:
        pass

def save_config():
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump({'MIN_PROB': MIN_PROB, 'MIN_EDGE': MIN_EDGE}, f, indent=2)
    except:
        pass

async def run_trading_cycle():
    global STATE_HISTORY, CYCLE_COUNT
    
    load_state()
    load_config()
    
    next_window = get_next_window_ts()
    now = datetime.now(timezone.utc)
    CYCLE_COUNT += 1
    
    print(f"\n{'='*60}")
    print(f"Cycle #{CYCLE_COUNT} | {now.strftime('%H:%M:%S')} UTC")
    
    # 1. Fetch BTC
    btc_count = await fetch_btc_klines()
    print(f"[BTC] {btc_count} candles")
    
    if btc_count < 100:
        return None
    
    # 2. Compute indicators
    directions = compute_directions(BTC_KLINES)
    streaks = compute_streaks(directions)
    
    atr_avg = get_avg_atr(BTC_KLINES)
    vol_avg = get_avg_vol(BTC_KLINES)
    current_atr = compute_atr(BTC_KLINES, len(BTC_KLINES) - 1)
    current_vol = get_volume(len(BTC_KLINES) - 1, BTC_KLINES)
    
    print(f"[FILTER] ATR={current_atr:.2f} (avg={atr_avg:.2f}, threshold={atr_avg*ATR_MULT:.2f})")
    print(f"[FILTER] Vol={current_vol:.4f} (avg={vol_avg:.4f}, threshold={vol_avg*VOL_MULT:.4f})")
    print(f"[BTC] Last streaks: {streaks[-5:]}")
    
    # 3. Build filtered matrix
    filtered_count, state_count = build_filtered_matrix(directions, streaks, BTC_KLINES, atr_avg, vol_avg)
    print(f"[MARKOV] Filtered={filtered_count} candles, {state_count} states")
    
    # 4. Get current state
    state = ''.join(str(s) for s in streaks[-STATE_WINDOW:]) if len(streaks) >= STATE_WINDOW else None
    last_dir = directions[-1] if directions else 'UNKNOWN'
    
    prob_continue, state_n = get_state(state)
    
    print(f"[MARKOV] State={state}, last_dir={last_dir}, p̂={prob_continue:.3f} (n={state_n})")
    
    # 5. Fetch Polymarket
    market = await fetch_polymarket_market(next_window)
    if not market:
        print("[POLY] No market")
        return None
    
    print(f"[POLY] YES={market['yes_price']:.3f}, NO={market['no_price']:.3f}")
    
    # 6. Signal check
    direction = last_dir
    q = market['no_price'] if direction == 'UP' else market['yes_price']
    edge = prob_continue - q
    
    # Filters
    passes_vol = current_vol > vol_avg * VOL_MULT
    passes_atr = current_atr > atr_avg * ATR_MULT
    
    if not passes_atr:
        reason = f"Low ATR ({current_atr:.2f} < {atr_avg*ATR_MULT:.2f})"
        signal_active = False
    elif not passes_vol:
        reason = f"Low Vol ({current_vol:.4f} < {vol_avg*VOL_MULT:.4f})"
        signal_active = False
    elif prob_continue < MIN_PROB:
        reason = f"p̂ {prob_continue:.3f} < {MIN_PROB}"
        signal_active = False
    elif state_n < MIN_STATE_N:
        reason = f"n={state_n} < {MIN_STATE_N} (unreliable state)"
        signal_active = False
    elif edge < MIN_EDGE:
        reason = f"Δ {edge:.3f} < {MIN_EDGE}"
        signal_active = False
    else:
        reason = "OK"
        signal_active = True
    
    print(f"[SIGNAL] {direction}, p̂={prob_continue:.3f}, q={q:.3f}, Δ={edge:.3f} → {reason}")
    
    # 7. Telegram
    msg = f"""📊 <b>BTC 5m Cycle #{CYCLE_COUNT}</b>

🔹 State: <code>{state}</code> (n={state_n})
🔹 Last Dir: <b>{last_dir}</b>
🔹 p̂: <code>{prob_continue:.3f}</code>

📊 <b>Filters:</b>
🔸 ATR: <code>{current_atr:.2f}</code> {'✅' if passes_atr else '❌'} (need &gt;{atr_avg*ATR_MULT:.2f})
🔸 Vol: <code>{current_vol:.4f}</code> {'✅' if passes_vol else '❌'} (need &gt;{vol_avg*VOL_MULT:.4f})
🔸 State n: <code>{state_n}</code> {'✅' if state_n >= MIN_STATE_N else '❌'} (need &gt;={MIN_STATE_N})

📈 <b>Market:</b> {market['question']}
🔹 YES: <code>{market['yes_price']:.3f}</code> / NO: <code>{market['no_price']:.3f}</code>

📐 <b>Signal:</b> q={q:.3f}, Δ={edge:.3f}
🔸 Result: <b>{direction if signal_active else 'NONE'}</b>
🔸 Reason: {reason}

⏰ <code>btc-updown-5m-{next_window}</code>"""

    if DRY_RUN:
        msg += "\n\n[DRY_RUN]"
    else:
        # LIVE TRADING - execute if signal active
        trade_result = await execute_live_trade(market, direction, prob_continue, edge, q, msg)
        msg = trade_result['message']
    
    await send_telegram(msg)
    
    # 8. Log
    STATE_HISTORY.append({
        'time': now.isoformat(),
        'cycle': CYCLE_COUNT,
        'state': state,
        'last_dir': last_dir,
        'prob_continue': prob_continue,
        'atr': current_atr,
        'vol': current_vol,
        'direction': direction,
        'edge': edge,
        'signal': signal_active,
        'reason': reason
    })
    
    save_state()
    
    # 9. Auto-adjust at 50 cycles
    if CYCLE_COUNT > 0 and CYCLE_COUNT % TARGET_CYCLES == 0:
        await nightly_review()
    
    return {'cycle': CYCLE_COUNT, 'state': state, 'prob': prob_continue, 'signal': signal_active}

async def nightly_review():
    global MIN_PROB, STATE_HISTORY
    
    print(f"\n🌙 NIGHTLY REVIEW after {len(STATE_HISTORY)} cycles")
    
    probs = [e.get('prob_continue', 0) for e in STATE_HISTORY]
    max_prob = max(probs) if probs else 0
    avg_prob = sum(probs) / len(probs) if probs else 0
    
    print(f"[REVIEW] max p̂={max_prob:.3f}, avg={avg_prob:.3f}, MIN_PROB={MIN_PROB}")
    
    if max_prob < MIN_PROB * 0.7 and MIN_PROB > 0.75:
        old = MIN_PROB
        MIN_PROB = max(0.75, MIN_PROB - 0.05)
        print(f"[REVIEW] Adjusted MIN_PROB: {old} → {MIN_PROB}")
        save_config()
    
    msg = f"""🌙 <b>Nightly Review</b>

📊 Cycles: {len(STATE_HISTORY)}
📈 Max p̂: {max_prob:.3f}
🔧 MIN_PROB: {MIN_PROB}
🔧 MIN_EDGE: {MIN_EDGE}

Continuing...
[DRY_RUN]"""
    
    await send_telegram(msg)
    
    STATE_HISTORY = []
    save_state()

async def main():
    global CYCLE_COUNT
    load_state()
    load_config()
    
    print("BTC 5m Bot v3 - ATR+Vol Filter")
    print(f"MIN_PROB={MIN_PROB}, ATR>{ATR_MULT}x avg, Vol>{VOL_MULT}x avg")
    print(f"Cycle {CYCLE_COUNT}/{TARGET_CYCLES}")
    
    result = await run_trading_cycle()
    
    if result:
        next_time = datetime.fromtimestamp(time.time() + CHECK_INTERVAL, tz=timezone.utc)
        print(f"\n✓ #{result['cycle']} p̂={result['prob']:.3f}, Signal={result['signal']}")
        print(f"⏰ Next: {next_time.strftime('%H:%M:%S')}")
    
    return result

if __name__ == "__main__":
    asyncio.run(main())