#!/usr/bin/env python3
"""
MNQ ORB Backtester v2
Fixed Opening Range Breakout strategy for MNQ (Micro E-mini Nasdaq-100)

Usage:
  python3 mnq_orb_backtest.py                    # Run with sample data
  python3 mnq_orb_backtest.py mnq_1min.csv     # Run with your data

Data format: Datetime, Open, High, Low, Close, Volume
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime, time

# ── ORB Strategy ────────────────────────────────────────────────
class MNQ_ORB:
    def __init__(self, risk_ticks=35, target_ticks=80,
                 orb_start='09:30', orb_window_minutes=15,
                 session_end='16:00'):
        self.risk_ticks = risk_ticks      # Stop: 35 ticks × $0.25 = $8.75
        self.target_ticks = target_ticks  # Target: 80 ticks × $0.25 = $20.00
        self.orb_start = orb_start          # "09:30"
        self.orb_window = orb_window_minutes  # 15 = 9:30-9:45
        self.session_end = session_end     # "16:00"
        self.tick_value = 0.25             # MNQ: $0.25 per tick

    def run(self, df):
        df = df.copy()
        # Handle mixed timezones (EDT/EST transition)
        df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
        df = df.set_index('Datetime').sort_index()
        # Convert to US/Eastern for time-based filtering
        df['us_eastern'] = df.index.tz_convert('US/Eastern')
        df['time'] = df['us_eastern'].dt.time

        signals = []

        orb_high = orb_low = None
        traded_today = False
        today_date = None

        t_start = datetime.strptime(self.orb_start, '%H:%M').time()
        t_orb_end = (datetime.combine(datetime.min, t_start)
                     + pd.Timedelta(minutes=self.orb_window)).time()
        t_end = datetime.strptime(self.session_end, '%H:%M').time()

        for i, (ts, row) in enumerate(df.iterrows()):
            t = row['time']
            bar_date = ts.date()

            # Outside session → reset
            if t < t_start or t > t_end:
                continue

            # New day reset
            if bar_date != today_date:
                today_date = bar_date
                traded_today = False
                orb_high = orb_low = None

            # ── Build ORB range ──────────────────────────────
            if t < t_orb_end:
                h, l = float(row['High']), float(row['Low'])
                orb_high = h if orb_high is None else max(orb_high, h)
                orb_low  = l if orb_low  is None else min(orb_low, l)
                continue

            if traded_today or orb_high is None:
                continue

            # ── Entry signals ──────────────────────────────
            close = float(row['Close'])
            upper = orb_high + self.target_ticks * self.tick_value
            lower = orb_low  - self.target_ticks * self.tick_value

            direction = None
            if close > upper:
                direction = 'LONG'
            elif close < lower:
                direction = 'SHORT'

            if direction is None:
                continue

            # ── Exit ───────────────────────────────────────
            entry_price = close
            stop_price = (entry_price - self.risk_ticks * self.tick_value
                           if direction == 'LONG'
                           else entry_price + self.risk_ticks * self.tick_value)
            target_price = (entry_price + self.target_ticks * self.tick_value
                            if direction == 'LONG'
                            else entry_price - self.target_ticks * self.tick_value)

            pnl_ticks = 0
            for j in range(i + 1, min(i + 24, len(df))):
                hi, lo = float(df.iloc[j]['High']), float(df.iloc[j]['Low'])
                if direction == 'LONG':
                    if hi >= target_price:
                        pnl_ticks = self.target_ticks
                        break
                    elif lo <= stop_price:
                        pnl_ticks = -self.risk_ticks
                        break
                else:
                    if lo <= target_price:
                        pnl_ticks = self.target_ticks
                        break
                    elif hi >= stop_price:
                        pnl_ticks = -self.risk_ticks
                        break

            # Time expired → close at last bar mid
            if pnl_ticks == 0:
                last_close = float(df.iloc[min(i + 24, len(df) - 1)]['Close'])
                pnl_ticks = (last_close - entry_price) / self.tick_value if direction == 'LONG' \
                            else (entry_price - last_close) / self.tick_value

            signals.append({
                'entry_time': ts,
                'direction': direction,
                'entry_price': entry_price,
                'pnl_ticks': pnl_ticks,
                'pnl_dollar': pnl_ticks * self.tick_value,
                'orb_high': orb_high,
                'orb_low': orb_low,
            })
            traded_today = True

        if not signals:
            return _empty_stats()

        sig_df = pd.DataFrame(signals)
        wins = sig_df[sig_df['pnl_ticks'] > 0]
        losses = sig_df[sig_df['pnl_ticks'] < 0]

        return {
            'total_trades': len(signals),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': len(wins) / len(signals) * 100,
            'total_pnl_ticks': sig_df['pnl_ticks'].sum(),
            'total_pnl_dollar': sig_df['pnl_dollar'].sum(),
            'avg_win_ticks': wins['pnl_ticks'].mean() if len(wins) > 0 else 0,
            'avg_loss_ticks': losses['pnl_ticks'].mean() if len(losses) > 0 else 0,
            'rr': abs(wins['pnl_ticks'].mean() / losses['pnl_ticks'].mean())
                   if len(losses) > 0 and len(wins) > 0 else 0,
            'profit_factor': len(wins) / max(len(losses), 1),
            'signals_df': sig_df,
        }

def _empty_stats():
    return dict(total_trades=0, wins=0, losses=0, win_rate=0,
                 total_pnl_ticks=0, total_pnl_dollar=0,
                 avg_win_ticks=0, avg_loss_ticks=0, rr=0, profit_factor=0,
                 signals_df=pd.DataFrame())

# ── Main ──────────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Generating realistic sample MNQ data...")
        np.random.seed(42)
        n_days = 30
        n_per_day = 390  # 6.5 hours × 60 min
        dates = []
        base = 21000
        price = base

        rows = []
        for day in range(n_days):
            day_drift = np.random.randn() * 0.002
            for minute in range(n_per_day):
                minute_of_day = 9*60+30 + minute  # start 09:30
                h, m = minute_of_day // 60, minute_of_day % 60
                ts = pd.Timestamp(f'2026-06-{1+day%28:02d} {h:02d}:{m:02d}:00', tz='UTC')
                ret = np.random.randn() * 0.0003 + day_drift / n_per_day
                price = price * (1 + ret)
                o = price * (1 + np.random.randn() * 0.0001)
                c = price
                hi = max(o, c) + abs(np.random.randn() * 0.0002 * price)
                lo = min(o, c) - abs(np.random.randn() * 0.0002 * price)
                rows.append({'Datetime': ts, 'Open': o, 'High': hi,
                             'Low': lo, 'Close': c, 'Volume': np.random.randint(50, 500)})

        df = pd.DataFrame(rows)
        data_note = " (SAMPLE — provide real MNQ 1min CSV to backtest)"
    else:
        df = pd.read_csv(sys.argv[1])
        data_note = f" ({sys.argv[1]})"

    print(f"\nData: {len(df)} rows{data_note}")

    strat = MNQ_ORB(risk_ticks=35, target_ticks=80)
    s = strat.run(df)

    print(f"\n{'='*55}")
    print(f"MNQ ORB Backtest")
    print(f"{'='*55}")
    print(f"Strategy: ORB 9:30-9:45 | Risk={strat.risk_ticks}t | Target={strat.target_ticks}t")
    print(f"{'-'*55}")
    print(f"Trades:        {s['total_trades']} ({s['wins']}W / {s['losses']}L)")
    print(f"Win Rate:      {s['win_rate']:.1f}%")
    print(f"P&L:           {s['total_pnl_ticks']:.1f} ticks = ${s['total_pnl_dollar']:.2f}")
    print(f"Avg Win:       {s['avg_win_ticks']:.1f} ticks")
    print(f"Avg Loss:      {s['avg_loss_ticks']:.1f} ticks")
    print(f"RR:            {s['rr']:.2f}:1")
    print(f"Profit Factor: {s['profit_factor']:.2f}")
    print(f"\nNote: Risk ${strat.risk_ticks * strat.tick_value:.2f} / Trade | Target ${strat.target_ticks * strat.tick_value:.2f}")
    print(f"Note: Sample data unlikely to produce realistic ORB patterns — use real MNQ data")
