---
name: ote_multi_bot
description: "Self-fetching OTE RR≥2.5 signal bot for MBT/MNQ/MGC. $100 kill-switch, max 2 signals/day, kill zone filter."
metadata:
  symbols: [MBT, MNQ, MGC]
  min_rr: 2.5
  max_risk_usd: 80
  kill_switch: 100
  max_signals_per_day: 2
---

# OTE Multi Bot

Self-contained OTE signal generator. No TradingView webhook required. Pulls data directly from exchanges.

## Supported Symbols

| Symbol | Exchange | Method |
|--------|----------|--------|
| MBT | Binance | REST short-poll |
| MNQ | Polygon.io | REST short-poll |
| MGC | Polygon.io | REST short-poll |

## Setup

1. Edit `config.json` with your Polygon.io API key (free tier)
2. Test: `node ote_multi_bot.js`
3. Add to cron: `*/3 * * * *` (every 3 minutes)

## Signal Conditions

- RR ≥ 2.5
- Risk ≤ $80 per trade
- Daily P&L > -$100 (kill-switch)
- Signal count < 2 today
- In kill zone (London 8-10 AM ET, NY 2-4 PM ET)

## Output

- Telegram message per signal
- Log: `artifacts/ote_multi_bot.log`
- Signals: `artifacts/ote_signals.json`
- P&L: `artifacts/daily_pnl.json`

## Config

See `config.json` for per-symbol settings (enabled, poll interval, contracts).