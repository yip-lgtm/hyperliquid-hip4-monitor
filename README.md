# HIP-4 Monitor & Backtester

Monitor Hyperliquid HIP-4 outcome markets for arbitrage opportunities vs Polymarket, with LLM-powered semantic matching, REST trading engine, and backtesting.

## Features

- **Real-time Monitoring** — WebSocket price feed + REST polling every 15 min
- **LLM Semantic Matching** — Minimax-powered event matching between HL and PM markets
- **Arbitrage Detection** — Cross-platform price diff calculation with configurable threshold
- **REST Trading Engine** — Dry-run by default, pre-trade risk checks, EIP-712 signing
- **Backtester V2** — Parameter scan, equity curve, profit factor, Sharpe/Sortino ratios
- **Git Auto-Backup** — Every 4h via cron job

## Quick Start

```bash
# Run monitor (fast mode, no LLM)
python3 -c "from skills.hyperliquid_hip4_monitor.monitor import HIP4Monitor; m=HIP4Monitor(); r=m.execute(ws_enabled=True,fast_mode=True); print(m.get_summary() if r.get('arb_signals') else 'No signal')"

# Run backtest
python3 -c "from skills.hyperliquid_hip4_monitor.monitor import HIP4BacktesterV2; bt=HIP4BacktesterV2(); r=bt.run_backtest(); print(bt.format_report(r))"

# Polymarket backtest script
python3 skills/hyperliquid_hip4_monitor/run_polymarket_backtest.py --samples 65
```

## Configuration

Edit `skills/hyperliquid_hip4_monitor/config.json`:

| Key | Default | Description |
|-----|---------|-------------|
| `arb_threshold` | 0.02 | Min diff to trigger signal (2%) |
| `position_size_pct` | 0.05 | % of account value per trade |
| `dry_run` | true | Live trading disabled by default |
| `confidence_threshold` | 0.72 | LLM match confidence min |
| `semantic_cache_ttl_minutes` | 90 | LLM cache TTL |

## Architecture

```
WebSocket / REST API
        ↓
  HL HIP-4 Markets (validatorL1Votes + allMids)
        ↓
  Polymarket Gamma API
        ↓
  SemanticMatcher (LLM keyword pre-filter)
        ↓
  Arb Signal Detection (diff > threshold)
        ↓
  RestTradingEngine (dry-run / risk checks)
        ↓
  BacktesterV2 (equity curve + stats)
```

## Strategy

| Parameter | Value |
|-----------|-------|
| Trigger | HL vs PM diff ≥ 2% |
| Execute | diff ≥ 3.2% + vote ≥ 80% |
| Size | min(account×5%, $100) |
| Exit | diff < 1% or settled |
| Mode | dry_run (default) |

## Project Structure

```
skills/hyperliquid_hip4_monitor/
├── monitor.py                  # Main: HIP4Monitor, RestTradingEngine, SemanticMatcher, HIP4BacktesterV2
├── config.json                 # All configuration
├── daemon.py                   # Long-running WS background process
├── backup.py                   # Git auto-commit + push
├── run_polymarket_backtest.py  # One-click Polymarket → backtest pipeline
├── hip4_events.jsonl           # Event and trade logs
├── hip4_performance.json       # Backtest signals
└── README.md                  # This directory's README
```

## Status

- Real arb signals: **0** (HL has 20 World Cup markets only)
- Mock backtest: 15 esports signals → +0.88% (100% win rate, unrealistic)
- WS daemon: running (PID in `/tmp/hip4_daemon.log`)
- Cron: every 15 min, announce-on-signal, silent otherwise

## License

Educational use only. Use at your own risk.
