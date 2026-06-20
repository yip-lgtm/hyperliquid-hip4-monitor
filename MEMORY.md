# MEMORY.md - Long-Term Memory

## Identity
- Assistant running in OpenClaw on a Linux host
- Workspace: /home/node/.openclaw/workspace

## User Info
- Primary user: Saba / yip611
- User cares about: trading systems, OTE signals, HIP-4 prediction markets, Hyperliquid

## Active Projects

### OTE Multi Bot
- Location: ~/.openclaw/workspace/skills/ote_multi_bot/
- Monitors MBT, MNQ, MGC via 3min cron
- MBT working; MNQ/MGC missing polygon_api_key
- PNL currently $0

### HIP-4 Backtester V2 ✅
- Upgraded from V1 to V2 with real-data support
- HIP4BacktesterV2 class added at line 1709 in monitor.py
- Supports incremental updates and settled signal tracking

### Hyperliquid HIP4 Monitor
- Location: ~/.openclaw/workspace/skills/hyperliquid_hip4_monitor/
- Tracks canonical prediction markets, validator votes, arb opportunities

## Notes
- Memory flush mode can block writes — if a write fails, code is stored in memory/*.md files
- Polygon API key needed for MNQ and MGC in OTE bot
