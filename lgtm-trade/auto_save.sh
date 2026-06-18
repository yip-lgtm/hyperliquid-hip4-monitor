#!/bin/bash
# Auto-save BTC 5m Markov Bot results to git
# Run: git add + commit + push

cd /home/node/.openclaw/workspace/lgtm-trade

# Add all relevant files
git add btc_5m_bot.py trade_tracker.json btc_5m_runner.log 2>/dev/null

# Only commit if there are changes
if git diff --cached --quiet; then
    echo "No changes to commit"
    exit 0
fi

# Commit with stats
STATS=$(cat trade_tracker.json 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    s=d.get('stats',{})
    print(f\"WR={s.get('win_rate',0)}% PnL=\${s.get('pnl',0)} {s.get('wins',0)}W/{s.get('losses',0)}L\")
except: print('no stats')
" 2>/dev/null)

git commit -m "Auto-save BTC 5m | $(date -u '+%Y-%m-%d %H:%M UTC') | $STATS" 2>/dev/null

# Push to remote
cd /home/node/.openclaw/workspace
git push origin master 2>/dev/null

echo "Saved: $STATS"
