#!/bin/bash
# BTC 5m Bot Auto-Runner
# Runs every 81 seconds continuously

cd /home/node/.openclaw/workspace/lgtm-trade

while true; do
    echo "=== $(date -u) ===" >> btc_5m_runner.log
    python3 btc_5m_bot.py >> btc_5m_runner.log 2>&1
    echo "---" >> btc_5m_runner.log
    sleep 81
done
