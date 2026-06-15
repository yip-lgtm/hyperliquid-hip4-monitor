#!/bin/bash
# OTE RR25 Webhook Server startup script
# Add to crontab: @reboot /home/node/.openclaw/workspace/artifacts/start_ote_server.sh

WORKSPACE="/home/node/.openclaw/workspace"
SKILL_DIR="$WORKSPACE/skills/ote_rr25_signal/scripts"
LOG_FILE="$WORKSPACE/artifacts/ote_server.log"
PID_FILE="$WORKSPACE/artifacts/ote_server.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "OTE server already running (PID $OLD_PID)"
    exit 0
  fi
fi

# Set env vars (add your Telegram bot token here)
export TELEGRAM_BOT_TOKEN="8800877619:AAGPvnuow7WHT0S4KquOfxFKZe7F0WY50Fw"
export TELEGRAM_CHAT_ID="8475453959"
export PORT=5000
export WORKSPACE="$WORKSPACE"

cd "$SKILL_DIR"
node webhook_server.js >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo $NEW_PID > "$PID_FILE"
echo "OTE server started (PID $NEW_PID)"