#!/bin/bash
# OTE Multi Bot startup script
# Add to crontab: @reboot /home/node/.openclaw/workspace/artifacts/start_ote_multi_bot.sh

WORKSPACE="/home/node/.openclaw/workspace"
BOT_DIR="$WORKSPACE/skills/ote_multi_bot"
LOG_FILE="$WORKSPACE/artifacts/ote_multi_bot.log"
PID_FILE="$WORKSPACE/artifacts/ote_multi_bot.pid"

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "OTE multi bot already running (PID $OLD_PID)"
    exit 0
  fi
fi

export WORKSPACE="$WORKSPACE"

cd "$BOT_DIR"
node ote_multi_bot.js >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo $NEW_PID > "$PID_FILE"
echo "OTE multi bot started (PID $NEW_PID)"