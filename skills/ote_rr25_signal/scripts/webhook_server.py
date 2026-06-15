#!/usr/bin/env python3
"""
OTE RR25 Webhook Server - 50K Challenge Edition
Receives TradingView alerts → validates → calculates OTE signal → Telegram
"""
import os
import json
import logging
from datetime import datetime, date
from pathlib import Path
from flask import Flask, request, jsonify

WORKSPACE = Path(os.environ.get("WORKSPACE", "/home/node/.openclaw/workspace"))
import sys
sys.path.insert(0, str(WORKSPACE / "skills" / "ote_rr25_signal" / "scripts"))

from ote_signal_check import check_ote_signal, load_pnl, save_pnl, log_signal, infer_daily_bias, in_ote_zone

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8475453959")
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

ARTIFACTS = WORKSPACE / "artifacts"
ARTIFACTS.mkdir(parents=True, exist_ok=True)


def send_telegram_message(text):
    """Send message via Telegram bot."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set, skipping Telegram notification")
        return False
    import urllib.request
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            return True
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def format_signal_message(signal):
    """Format OTE signal as readable Telegram message."""
    direction_emoji = "🟢" if signal["direction"] == "LONG" else "🔴"
    lines = [
        f"{direction_emoji} *OTE RR25 SIGNAL*",
        f"",
        f"*Symbol:* {signal.get('symbol', 'MNQ')}",
        f"*Direction:* {signal['direction']}",
        f"*Entry:* {signal['entry']}",
        f"*SL:* {signal['sl']}",
        f"*Partial (60%):* {signal['target1']}",
        f"*Target (RR2.5):* {signal['target2']}",
        f"*Runner (-1.0):* {signal.get('runner_target', 'N/A')}",
        f"",
        f"*RR:* {signal['rr']}",
        f"*Risk:* ${signal['risk_usd']}",
        f"*Size:* {signal['size']} contracts",
        f"",
        f"*Swing Low:* {signal.get('swing_low', 'N/A')}",
        f"*Swing High:* {signal.get('swing_high', 'N/A')}",
        f"*Zone:* {signal.get('ote_zone_hit', False)} | *Reject:* {signal.get('ltf_rejection', False)} | *MSS:* {signal.get('mss_confirmed', False)}",
        f"",
        f"_{signal.get('note', '')}_"
    ]
    return "\n".join(lines)


@app.route("/webhook/ote", methods=["POST"])
def webhook_ote():
    """
    TradingView alert JSON:
    {
      "symbol": "MNQ",
      "close": 19845.50,
      "swing_low": 19750.00,
      "swing_high": 19950.00,
      "daily_bias": "bullish",        (optional, inferred if missing)
      "ote_zone_hit": true,           (optional, inferred if missing)
      "ltf_rejection": true,         (optional)
      "mss_confirmed": true,         (optional)
      "kill_zone": false,            (optional)
      "time": "2026-06-12T09:30:00Z"
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "no json body"}), 400

        logger.info(f"Webhook received: {json.dumps(data)}")

        # Infer missing fields
        if "daily_bias" not in data or not data["daily_bias"]:
            swing_low = data.get("swing_low")
            swing_high = data.get("swing_high")
            close = data.get("close")
            if swing_low and swing_high and close:
                data["daily_bias"] = infer_daily_bias(close, swing_low, swing_high)

        if "ote_zone_hit" not in data:
            swing_low = data.get("swing_low")
            swing_high = data.get("swing_high")
            close = data.get("close")
            if swing_low and swing_high and close:
                data["ote_zone_hit"] = in_ote_zone(close, swing_low, swing_high)

        # Check signal
        signal = check_ote_signal(data)

        if signal:
            logger.info(f"SIGNAL TRIGGERED: {json.dumps(signal)}")

            if TEST_MODE:
                logger.info("TEST MODE - skipping Telegram send")
            else:
                msg = format_signal_message(signal)
                send_telegram_message(msg)

            return jsonify({"status": "signal_sent", "signal": signal}), 200
        else:
            logger.info("No signal - conditions not met")
            return jsonify({"status": "no_signal", "reason": "conditions_not_met", "data_received": data}), 200

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    pnl = load_pnl()
    return jsonify({
        "status": "ok",
        "date": str(date.today()),
        "total_pnl": pnl.get("total_pnl", 0),
        "signal_count": pnl.get("signal_count", 0),
        "kill_switch_triggered": pnl.get("total_pnl", 0) <= -90
    })


@app.route("/pnl", methods=["GET"])
def get_pnl():
    return jsonify(load_pnl())


@app.route("/pnl", methods=["POST"])
def update_pnl():
    try:
        data = request.get_json()
        pnl = load_pnl()
        if "total_pnl" in data:
            pnl["total_pnl"] = data["total_pnl"]
        pnl["last_updated"] = datetime.now().isoformat()
        save_pnl(pnl)
        logger.info(f"P&L updated: {pnl}")
        return jsonify({"status": "ok", "pnl": pnl})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reset-day", methods=["POST"])
def reset_day():
    pnl = {"date": str(date.today()), "total_pnl": 0, "signal_count": 0, "signals": []}
    save_pnl(pnl)
    return jsonify({"status": "reset", "pnl": pnl})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    logger.info(f"Starting OTE webhook server on port {port}, TEST_MODE={TEST_MODE}")
    app.run(host="0.0.0.0", port=port, debug=debug)