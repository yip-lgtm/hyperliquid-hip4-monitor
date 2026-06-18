#!/usr/bin/env node
/**
 * BTC 5min Signal Bot
 * Data: Kraken public OHLC API (no key needed)
 * Signal: RSI + EMA crossover + momentum
 * Config: ~/.openclaw/workspace/skills/ote_multi_bot/config.json
 */

const fs = require('fs');
const https = require('https');

// ── Config ──────────────────────────────────────────────────────────────────
const CONFIG = JSON.parse(fs.readFileSync(__dirname + '/config.json', 'utf8'));
const SYMBOL = 'XXBTZUSD'; // Kraken BTC/USD
const INTERVAL = 5; // 5 minutes
const TELEGRAM = CONFIG.telegram;

// ── Kraken OHLC Fetcher ───────────────────────────────────────────────────────
function fetchKrakenOHLC(symbol, interval = 5, count = 50) {
    return new Promise((resolve, reject) => {
        const url = `https://api.kraken.com/0/public/OHLC?pair=${symbol}&interval=${interval}`;
        const req = https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    if (json.error && json.error.length > 0) return reject(new Error(json.error.join(', ')));
                    const pair = Object.keys(json.result).find(k => k !== 'last');
                    resolve({ candles: json.result[pair], last: json.result.last });
                } catch (e) { reject(e); }
            });
        });
        req.on('error', reject);
        req.setTimeout(10000, () => { req.destroy(); reject(new Error('Request timeout')); });
    });
}

// ── Technical Indicators ────────────────────────────────────────────────────
function calcEMA(prices, period) {
    const k = 2 / (period + 1);
    const ema = [prices[0]];
    for (let i = 1; i < prices.length; i++) {
        ema.push(prices[i] * k + ema[i - 1] * (1 - k));
    }
    return ema;
}

function calcRSI(prices, period = 14) {
    if (prices.length < period + 1) return null;
    let gains = 0, losses = 0;
    for (let i = prices.length - period; i < prices.length; i++) {
        const diff = prices[i] - prices[i - 1];
        if (diff > 0) gains += diff;
        else losses += Math.abs(diff);
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - (100 / (1 + rs));
}

function calcATR(highs, lows, closes, period = 14) {
    if (closes.length < period + 1) return null;
    const trs = [];
    for (let i = 1; i < highs.length; i++) {
        const tr = Math.max(
            highs[i] - lows[i],
            Math.abs(highs[i] - closes[i - 1]),
            Math.abs(lows[i] - closes[i - 1])
        );
        trs.push(tr);
    }
    return trs.slice(-period).reduce((a, b) => a + b, 0) / period;
}

// ── Signal Logic ─────────────────────────────────────────────────────────────
function generateSignal(candles) {
    const closes = candles.map(c => parseFloat(c[4]));
    const opens = candles.map(c => parseFloat(c[1]));
    const highs = candles.map(c => parseFloat(c[2]));
    const lows = candles.map(c => parseFloat(c[3]));
    const volumes = candles.map(c => parseFloat(c[6]));

    const ema9 = calcEMA(closes, 9);
    const ema21 = calcEMA(closes, 21);
    const ema50 = calcEMA(closes, 50);
    const rsi = calcRSI(closes, 14);
    const atr = calcATR(highs, lows, closes, 14);

    const latest = closes.length - 1;
    const prev = latest - 1;

    // EMA crossover
    const ema_cross_up = ema9[prev] < ema21[prev] && ema9[latest] > ema21[latest];
    const ema_cross_down = ema9[prev] > ema21[prev] && ema9[latest] < ema21[latest];

    // EMA trend alignment
    const bull_trend = ema9[latest] > ema21[latest] && ema21[latest] > ema50[latest];
    const bear_trend = ema9[latest] < ema21[latest] && ema21[latest] < ema50[latest];

    // RSI
    const rsi_oversold = rsi !== null && rsi < 35;
    const rsi_overbought = rsi !== null && rsi > 65;

    // Momentum: last 3 candles direction
    const momentum = closes[latest] - closes[prev];

    // Signal conditions
    let direction = null;
    let confidence = 0;
    let reason = '';

    if (ema_cross_up && (rsi_oversold || !rsi_overbought)) {
        direction = 'UP';
        confidence = 70 + (rsi_oversold ? 15 : 0) + (bull_trend ? 10 : 0);
        reason = `EMA cross UP + RSI ${rsi?.toFixed(1)} ${rsi_oversold ? '(oversold)' : ''} ${bull_trend ? '+ bull trend' : ''}`;
    } else if (ema_cross_down && (rsi_overbought || !rsi_oversold)) {
        direction = 'DOWN';
        confidence = 70 + (rsi_overbought ? 15 : 0) + (bear_trend ? 10 : 0);
        reason = `EMA cross DOWN + RSI ${rsi?.toFixed(1)} ${rsi_overbought ? '(overbought)' : ''} ${bear_trend ? '+ bear trend' : ''}`;
    } else if (rsi_oversold && bull_trend && momentum > 0) {
        direction = 'UP';
        confidence = 65;
        reason = `RSI ${rsi?.toFixed(1)} oversold + bull trend + positive momentum`;
    } else if (rsi_overbought && bear_trend && momentum < 0) {
        direction = 'DOWN';
        confidence = 65;
        reason = `RSI ${rsi?.toFixed(1)} overbought + bear trend + negative momentum`;
    }

    return {
        direction,
        confidence,
        reason,
        price: closes[latest],
        ema9: ema9[latest],
        ema21: ema21[latest],
        ema50: ema50[latest],
        rsi,
        atr,
        momentum: momentum.toFixed(2),
        candle_time: new Date(candles[latest][0] * 1000).toISOString(),
    };
}

// ── Telegram ─────────────────────────────────────────────────────────────────
function sendTelegram(text) {
    return new Promise((resolve, reject) => {
        const url = `https://api.telegram.org/bot${TELEGRAM.bot_token}/sendMessage`;
        const body = JSON.stringify({ chat_id: TELEGRAM.chat_id, text, parse_mode: 'HTML' });
        const req = https.request(new URL(url), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
        }, (res) => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => resolve(data));
        });
        req.on('error', reject);
        req.write(body);
        req.end();
    });
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
    const now = new Date();
    const ts = now.toISOString();
    console.log(`[${ts}] BTC 5min signal check`);

    const data = await fetchKrakenOHLC(SYMBOL, INTERVAL, 60);
    const signal = generateSignal(data.candles);

    console.log(`Price: ${signal.price}`);
    console.log(`EMA9: ${signal.ema9.toFixed(2)} | EMA21: ${signal.ema21.toFixed(2)} | EMA50: ${signal.ema50.toFixed(2)}`);
    console.log(`RSI(14): ${signal.rsi?.toFixed(2) || 'N/A'}`);
    console.log(`ATR: ${signal.atr?.toFixed(2) || 'N/A'}`);
    console.log(`Momentum: ${signal.momentum}`);
    console.log(`Signal: ${signal.direction || 'NONE'} (${signal.confidence || 0}%) ${signal.reason}`);

    if (signal.direction && signal.confidence >= 65) {
        const emoji = signal.direction === 'UP' ? '🟢' : '🔴';
        const msg = [
            `${emoji} <b>BTC 5min ${signal.direction}</b>`,
            `Price: <code>$${signal.price.toFixed(2)}</code>`,
            `Confidence: <b>${signal.confidence}%</b>`,
            `RSI: ${signal.rsi?.toFixed(1)} | ATR: ${signal.atr?.toFixed(2)}`,
            `${signal.reason}`,
            `EMA9: ${signal.ema9.toFixed(0)} | EMA21: ${signal.ema21.toFixed(0)} | EMA50: ${signal.ema50.toFixed(0)}`,
            `Time: ${signal.candle_time}`,
        ].join('\n');

        await sendTelegram(msg);
        console.log('Telegram alert sent!');
    } else {
        console.log('No signal (confidence < 65% or no direction)');
    }
}

main().catch(console.error);
