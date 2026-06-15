#!/usr/bin/env node
/**
 * OTE RR25 Webhook Server v2
 * - Accepts pre-calculated target from TradingView alert
 * - Falls back to own fib calculation if target not provided
 * - Strict direction validation: LONG targets must be above entry, SHORT below
 */
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const WORKSPACE = process.env.WORKSPACE || '/home/node/.openclaw/workspace';
const ARTIFACTS = path.join(WORKSPACE, 'artifacts');
const PNL_FILE = path.join(ARTIFACTS, 'daily_pnl.json');
const SIGNAL_LOG = path.join(ARTIFACTS, 'ote_signals.json');
const TEST_LOG = path.join(ARTIFACTS, 'ote_test_log.json');

const PORT = parseInt(process.env.PORT || '5000');
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '8475453959';
const TEST_MODE = process.env.TEST_MODE === 'true';

const KILL_SWITCH = 90;
const MAX_SIGNALS_PER_DAY = 2;
const MIN_RR = 2.5;
const MAX_RISK_USD = 80;

// ─── P&L ─────────────────────────────────────────────────────
function loadPNL() {
  try {
    if (!fs.existsSync(PNL_FILE)) return freshPNL();
    const data = JSON.parse(fs.readFileSync(PNL_FILE, 'utf8'));
    const today = new Date().toISOString().split('T')[0];
    if (data.date !== today) return freshPNL();
    return data;
  } catch { return freshPNL(); }
}

function freshPNL() {
  return { date: new Date().toISOString().split('T')[0], total_pnl: 0, signal_count: 0, signals: [] };
}

function savePNL(data) {
  if (!fs.existsSync(ARTIFACTS)) fs.mkdirSync(ARTIFACTS, { recursive: true });
  fs.writeFileSync(PNL_FILE, JSON.stringify(data, null, 2));
}

function logSignal(signal, isTest) {
  const target = isTest ? TEST_LOG : SIGNAL_LOG;
  let logs = [];
  try { if (fs.existsSync(target)) logs = JSON.parse(fs.readFileSync(target, 'utf8')); } catch {}
  logs.push(signal);
  if (!fs.existsSync(ARTIFACTS)) fs.mkdirSync(ARTIFACTS, { recursive: true });
  fs.writeFileSync(target, JSON.stringify(logs, null, 2));
}

// ─── Signal Logic ─────────────────────────────────────────────
/**
 * TradingView alert payload:
 * {
 *   close: float,           // current price / entry
 *   swing_low: float,
 *   swing_high: float,
 *   daily_bias: "bullish" | "bearish",
 *   target?: float,         // OPTIONAL: pre-calculated TP from TradingView
 *   sl?: float,             // OPTIONAL: stop loss override
 *   ote_zone_hit?: boolean,
 *   kill_zone?: boolean,
 *   ltf_rejection?: boolean,
 *   mss_confirmed?: boolean
 * }
 */
function checkSignal(data) {
  const pnl = loadPNL();

  // Safety checks
  if (pnl.total_pnl <= -KILL_SWITCH) return null;
  if (pnl.signal_count >= MAX_SIGNALS_PER_DAY) return null;

  const {
    close, swing_low, swing_high, daily_bias,
    target, sl: provided_sl,
    ote_zone_hit = true, kill_zone = false
  } = data;

  if (!close || !swing_low || !swing_high || !daily_bias) return null;
  if (!['bullish', 'bearish'].includes(daily_bias)) return null;

  const direction = daily_bias;
  const entry = close;

  // Determine stop loss
  let sl;
  if (provided_sl !== undefined && provided_sl !== null) {
    sl = provided_sl;
  } else {
    sl = direction === 'bullish' ? swing_low : swing_high;
  }

  // Calculate risk
  const risk = direction === 'bullish' ? entry - sl : sl - entry;
  if (risk <= 0) return null;

  const riskTicks = risk / 0.25;
  const riskUsd = riskTicks * 2; // 2 contracts
  if (riskUsd > MAX_RISK_USD) return null;

  // Determine target
  let finalTarget;
  if (target !== undefined && target !== null) {
    // Use TradingView-provided target
    finalTarget = target;
  } else {
    // Fallback: fib extension from swing range
    const swingRange = swing_high - swing_low;
    if (direction === 'bullish') {
      // For bullish: look for target ABOVE entry (runner to new high extension)
      // Use -0.5 and -1 extension from swing low, but require target > entry
      const targetMinus05 = swing_low - (swingRange * 0.5);
      const targetMinus1 = swing_low - (swingRange * 1.0);
      // Pick the one that's above entry (valid for LONG)
      const candidates = [targetMinus05, targetMinus1].filter(t => t > entry);
      if (candidates.length === 0) return null; // no valid bullish target
      finalTarget = Math.max(...candidates);
    } else {
      // For bearish: target must be BELOW entry
      const targetMinus05 = swing_high + (swingRange * 0.5);
      const targetMinus1 = swing_high + (swingRange * 1.0);
      const candidates = [targetMinus05, targetMinus1].filter(t => t < entry);
      if (candidates.length === 0) return null;
      finalTarget = Math.min(...candidates);
    }
  }

  // Validate target direction
  if (direction === 'bullish' && finalTarget <= entry) return null;
  if (direction === 'bearish' && finalTarget >= entry) return null;

  // Calculate RR
  const reward = direction === 'bullish'
    ? finalTarget - entry
    : entry - finalTarget;
  if (reward <= 0) return null;

  const rr = reward / risk;
  if (rr < MIN_RR) return null;

  // Partial target (60%)
  const target1 = direction === 'bullish'
    ? entry + reward * 0.6
    : entry - reward * 0.6;

  const signal = {
    type: 'OTE_RR25',
    direction: direction === 'bullish' ? 'LONG' : 'SHORT',
    entry: Math.round(entry * 100) / 100,
    sl: Math.round(sl * 100) / 100,
    target1: Math.round(target1 * 100) / 100,
    target2: Math.round(finalTarget * 100) / 100,
    rr: Math.round(rr * 100) / 100,
    risk_usd: Math.round(riskUsd * 100) / 100,
    size: 2,
    time: new Date().toISOString(),
    note: `RR ${Math.round(rr*100)/100} | Risk $${Math.round(riskUsd*100)/100} | Kill zone: ${kill_zone}`
  };

  // Update PNL tracking
  pnl.signal_count = (pnl.signal_count || 0) + 1;
  pnl.signals = pnl.signals || [];
  pnl.signals.push({ time: signal.time, rr: signal.rr, risk_usd: signal.risk_usd });
  savePNL(pnl);
  logSignal(signal, TEST_MODE);

  return signal;
}

// ─── Telegram ────────────────────────────────────────────────
function sendTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN) {
    console.log('[WARN] TELEGRAM_BOT_TOKEN not set');
    return Promise.resolve(false);
  }
  const body = JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'Markdown' });
  const options = {
    hostname: 'api.telegram.org',
    path: `/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  };
  return new Promise((resolve) => {
    const req = https.request(options, (res) => {
      let d = '';
      res.on('data', chunk => d += chunk);
      res.on('end', () => { console.log('[TG] Status:', res.statusCode); resolve(true); });
    });
    req.on('error', e => { console.error('[TG] Error:', e.message); resolve(false); });
    req.write(body);
    req.end();
  });
}

function formatSignal(sig) {
  const emoji = sig.direction === 'LONG' ? '🟢' : '🔴';
  return [
    `${emoji} *OTE RR25 SIGNAL*`,
    ``,
    `*Direction:* ${sig.direction}`,
    `*Entry:* \`${sig.entry}\``,
    `*SL:* \`${sig.sl}\``,
    `*Target 1 (60%):* \`${sig.target1}\``,
    `*Target 2 (RR2.5):* \`${sig.target2}\``,
    `*RR:* \`${sig.rr}\``,
    `*Risk:* \`$${sig.risk_usd}\``,
    `*Size:* \`${sig.size} contracts\``,
    ``,
    `_${sig.note}_`
  ].join('\n');
}

// ─── HTTP Server ─────────────────────────────────────────────
function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try { resolve(JSON.parse(body)); } catch { reject(new Error('Invalid JSON')); }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = req.url;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' });
    res.end();
    return;
  }

  try {
    if (req.method === 'POST' && url === '/webhook/ote') {
      const data = await parseBody(req);
      console.log('[WEBHOOK]', JSON.stringify(data));
      const signal = checkSignal(data);
      if (signal) {
        console.log('[SIGNAL]', JSON.stringify(signal));
        if (!TEST_MODE) await sendTelegram(formatSignal(signal));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'signal_sent', signal }));
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'no_signal', reason: 'conditions_not_met' }));
      }
      return;
    }

    if (req.method === 'GET' && url === '/health') {
      const pnl = loadPNL();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok', date: pnl.date, total_pnl: pnl.total_pnl,
        signal_count: pnl.signal_count, kill_switch_triggered: pnl.total_pnl <= -KILL_SWITCH
      }));
      return;
    }

    if (req.method === 'GET' && url === '/pnl') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(loadPNL()));
      return;
    }

    if (req.method === 'POST' && url === '/pnl') {
      const data = await parseBody(req);
      const pnl = loadPNL();
      if ('total_pnl' in data) pnl.total_pnl = data.total_pnl;
      pnl.last_updated = new Date().toISOString();
      savePNL(pnl);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', pnl }));
      return;
    }

    if (req.method === 'POST' && url === '/reset-day') {
      savePNL(freshPNL());
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'reset' }));
      return;
    }

    if (req.method === 'POST' && url === '/reset-pnl') {
      // Manual P&L reset (for testing)
      const pnl = loadPNL();
      pnl.total_pnl = 0;
      pnl.last_updated = new Date().toISOString();
      savePNL(pnl);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', pnl }));
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
  } catch (e) {
    console.error('[ERROR]', e.message);
    res.writeHead(500);
    res.end(JSON.stringify({ error: e.message }));
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`OTE RR25 v2 server on port ${PORT} | TEST_MODE=${TEST_MODE}`);
});