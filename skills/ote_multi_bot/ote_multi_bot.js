#!/usr/bin/env node
/**
 * OTE Multi Bot (MBT + MNQ + MGC)
 * Self-fetching data, RR ≥ 2.5 only, $100 kill-switch
 * MBT: Kraken (confirmed working)
 * MNQ/MGC: Polygon.io (needs free API key in config.json)
 */
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

const WORKSPACE = process.env.WORKSPACE || '/home/node/.openclaw/workspace';
const CONFIG_FILE = path.join(WORKSPACE, 'skills', 'ote_multi_bot', 'config.json');
const ARTIFACTS = path.join(WORKSPACE, 'artifacts');
const PNL_FILE = path.join(ARTIFACTS, 'daily_pnl.json');
const SIGNAL_LOG = path.join(ARTIFACTS, 'ote_signals.json');
const BOT_LOG = path.join(ARTIFACTS, 'ote_multi_bot.log');

const KILL_SWITCH = 100;
const MAX_SIGNALS_PER_DAY = 2;
const MIN_RR = 2.5;
const MAX_RISK_USD = 80;

// ─── Config ─────────────────────────────────────────────────
let CONFIG;
try {
  CONFIG = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
} catch (e) {
  console.error('Failed to load config.json:', e.message);
  process.exit(1);
}

// ─── Logging ────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toISOString();
  const line = `[${ts}] ${msg}`;
  console.log(line);
  if (!fs.existsSync(ARTIFACTS)) fs.mkdirSync(ARTIFACTS, { recursive: true });
  fs.appendFileSync(BOT_LOG, line + '\n');
}

// ─── HTTP ────────────────────────────────────────────────────
function fetch(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, (res) => {
      if (res.statusCode >= 400) { reject(new Error(`HTTP ${res.statusCode}`)); return; }
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(d));
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

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

function logSignal(signal) {
  let logs = [];
  try { if (fs.existsSync(SIGNAL_LOG)) logs = JSON.parse(fs.readFileSync(SIGNAL_LOG, 'utf8')); } catch {}
  logs.push(signal);
  fs.writeFileSync(SIGNAL_LOG, JSON.stringify(logs, null, 2));
}

// ─── Data Fetchers ──────────────────────────────────────────

/** MBT: Kraken BTC/USD1m OHLC, last 30 bars */
async function fetchMBT() {
  try {
    const raw = await fetch('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1');
    const data = JSON.parse(raw);
    const bars = data.result.XXBTZUSD;
    if (!bars || bars.length < 30) return null;

    const recent = bars.slice(-30);
    let high = 0, low = Infinity;
    for (const k of recent) {
      const h = parseFloat(k[2]), l = parseFloat(k[3]);
      if (h > high) high = h;
      if (l < low) low = l;
    }
    const close = parseFloat(bars[bars.length - 1][4]);
    return { close, high, low, symbol: 'MBT' };
  } catch (e) {
    log(`[MBT] Fetch error: ${e.message}`);
    return null;
  }
}

/** MNQ/MGC: Polygon.io agg bars (needs API key) */
async function fetchPolygon(ticker, polygonSymbol) {
  const apiKey = CONFIG.polygon_api_key || CONFIG.global?.polygon_api_key;
  if (!apiKey || apiKey === 'YOUR_POLYGON_API_KEY') {
    log(`[${ticker}] Polygon.io API key not set in config.json (global.polygon_api_key)`);
    return null;
  }
  try {
    const to = Math.floor(Date.now() / 1000);
    const from = to - 3600;
    const url = `https://api.polygon.io/v2/aggs/ticker/${polygonSymbol}/range/1/minute/${from}/${to}?apiKey=${apiKey}&unadjusted=false`;
    const raw = await fetch(url);
    const data = JSON.parse(raw);
    if (!data.results || data.results.length < 2) return null;

    const recent = data.results.slice(-30);
    let high = 0, low = Infinity;
    for (const r of recent) {
      if (r.h > high) high = r.h;
      if (r.l < low) low = r.l;
    }
    const last = data.results[data.results.length - 1];
    return { close: last.c, high, low, symbol: ticker };
  } catch (e) {
    log(`[${ticker}] Fetch error: ${e.message}`);
    return null;
  }
}

async function fetchMarketData(sym) {
  const cfg = CONFIG.symbols[sym];
  if (!cfg || !cfg.enabled) return null;
  if (sym === 'MBT') return fetchMBT();
  if (sym === 'MNQ') return fetchPolygon('MNQ', cfg.polygon_symbol);
  if (sym === 'MGC') return fetchPolygon('MGC', cfg.polygon_symbol);
  return null;
}

// ─── OTE Engine ─────────────────────────────────────────────
function checkOTERR(sym, marketData) {
  const { close, high, low, symbol } = marketData;
  const glob = CONFIG.global;
  const symCfg = CONFIG.symbols[sym];

  if (!close || !high || !low) return null;

  const swingLow = low;
  const swingHigh = high;
  const entry = close;
  const swingRange = swingHigh - swingLow;

  // Tick size and point value
  const tickSize = sym === 'MBT' ? 0.1 : 0.25;
  const pointValue = symCfg.point_value;

  const riskLong = entry - swingLow;
  const riskShort = swingHigh - entry;

  const results = [];

  // LONG: fib extension targets above entry
  if (riskLong > 0) {
    const t05 = swingLow - swingRange * 0.5;
    const t1 = swingLow - swingRange * 1.0;
    for (const target of [t05, t1]) {
      if (target <= entry) continue;
      const reward = target - entry;
      const rr = reward / riskLong;
      const riskUsd = (riskLong / tickSize) * pointValue * symCfg.max_contracts;
      if (rr >= MIN_RR && riskUsd <= MAX_RISK_USD) {
        results.push({ sym, direction: 'LONG', entry, sl: swingLow, target, riskLong, riskUsd, tickSize, rr });
      }
    }
  }

  // SHORT: fib extension targets below entry
  if (riskShort > 0) {
    const t05 = swingHigh + swingRange * 0.5;
    const t1 = swingHigh + swingRange * 1.0;
    for (const target of [t05, t1]) {
      if (target >= entry) continue;
      const reward = entry - target;
      const rr = reward / riskShort;
      const riskUsd = (riskShort / tickSize) * pointValue * symCfg.max_contracts;
      if (rr >= MIN_RR && riskUsd <= MAX_RISK_USD) {
        results.push({ sym, direction: 'SHORT', entry, sl: swingHigh, target, riskShort, riskUsd, tickSize, rr });
      }
    }
  }

  if (results.length === 0) return null;
  return results.sort((a, b) => b.rr - a.rr)[0];
}

// ─── Kill Zone ──────────────────────────────────────────────
function isInKillZone() {
  const now = new Date();
  const etHour = parseInt(now.toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', hour12: false }));
  const g = CONFIG.global;
  return (etHour >= g.kill_zone_london_start && etHour < g.kill_zone_london_end) ||
         (etHour >= g.kill_zone_ny_start && etHour < g.kill_zone_ny_end);
}

// ─── Telegram ───────────────────────────────────────────────
async function sendTelegram(text) {
  const { bot_token, chat_id } = CONFIG.telegram;
  if (!bot_token || !chat_id) { log('[TG] Not configured'); return false; }
  const body = JSON.stringify({ chat_id, text, parse_mode: 'Markdown' });
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.telegram.org', path: `/bot${bot_token}/sendMessage`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (res.statusCode === 200) { log('[TG] Sent OK'); resolve(true); }
        else { log(`[TG] Error ${res.statusCode}`); resolve(false); }
      });
    });
    req.on('error', e => { log(`[TG] Net error: ${e.message}`); resolve(false); });
    req.write(body);
    req.end();
  });
}

function formatSignal(sig) {
  const emoji = sig.direction === 'LONG' ? '🟢' : '🔴';
  return [
    `${emoji} *OTE RR25 SIGNAL*`,
    ``,
    `*Symbol:* \`${sig.sym}\``,
    `*Direction:* ${sig.direction}`,
    `*Entry:* \`${sig.entry.toFixed(2)}\``,
    `*SL:* \`${sig.sl.toFixed(2)}\``,
    `*Target 1 (60%):* \`${(sig.entry + (sig.target - sig.entry) * 0.6).toFixed(2)}\``,
    `*Target 2 (RR${sig.rr.toFixed(2)}):* \`${sig.target.toFixed(2)}\``,
    `*RR:* \`${sig.rr.toFixed(2)}\``,
    `*Risk:* \`$${sig.riskUsd.toFixed(2)}\``,
    `*Size:* \`${CONFIG.symbols[sig.sym].max_contracts} contracts\``,
    ``,
    `_${isInKillZone() ? 'Kill zone: YES' : 'Kill zone: NO'} | ${new Date().toISOString()}_`
  ].join('\n');
}

// ─── Main ───────────────────────────────────────────────────
async function runBot() {
  log('=== OTE Multi Bot run ===');
  const pnl = loadPNL();
  log(`PNL: $${pnl.total_pnl} | Signals: ${pnl.signal_count}/${MAX_SIGNALS_PER_DAY}`);

  for (const [sym, symCfg] of Object.entries(CONFIG.symbols)) {
    if (!symCfg.enabled) continue;

    try {
      const marketData = await fetchMarketData(sym);
      if (!marketData) continue;

      log(`[${sym}] Close: ${marketData.close} | High: ${marketData.high} | Low: ${marketData.low}`);

      const signal = checkOTERR(sym, marketData);
      if (!signal) { log(`[${sym}] No RR≥${MIN_RR} signal`); continue; }

      const pnlNow = loadPNL();
      if (pnlNow.total_pnl <= -KILL_SWITCH) { log(`[${sym}] Kill-switch TRIGGERED`); continue; }
      if (pnlNow.signal_count >= MAX_SIGNALS_PER_DAY) { log(`[${sym}] Max signals reached`); continue; }

      const fullSignal = {
        type: 'OTE_RR25',
        sym: signal.sym,
        direction: signal.direction,
        entry: Math.round(signal.entry * 100) / 100,
        sl: Math.round(signal.sl * 100) / 100,
        target1: Math.round((signal.entry + (signal.target - signal.entry) * 0.6) * 100) / 100,
        target2: Math.round(signal.target * 100) / 100,
        rr: Math.round(signal.rr * 100) / 100,
        risk_usd: Math.round(signal.riskUsd * 100) / 100,
        size: symCfg.max_contracts,
        time: new Date().toISOString(),
        note: `RR ${signal.rr.toFixed(2)} | Risk $${signal.riskUsd.toFixed(2)}`
      };

      log(`[${sym}] *** SIGNAL: ${signal.direction} @ ${signal.entry} → T2 ${signal.target.toFixed(2)} | RR ${signal.rr.toFixed(2)} | Risk $${signal.riskUsd.toFixed(2)} ***`);

      // Update PNL
      const updatedPNL = loadPNL();
      updatedPNL.signal_count = (updatedPNL.signal_count || 0) + 1;
      updatedPNL.signals = updatedPNL.signals || [];
      updatedPNL.signals.push({ sym, time: fullSignal.time, rr: fullSignal.rr, risk_usd: fullSignal.risk_usd });
      savePNL(updatedPNL);
      logSignal(fullSignal);

      await sendTelegram(formatSignal(fullSignal));

    } catch (e) {
      log(`[${sym}] Error: ${e.message}`);
    }
  }
}

(async () => { await runBot(); })();