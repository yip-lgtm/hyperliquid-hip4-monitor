#!/usr/bin/env node
/**
 * OTE Multi Bot Backtest — Correct OTE Logic
 * OTE Zone: 0.705–0.79 retracement from swing
 * Target: Entry + (RR × Risk), fixed RR=2.5
 * Entry must be inside OTE zone AND show rejection/momentum confirmation
 */
const https = require('https');
const fs = require('fs');
const path = require('path');

const WORKSPACE = process.env.WORKSPACE || '/home/node/.openclaw/workspace';
const RESULTS_FILE = path.join(WORKSPACE, 'artifacts', 'ote_backtest_results.json');

// ─── Config ─────────────────────────────────────────────────
const CONFIG = {
  MBT: { enabled: true, kraken_pair: 'XXBTZUSD', point_value: 0.1, max_contracts: 1 },
  MNQ: { enabled: false, exchange: 'polygon' },
  MGC: { enabled: false, exchange: 'polygon' }
};

const MIN_RR = 2.5;
const MAX_RISK_USD = 80;
const DAYS_BACK = 30;
const LOOKBACK = 20;   // bars for swing high/low
const OTE_LOW = 0.705;
const OTE_HIGH = 0.79;
const KILL_SWITCH = 100;
const MAX_SIGNALS_PER_DAY = 2;
const MAX_HOLD_BARS = 24; // exit if not hit within 24 x 1h bars

function fetch(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, (res) => {
      if (res.statusCode >= 400) { reject(new Error(`HTTP ${res.statusCode}`)); return; }
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(d));
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─── Data Fetch ─────────────────────────────────────────────
async function fetchKrakenOHLC(pair, intervalMinutes, since) {
  const url = `https://api.kraken.com/0/public/OHLC?pair=${pair}&interval=${intervalMinutes}&since=${since}`;
  const raw = await fetch(url);
  const data = JSON.parse(raw);
  const key = Object.keys(data.result).find(k => k !== 'last');
  return data.result[key] || [];
}

// ─── OTE Core ───────────────────────────────────────────────
// Returns OTE zone boundaries for a given swing high/low
function oteZone(swingLow, swingHigh) {
  const range = swingHigh - swingLow;
  return {
    zoneHigh: swingLow + range * OTE_HIGH, // 0.79 retracement level
    zoneLow: swingLow + range * OTE_LOW,    // 0.705 retracement level
    range
  };
}

// Check if price is in OTE zone
function priceInOTEZone(price, zoneLow, zoneHigh) {
  return price >= zoneLow && price <= zoneHigh;
}

// ─── Kill Zone ─────────────────────────────────────────────
function isInKillZone(timestamp) {
  const d = new Date(timestamp * 1000);
  const etHour = parseInt(d.toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', hour12: false }));
  return (etHour >= 8 && etHour < 10) || (etHour >= 14 && etHour < 16);
}

// ─── Rejection / MSS Confirmation ───────────────────────────
// Candle must show reversal characteristics:
// Bullish: close near high, lower wick, or hammer-like
// Check last 2-3 candles for momentum shift
function hasRejectionConfirmation(bars, currentIdx, direction) {
  if (currentIdx < 2) return false;
  const lookback = 3;
  const slice = bars.slice(currentIdx - lookback, currentIdx + 1);
  if (slice.length < 3) return false;

  for (let i = 0; i < slice.length - 1; i++) {
    const b = slice[i];
    const open = parseFloat(b[1]);
    const close = parseFloat(b[4]);
    const high = parseFloat(b[2]);
    const low = parseFloat(b[3]);
    const bodySize = Math.abs(close - open);
    const upperWick = high - Math.max(open, close);
    const lowerWick = Math.min(open, close) - low;

    if (direction === 'LONG') {
      // Bullish rejection: close near high, small lower wick, or hammer
      // OR: close above open with decreasing low
      if (close > open && lowerWick > bodySize * 1.5) return true; // hammer-like
      if (close > open && close > high - bodySize) return true; // close near high
    } else {
      // Bearish rejection: close near low, small upper wick
      if (close < open && upperWick > bodySize * 1.5) return true; // shooting star-like
      if (close < open && close < low + bodySize) return true; // close near low
    }
  }
  return false;
}

// ─── Main Backtest ─────────────────────────────────────────
async function runBacktest(sym, intervalMinutes) {
  const cfg = CONFIG[sym];
  console.log(`\n=== ${sym} Backtest | ${DAYS_BACK}d | ${intervalMinutes}m | LB=${LOOKBACK} ===`);

  const now = Math.floor(Date.now() / 1000);
  const since = now - DAYS_BACK * 86400;
  const bars = await fetchKrakenOHLC(cfg.kraken_pair, intervalMinutes, since);

  if (!bars || bars.length < 50) {
    console.log('Insufficient data'); return { signals: [], stats: {} };
  }
  console.log(`Loaded ${bars.length} bars`);

  // bars format: [timestamp, open, high, low, close, volume, ...
  // For Kraken: [0=time, 1=open, 2=high, 3=low, 4=close, 5=vwap, 6=volume, 7=count]
  const signals = [];
  const tickSize = sym === 'MBT' ? 0.1 : 0.25;
  const pointValue = cfg.point_value;

  for (let i = LOOKBACK + 1; i < bars.length - 1; i++) {
    // Swing from PAST bars only (no look-ahead)
    const pastSlice = bars.slice(i - LOOKBACK, i);
    let swingHigh = 0, swingLow = Infinity;
    for (const b of pastSlice) {
      const h = parseFloat(b[2]), l = parseFloat(b[3]);
      if (h > swingHigh) swingHigh = h;
      if (l < swingLow) swingLow = l;
    }

    const entryCandle = bars[i];
    const entry = parseFloat(entryCandle[4]);
    const entryTime = parseInt(entryCandle[0]);

    if (!swingHigh || !swingLow || swingHigh === swingLow) continue;

    const zone = oteZone(swingLow, swingHigh);

    // Check both LONG and SHORT scenarios
    for (const direction of ['LONG', 'SHORT']) {
      const inZone = priceInOTEZone(entry,
        direction === 'LONG' ? zone.zoneLow : zone.zoneLow,
        direction === 'LONG' ? zone.zoneHigh : zone.zoneHigh);

      if (!inZone) continue;

      // For LONG: entry must be in zoneLow-zoneHigh, and price should be near zoneLow (good entry)
      // For SHORT: entry must be in zoneLow-zoneHigh, and price should be near zoneHigh (good entry)

      const sl = direction === 'LONG' ? swingLow : swingHigh;
      const risk = direction === 'LONG' ? entry - sl : sl - entry;
      if (risk <= 0) continue;

      const riskUsd = (risk / tickSize) * pointValue * cfg.max_contracts;
      if (riskUsd > MAX_RISK_USD) continue;

      // Target: Entry ± RR × Risk (RR fixed at 2.5)
      const target = direction === 'LONG'
        ? entry + MIN_RR * risk
        : entry - MIN_RR * risk;

      // Check kill zone
      if (!isInKillZone(entryTime)) continue;

      // Simple momentum filter: check if recent bars favor this direction
      // Require at least 2 of last 3 candles to move in direction of trade
      const recent = bars.slice(Math.max(0, i - 3), i);
      let favorable = 0;
      for (const b of recent) {
        const close = parseFloat(b[4]);
        const open = parseFloat(b[1]);
        if (direction === 'LONG' && close > open) favorable++;
        if (direction === 'SHORT' && close < open) favorable++;
      }
      if (favorable < 2) continue;

      // Simulate: check future bars for target hit or stop hit
      let result = 'open'; // 'win', 'loss', 'open'
      let exitPrice = null;
      let exitBar = 0;

      for (let j = i + 1; j < Math.min(i + MAX_HOLD_BARS + 1, bars.length); j++) {
        const futureHigh = parseFloat(bars[j][2]);
        const futureLow = parseFloat(bars[j][3]);
        const futureClose = parseFloat(bars[j][4]);

        if (direction === 'LONG') {
          if (futureHigh >= target) { result = 'win'; exitPrice = target; exitBar = j - i; break; }
          if (futureLow <= sl) { result = 'loss'; exitPrice = sl; exitBar = j - i; break; }
        } else {
          if (futureLow <= target) { result = 'win'; exitPrice = target; exitBar = j - i; break; }
          if (futureHigh >= sl) { result = 'loss'; exitPrice = sl; exitBar = j - i; break; }
        }
      }

      if (result === 'open') continue; // ignore still-open trades

      signals.push({
        sym, direction, entry, sl, target, rr: MIN_RR,
        risk_usd: riskUsd, risk_ticks: risk / tickSize,
        zoneLow: zone.zoneLow, zoneHigh: zone.zoneHigh,
        swingLow, swingHigh, swingRange: zone.range,
        time: new Date(entryTime * 1000).toISOString(),
        result, exitPrice, exitBars: exitBar,
        zone_entry: priceInOTEZone(entry, zone.zoneLow, zone.zoneHigh) ? 'inside' : 'unknown'
      });
    }
  }

  return signals;
}

// ─── Stats ─────────────────────────────────────────────────
function calcStats(signals) {
  if (signals.length === 0) return { total: 0, wins: 0, losses: 0, winRate: '0%', avgRR: 0, avgRiskUsd: 0, simPnl: 0, rrDistribution: {} };

  const wins = signals.filter(s => s.result === 'win');
  const losses = signals.filter(s => s.result === 'loss');
  const winRate = (wins.length / signals.length * 100).toFixed(1) + '%';
  const avgRR = (signals.reduce((a, s) => a + s.rr, 0) / signals.length).toFixed(2);
  const avgRiskUsd = (signals.reduce((a, s) => a + s.risk_usd, 0) / signals.length).toFixed(2);

  let simPnl = 0;
  for (const s of signals) {
    simPnl += s.result === 'win' ? s.rr * s.risk_usd : -s.risk_usd;
  }

  // RR distribution
  const rrDist = { '2.5': 0, '3.0': 0, '5.0': 0, '10+': 0 };
  for (const s of signals) {
    if (s.rr >= 10) rrDist['10+']++;
    else if (s.rr >= 5) rrDist['5.0']++;
    else if (s.rr >= 3) rrDist['3.0']++;
    else rrDist['2.5']++;
  }

  return {
    total: signals.length,
    wins: wins.length,
    losses: losses.length,
    winRate, avgRR, avgRiskUsd,
    simPnl: simPnl.toFixed(2),
    avgExitBars: (signals.reduce((a, s) => a + s.exitBars, 0) / signals.length).toFixed(1),
    rrDistribution: rrDist,
    killZoneCount: signals.filter(s => isInKillZone(new Date(s.time).getTime() / 1000)).length
  };
}

// ─── Main ───────────────────────────────────────────────────
(async () => {
  console.log('═══════════════════════════════════════');
  console.log(' OTE Backtest — Correct OTE Logic');
  console.log(` Period: ${DAYS_BACK} days | Min RR: ${MIN_RR} | Max Risk: $${MAX_RISK_USD}`);
  console.log(` OTE Zone: ${OTE_LOW}–${OTE_HIGH} | Lookback: ${LOOKBACK} bars`);
  console.log(` Kill Zone: London 8-10 AM ET | NY 2-4 PM ET`);
  console.log('═══════════════════════════════════════');

  const allSignals = [];

  // MBT
  if (CONFIG.MBT.enabled) {
    const signals = await runBacktest('MBT', 60); // 1h bars
    allSignals.push(...signals);
  }

  console.log('\n═══════════════════════════════════════');
  console.log(' RESULTS');
  console.log('═══════════════════════════════════════');

  for (const sym of Object.keys(CONFIG).filter(k => CONFIG[k].enabled)) {
    const symSigs = allSignals.filter(s => s.sym === sym);
    const stats = calcStats(symSigs);
    console.log(`\n${sym}:`);
    console.log(`  Signals: ${stats.total} | Wins: ${stats.wins} | Losses: ${stats.losses} | Win Rate: ${stats.winRate}`);
    console.log(`  Avg Risk: $${stats.avgRiskUsd} | Sim P&L: $${stats.simPnl}`);
    console.log(`  Avg Exit Bars: ${stats.avgExitBars}h | Kill Zone: ${stats.killZoneCount}/${stats.total}`);
    if (symSigs.length > 0) {
      console.log(`  RR Dist: ${JSON.stringify(stats.rrDistribution)}`);
      console.log('\n  All Signals:');
      for (const s of symSigs) {
        console.log(`  ${s.time} | ${s.direction} | E:${s.entry.toFixed(1)} SL:${s.sl.toFixed(1)} T:${s.target.toFixed(1)} | RR:${s.rr} | ${s.result.toUpperCase()} (exit@${s.exitBars}b) | $${s.risk_usd.toFixed(0)}`);
      }
    }
  }

  // Summary
  const totalStats = calcStats(allSignals);
  console.log('\n───────────────────────────────────────');
  console.log(`TOTAL: ${totalStats.total} signals | Win Rate: ${totalStats.winRate}`);
  console.log(`Sim P&L: $${totalStats.simPnl} | Avg Risk: $${totalStats.avgRiskUsd}/trade`);
  console.log('───────────────────────────────────────');

  // Save
  const output = {
    meta: { period: `${DAYS_BACK}d`, interval: '1h', lookback: LOOKBACK, minRR: MIN_RR, maxRiskUsd: MAX_RISK_USD, oteZone: `${OTE_LOW}-${OTE_HIGH}` },
    totalStats,
    bySymbol: {}
  };
  for (const sym of [...new Set(allSignals.map(s => s.sym))]) {
    const symSigs = allSignals.filter(s => s.sym === sym);
    output.bySymbol[sym] = { stats: calcStats(symSigs), signals: symSigs };
  }
  fs.writeFileSync(RESULTS_FILE, JSON.stringify(output, null, 2));
  console.log(`\nResults saved to: ${RESULTS_FILE}`);
})();