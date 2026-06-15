const YahooFinance = require('yahoo-finance2').default;
const yf = new YahooFinance();
const p1 = Math.floor((Date.now() - 30 * 86400 * 1000) / 1000);
const p2 = Math.floor(Date.now() / 1000);

async function testSymbol(sym, label) {
  try {
    const r = await yf.chart(sym, { interval: '1h', period1: p1, period2: p2 });
    console.log(`${label}: ${r.meta?.regularMarketPrice || 'OK'} | bars: ${r.timestamp?.length}`);
  } catch (e) {
    console.log(`${label}: ERROR - ${e.message.includes('Too Many') ? 'Rate Limited' : e.message.substring(0, 80)}`);
  }
  await new Promise(r => setTimeout(r, 1500));
}

(async () => {
  await testSymbol('BTC-USD', 'BTC-USD');
  await testSymbol('MNQ=F', 'MNQ=F (Nasdaq)');
  await testSymbol('MGC=F', 'MGC=F (Gold)');
  await testSymbol('GC=F', 'GC=F (Gold continuous)');
})();