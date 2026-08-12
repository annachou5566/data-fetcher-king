#!/usr/bin/env node

import { writeFile } from 'node:fs/promises';

const TIMEOUT_MS = 15_000;
const KRAKEN_HISTORY = 'https://futures.kraken.com/derivatives/api/v3/history';
const KRAKEN_SYMBOLS = ['PI_XBTUSD', 'PI_ETHUSD'];
const MAX_PAGES = 8;
const USD_LIKE = new Set(['USD', 'USDT', 'USDC']);

const text = value => String(value ?? '').trim();
const positive = value => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
};

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'Cache-Control': 'no-cache',
      'User-Agent': 'data-fetcher-king/wave-alpha-phase-c-observer',
    },
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const body = await response.text();
  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    throw new Error(`non-json ${response.status} from ${new URL(url).hostname}`);
  }
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
  return payload;
}

async function scanKraken(symbol) {
  const rows = new Map();
  let lastTime = '';
  let previousBoundary = '';
  let pages = 0;

  for (let page = 0; page < MAX_PAGES; page++) {
    const query = new URLSearchParams({ symbol });
    if (lastTime) query.set('lastTime', lastTime);
    const payload = await fetchJson(`${KRAKEN_HISTORY}?${query}`);
    if (payload?.result !== 'success' || !Array.isArray(payload?.history)) {
      throw new Error(`Kraken history contract failed for ${symbol}`);
    }
    pages += 1;
    const pageRows = payload.history.filter(row => row && typeof row === 'object');
    for (const row of pageRows) {
      const key = `${row.trade_id ?? ''}:${row.time ?? ''}`;
      if (!rows.has(key)) rows.set(key, row);
    }
    if (pageRows.length < 100) break;
    const boundary = text(pageRows.at(-1)?.time);
    if (!boundary || boundary === previousBoundary) break;
    previousBoundary = boundary;
    lastTime = boundary;
  }

  const evidence = [];
  for (const row of rows.values()) {
    if (text(row.type).toLowerCase() !== 'liquidation') continue;
    const notional = positive(row.notional_amount);
    const currency = text(row.notional_currency).toUpperCase();
    const tradeId = text(row.trade_id);
    const time = text(row.time);
    evidence.push({
      fingerprint: `kraken:${symbol}:${tradeId}:${time}`,
      exchange: 'kraken-futures',
      source: 'official-public-history',
      symbol,
      tradeId: tradeId || null,
      time: time || null,
      liquidationMarker: 'liquidation',
      takerSide: text(row.side).toLowerCase() || null,
      price: positive(row.price),
      size: positive(row.size),
      authoritativeUsdNotional: notional && USD_LIKE.has(currency) ? notional : null,
      notionalCurrency: currency || null,
      waveSide: null,
      sideReason: 'blocked: Kraken documents trade side as taker side but public row does not prove whether liquidated participant was maker or taker',
    });
  }

  return { symbol, pages, rows: rows.size, liquidationRows: evidence.length, evidence };
}

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsedForMarketData: false,
  runtimeMutation: false,
  scope: 'kraken-only; Deribit qualification closed in Wave Alpha on 2026-08-12',
  kraken: [],
  evidence: [],
};

for (const symbol of KRAKEN_SYMBOLS) {
  const result = await scanKraken(symbol);
  report.kraken.push({ ...result, evidence: undefined });
  report.evidence.push(...result.evidence);
}

report.evidence.sort((a, b) => a.fingerprint.localeCompare(b.fingerprint));
report.summary = {
  evidenceRows: report.evidence.length,
  krakenLiquidations: report.evidence.length,
};

await writeFile('/tmp/wave-alpha-phase-c-evidence.json', JSON.stringify(report, null, 2));
console.log(`WAVE_PHASE_C_OBSERVER=${JSON.stringify(report)}`);
