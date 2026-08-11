#!/usr/bin/env node

import { writeFile } from 'node:fs/promises';

const TIMEOUT_MS = 15_000;
const KRAKEN_HISTORY = 'https://futures.kraken.com/derivatives/api/v3/history';
const DERIBIT_API = 'https://www.deribit.com/api/v2';
const KRAKEN_SYMBOLS = ['PI_XBTUSD', 'PI_ETHUSD'];
const DERIBIT_CURRENCIES = ['BTC', 'ETH'];
const MAX_PAGES = 8;
const DERIBIT_LOOKBACK_MS = 6 * 60 * 60 * 1000;
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
    const fingerprint = `kraken:${symbol}:${tradeId}:${time}`;
    evidence.push({
      fingerprint,
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

async function getDeribitMetadata(currency) {
  const query = new URLSearchParams({ currency, kind: 'future', expired: 'false' });
  const payload = await fetchJson(`${DERIBIT_API}/public/get_instruments?${query}`);
  if (!Array.isArray(payload?.result)) throw new Error(`Deribit metadata contract failed for ${currency}`);
  const metadata = {};
  for (const item of payload.result) {
    if (!item || item.kind !== 'future' || item.is_active !== true) continue;
    const type = text(item.instrument_type);
    if (!['linear', 'reversed'].includes(type)) continue;
    metadata[item.instrument_name] = {
      instrumentType: type,
      quoteCurrency: text(item.quote_currency).toUpperCase(),
      baseCurrency: text(item.base_currency).toUpperCase(),
      settlementCurrency: text(item.settlement_currency).toUpperCase(),
      contractSize: positive(item.contract_size),
    };
  }
  if (!Object.keys(metadata).length) throw new Error(`Deribit has no active futures metadata for ${currency}`);
  return metadata;
}

function deribitWaveSide(liquidation, takerDirection) {
  const direction = text(takerDirection).toLowerCase();
  if (!['buy', 'sell'].includes(direction)) return { side: null, reason: 'invalid taker direction' };
  if (liquidation === 'T') {
    return { side: direction === 'sell' ? 'long' : 'short', reason: 'T = taker under liquidation; direction is taker direction' };
  }
  if (liquidation === 'M') {
    return { side: direction === 'buy' ? 'long' : 'short', reason: 'M = maker under liquidation; maker direction is opposite documented taker direction' };
  }
  if (liquidation === 'MT') {
    return { side: null, reason: 'blocked: both matched sides under liquidation; Wave Alpha MT allocation policy pending' };
  }
  return { side: null, reason: 'not an explicit liquidation code' };
}

function deribitUsd(row, meta) {
  const amount = positive(row.amount);
  const price = positive(row.price);
  if (!amount || !meta) return { usd: null, rule: 'missing amount or metadata' };
  if (meta.instrumentType === 'reversed') return { usd: amount, rule: 'reversed future amount is USD units' };
  if (meta.instrumentType === 'linear' && price && USD_LIKE.has(meta.quoteCurrency)) {
    return { usd: amount * price, rule: 'linear future base amount multiplied by USD-like quote price' };
  }
  return { usd: null, rule: 'unsupported unit/quote combination' };
}

async function scanDeribit(currency) {
  const metadata = await getDeribitMetadata(currency);
  const rows = new Map();
  const now = Date.now();
  const start = now - DERIBIT_LOOKBACK_MS;
  let pageEnd = now;
  let pages = 0;

  for (let page = 0; page < MAX_PAGES; page++) {
    const query = new URLSearchParams({
      currency,
      kind: 'future',
      start_timestamp: String(start),
      end_timestamp: String(pageEnd),
      count: '1000',
      sorting: 'desc',
    });
    const payload = await fetchJson(`${DERIBIT_API}/public/get_last_trades_by_currency_and_time?${query}`);
    const pageRows = payload?.result?.trades;
    if (!Array.isArray(pageRows)) throw new Error(`Deribit trade contract failed for ${currency}`);
    pages += 1;
    for (const row of pageRows) {
      if (!row || typeof row !== 'object') continue;
      const key = text(row.trade_id) || `${row.instrument_name ?? ''}:${row.trade_seq ?? ''}`;
      if (!rows.has(key)) rows.set(key, row);
    }
    if (payload?.result?.has_more !== true || pageRows.length < 1000) break;
    const timestamps = pageRows.map(row => Number(row?.timestamp)).filter(Number.isFinite);
    const oldest = timestamps.length ? Math.min(...timestamps) : NaN;
    if (!Number.isFinite(oldest) || oldest <= start || oldest >= pageEnd) break;
    pageEnd = oldest - 1;
  }

  const evidence = [];
  for (const row of rows.values()) {
    const liquidation = text(row.liquidation);
    if (!['M', 'T', 'MT'].includes(liquidation)) continue;
    const instrument = text(row.instrument_name);
    const tradeId = text(row.trade_id);
    const meta = metadata[instrument] || null;
    const side = deribitWaveSide(liquidation, row.direction);
    const notional = deribitUsd(row, meta);
    const fingerprint = `deribit:${instrument}:${tradeId}:${liquidation}`;
    evidence.push({
      fingerprint,
      exchange: 'deribit-futures',
      source: 'official-public-trades',
      currency,
      instrument,
      tradeId: tradeId || null,
      tradeSeq: row.trade_seq ?? null,
      timestamp: Number.isFinite(Number(row.timestamp)) ? Number(row.timestamp) : null,
      liquidationMarker: liquidation,
      takerDirection: text(row.direction).toLowerCase() || null,
      price: positive(row.price),
      amount: positive(row.amount),
      contracts: positive(row.contracts),
      instrumentType: meta?.instrumentType ?? null,
      quoteCurrency: meta?.quoteCurrency ?? null,
      authoritativeUsdNotional: notional.usd,
      notionalRule: notional.rule,
      waveSide: side.side,
      sideReason: side.reason,
    });
  }

  return { currency, pages, rows: rows.size, activeFutures: Object.keys(metadata).length, liquidationRows: evidence.length, evidence };
}

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsedForMarketData: false,
  runtimeMutation: false,
  kraken: [],
  deribit: [],
  evidence: [],
};

for (const symbol of KRAKEN_SYMBOLS) {
  const result = await scanKraken(symbol);
  report.kraken.push({ ...result, evidence: undefined });
  report.evidence.push(...result.evidence);
}
for (const currency of DERIBIT_CURRENCIES) {
  const result = await scanDeribit(currency);
  report.deribit.push({ ...result, evidence: undefined });
  report.evidence.push(...result.evidence);
}

report.evidence.sort((a, b) => a.fingerprint.localeCompare(b.fingerprint));
report.summary = {
  evidenceRows: report.evidence.length,
  krakenLiquidations: report.evidence.filter(row => row.exchange === 'kraken-futures').length,
  deribitM: report.evidence.filter(row => row.exchange === 'deribit-futures' && row.liquidationMarker === 'M').length,
  deribitT: report.evidence.filter(row => row.exchange === 'deribit-futures' && row.liquidationMarker === 'T').length,
  deribitMT: report.evidence.filter(row => row.exchange === 'deribit-futures' && row.liquidationMarker === 'MT').length,
};

await writeFile('/tmp/wave-alpha-phase-c-evidence.json', JSON.stringify(report, null, 2));
console.log(`WAVE_PHASE_C_OBSERVER=${JSON.stringify(report)}`);
