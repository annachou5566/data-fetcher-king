#!/usr/bin/env node

const API_BASE = 'https://api.coinalyze.net/v1';
const API_KEY = String(process.env.COINALYZE_API_KEY || '').trim();
const TIMEOUT_MS = 20_000;
const RATE_WINDOW_MS = 61_000;
const RATE_BUDGET_UNITS = 38;
const HISTORY_INTERVAL = '1hour';
const LOOKBACK_SEC = 24 * 60 * 60;
const POSITIVE_CONTROL_SYMBOLS = ['BTCUSDT_PERP.A', 'ETHUSDT_PERP.A'];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const round2 = value => Math.round((Number(value) || 0) * 100) / 100;

if (!API_KEY) {
  console.log(JSON.stringify({
    provider: 'Coinalyze',
    vantage: 'github-hosted-public-runner',
    qualification: 'SECRET_UNAVAILABLE',
    apiKeyPrinted: false,
    productionEligible: false,
    aggregateEligible: false,
  }, null, 2));
  process.exit(2);
}

let rateWindowStartedAt = Date.now();
let usedUnits = 0;

async function reserveUnits(units) {
  while (true) {
    const now = Date.now();
    if (now - rateWindowStartedAt >= RATE_WINDOW_MS) {
      rateWindowStartedAt = now;
      usedUnits = 0;
    }
    if (usedUnits + units <= RATE_BUDGET_UNITS) {
      usedUnits += units;
      return;
    }
    await sleep(Math.max(1_000, RATE_WINDOW_MS - (now - rateWindowStartedAt) + 500));
  }
}

function classificationForStatus(status) {
  if (status === 401) return 'KEY_REJECTED_OR_MISSING';
  if (status === 403) return 'RUNNER_VANTAGE_BLOCKED_OR_FORBIDDEN';
  if (status === 429) return 'RATE_LIMITED';
  return `HTTP_${status}`;
}

async function apiGet(path, params = {}, units = 1, retries = 1) {
  await reserveUnits(units);
  const url = new URL(`${API_BASE}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  }

  for (let attempt = 0; attempt <= retries; attempt++) {
    const response = await fetch(url, {
      headers: {
        Accept: 'application/json',
        api_key: API_KEY,
        'User-Agent': 'data-fetcher-king/wave-alpha-coinalyze-qualification',
      },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });

    if (response.status === 429 && attempt < retries) {
      const retryAfter = Math.max(1, Number(response.headers.get('retry-after')) || 60);
      await sleep((retryAfter + 1) * 1000);
      rateWindowStartedAt = Date.now();
      usedUnits = 0;
      continue;
    }

    const text = await response.text();
    let payload = null;
    try { payload = JSON.parse(text); } catch {}

    if (!response.ok) {
      const error = new Error(classificationForStatus(response.status));
      error.status = response.status;
      error.payloadType = Array.isArray(payload) ? 'array' : typeof payload;
      throw error;
    }
    return payload;
  }
  throw new Error('unreachable');
}

function compactMarket(row) {
  return {
    symbol: String(row?.symbol || ''),
    exchange: String(row?.exchange || ''),
    symbolOnExchange: String(row?.symbol_on_exchange || ''),
    baseAsset: String(row?.base_asset || ''),
    quoteAsset: String(row?.quote_asset || ''),
    perpetual: Boolean(row?.is_perpetual),
    margined: String(row?.margined || ''),
    liquidationUnit: String(row?.oi_lq_vol_denominated_in || ''),
  };
}

function histogram(rows, key) {
  const out = {};
  for (const row of rows) {
    const value = String(row[key] || 'UNKNOWN');
    out[value] = (out[value] || 0) + 1;
  }
  return Object.fromEntries(Object.entries(out).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
}

function summarizeHistoryPayload(payload, requestedSymbols) {
  if (!Array.isArray(payload)) throw new Error('unexpected liquidation-history contract');
  const requested = new Set(requestedSymbols);
  const returned = new Set();
  const totals = new Map();
  let points = 0;

  for (const item of payload) {
    const symbol = String(item?.symbol || '');
    if (!symbol) continue;
    returned.add(symbol);
    let longUsd = 0;
    let shortUsd = 0;
    let symbolPoints = 0;
    for (const row of Array.isArray(item?.history) ? item.history : []) {
      const l = Number(row?.l);
      const s = Number(row?.s);
      if (Number.isFinite(l)) longUsd += l;
      if (Number.isFinite(s)) shortUsd += s;
      symbolPoints += 1;
    }
    points += symbolPoints;
    totals.set(symbol, {
      longUsd: round2(longUsd),
      shortUsd: round2(shortUsd),
      totalUsd: round2(longUsd + shortUsd),
      points: symbolPoints,
    });
  }

  const omitted = [...requested].filter(symbol => !returned.has(symbol));
  const unexpected = [...returned].filter(symbol => !requested.has(symbol));
  return {
    requestedCount: requested.size,
    returnedCount: [...returned].filter(symbol => requested.has(symbol)).length,
    omittedCount: omitted.length,
    omittedSample: omitted.slice(0, 10),
    unexpectedCount: unexpected.length,
    unexpectedSample: unexpected.slice(0, 10),
    points,
    totals,
  };
}

async function requestHistory(symbols, from, to) {
  return apiGet('/liquidation-history', {
    symbols: symbols.join(','),
    interval: HISTORY_INTERVAL,
    from,
    to,
    convert_to_usd: 'true',
  }, symbols.length, 1);
}

async function main() {
  const checkedAt = new Date().toISOString();
  let exchanges;
  let futureMarkets;
  try {
    exchanges = await apiGet('/exchanges');
    futureMarkets = await apiGet('/future-markets');
  } catch (error) {
    console.log(JSON.stringify({
      checkedAt,
      provider: 'Coinalyze',
      vantage: 'github-hosted-public-runner',
      qualification: error.message,
      httpStatus: error.status || null,
      apiKeyPrinted: false,
      rawPayloadPrinted: false,
      productionEligible: false,
      aggregateEligible: false,
      note: error.status === 403
        ? '403 on this vantage is not proof the provider is unusable from Oracle/Cloudflare/other egress.'
        : null,
    }, null, 2));
    return;
  }

  if (!Array.isArray(exchanges) || !Array.isArray(futureMarkets)) {
    throw new Error('unexpected discovery contract');
  }

  const exchangeCandidates = exchanges
    .filter(row => /hyperliquid/i.test(`${row?.name || ''} ${row?.code || ''}`))
    .map(row => ({ name: String(row?.name || ''), code: String(row?.code || '') }));
  const exchangeCodes = new Set(exchangeCandidates.map(row => row.code).filter(Boolean));

  const hyperliquidMarkets = futureMarkets
    .filter(row => exchangeCodes.has(String(row?.exchange || '')) || /hyperliquid/i.test(String(row?.exchange || '')))
    .map(compactMarket)
    .filter(row => row.symbol)
    .sort((a, b) => a.symbol.localeCompare(b.symbol));

  const discovery = {
    exchangeCandidates,
    totalFutureMarketsReturned: futureMarkets.length,
    hyperliquidMarketCount: hyperliquidMarkets.length,
    hyperliquidPerpetualCount: hyperliquidMarkets.filter(row => row.perpetual).length,
    quoteAssets: histogram(hyperliquidMarkets, 'quoteAsset'),
    marginClasses: histogram(hyperliquidMarkets, 'margined'),
    liquidationUnits: histogram(hyperliquidMarkets, 'liquidationUnit'),
  };

  if (!hyperliquidMarkets.length) {
    console.log(JSON.stringify({
      checkedAt,
      provider: 'Coinalyze',
      vantage: 'github-hosted-public-runner',
      qualification: 'NO_HYPERLIQUID_MARKETS_DISCOVERED',
      discovery,
      apiKeyPrinted: false,
      rawPayloadPrinted: false,
      productionEligible: false,
      aggregateEligible: false,
    }, null, 2));
    return;
  }

  const to = Math.floor(Date.now() / 1000);
  const from = to - LOOKBACK_SEC;

  let control;
  try {
    const payload = await requestHistory(POSITIVE_CONTROL_SYMBOLS, from, to);
    const parsed = summarizeHistoryPayload(payload, POSITIVE_CONTROL_SYMBOLS);
    const totals = [...parsed.totals.entries()].map(([symbol, value]) => ({ symbol, ...value }));
    control = {
      symbols: POSITIVE_CONTROL_SYMBOLS,
      requestedCount: parsed.requestedCount,
      returnedCount: parsed.returnedCount,
      omittedCount: parsed.omittedCount,
      points: parsed.points,
      totalUsd: round2(totals.reduce((sum, row) => sum + row.totalUsd, 0)),
      rows: totals,
      working: parsed.returnedCount > 0 && parsed.points > 0,
    };
  } catch (error) {
    control = {
      symbols: POSITIVE_CONTROL_SYMBOLS,
      working: false,
      qualification: error.message,
      httpStatus: error.status || null,
    };
  }

  const totalsBySymbol = new Map();
  let requestedCount = 0;
  let returnedCount = 0;
  let omittedCount = 0;
  let unexpectedCount = 0;
  let points = 0;
  let nonEmptyMarkets = 0;
  let cursor = 0;
  let historyQualification = 'OK';
  let historyHttpStatus = null;
  const omittedSamples = [];

  while (cursor < hyperliquidMarkets.length) {
    const now = Date.now();
    if (now - rateWindowStartedAt >= RATE_WINDOW_MS) {
      rateWindowStartedAt = now;
      usedUnits = 0;
    }
    const available = RATE_BUDGET_UNITS - usedUnits;
    if (available <= 0) {
      await sleep(Math.max(1_000, RATE_WINDOW_MS - (now - rateWindowStartedAt) + 500));
      continue;
    }
    const batchSize = Math.min(20, available, hyperliquidMarkets.length - cursor);
    const batch = hyperliquidMarkets.slice(cursor, cursor + batchSize);
    const symbols = batch.map(row => row.symbol);

    let payload;
    try {
      payload = await requestHistory(symbols, from, to);
    } catch (error) {
      historyQualification = error.message;
      historyHttpStatus = error.status || null;
      break;
    }

    const parsed = summarizeHistoryPayload(payload, symbols);
    requestedCount += parsed.requestedCount;
    returnedCount += parsed.returnedCount;
    omittedCount += parsed.omittedCount;
    unexpectedCount += parsed.unexpectedCount;
    points += parsed.points;
    omittedSamples.push(...parsed.omittedSample.slice(0, Math.max(0, 20 - omittedSamples.length)));

    for (const [symbol, value] of parsed.totals.entries()) {
      totalsBySymbol.set(symbol, value);
      if (value.points > 0) nonEmptyMarkets += 1;
    }

    cursor += batch.length;
  }

  let totalLongUsd = 0;
  let totalShortUsd = 0;
  for (const value of totalsBySymbol.values()) {
    totalLongUsd += value.longUsd;
    totalShortUsd += value.shortUsd;
  }

  const topMarkets = [...totalsBySymbol.entries()]
    .map(([symbol, value]) => ({ symbol, ...value }))
    .sort((a, b) => b.totalUsd - a.totalUsd)
    .slice(0, 10);

  const allRequestsCompleted = historyQualification === 'OK' && requestedCount === hyperliquidMarkets.length;
  const everyRequestedSymbolReturned = allRequestsCompleted && omittedCount === 0 && returnedCount === requestedCount;
  const controlWorking = Boolean(control?.working);
  let qualification;
  if (!allRequestsCompleted) qualification = historyQualification;
  else if (!controlWorking) qualification = 'POSITIVE_CONTROL_INCONCLUSIVE';
  else if (returnedCount === 0 && omittedCount === requestedCount) qualification = 'HYPERLIQUID_SYMBOLS_OMITTED_WITH_WORKING_CONTROL';
  else if (!everyRequestedSymbolReturned) qualification = 'PARTIAL_HYPERLIQUID_SYMBOL_RESPONSE';
  else qualification = 'COMPLETE_RETURNED_SYMBOL_SWEEP';

  console.log(JSON.stringify({
    checkedAt,
    provider: 'Coinalyze',
    vantage: 'github-hosted-public-runner',
    qualification,
    historyHttpStatus,
    discovery,
    positiveControl: control,
    history: {
      interval: HISTORY_INTERVAL,
      from,
      to,
      convertToUsd: true,
      marketsDiscovered: hyperliquidMarkets.length,
      requestedCount,
      returnedCount,
      omittedCount,
      omittedSamples,
      unexpectedCount,
      nonEmptyMarkets,
      points,
      longUsd: round2(totalLongUsd),
      shortUsd: round2(totalShortUsd),
      totalUsd: round2(totalLongUsd + totalShortUsd),
      topMarkets,
      allRequestsCompleted,
      everyRequestedSymbolReturned,
    },
    apiKeyPrinted: false,
    rawPayloadPrinted: false,
    productionEligible: false,
    aggregateEligible: false,
    nextGate: qualification === 'COMPLETE_RETURNED_SYMBOL_SWEEP'
      ? 'Compare the 24h total against an independent same-time benchmark; do not activate yet.'
      : qualification === 'HYPERLIQUID_SYMBOLS_OMITTED_WITH_WORKING_CONTROL'
        ? 'Coinalyze liquidation-history is working for the positive control but does not expose usable Hyperliquid rows in this window; do not use as fallback.'
        : 'Keep Coinalyze inconclusive/research-only; do not infer zero liquidation from omitted symbols.',
  }, null, 2));
}

main().catch(error => {
  console.error(JSON.stringify({
    provider: 'Coinalyze',
    qualification: 'PROBE_ERROR',
    error: String(error?.message || error),
    apiKeyPrinted: false,
    productionEligible: false,
    aggregateEligible: false,
  }));
  process.exitCode = 1;
});
