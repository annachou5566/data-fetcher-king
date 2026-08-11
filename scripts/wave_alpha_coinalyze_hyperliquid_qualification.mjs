#!/usr/bin/env node

const API_BASE = 'https://api.coinalyze.net/v1';
const API_KEY = String(process.env.COINALYZE_API_KEY || '').trim();
const TIMEOUT_MS = 20_000;
const RATE_WINDOW_MS = 61_000;
const RATE_BUDGET_UNITS = 38;
const HISTORY_INTERVAL = '1hour';
const LOOKBACK_SEC = 24 * 60 * 60;

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
  const totalsBySymbol = new Map();
  let queried = 0;
  let points = 0;
  let nonEmptyMarkets = 0;
  let cursor = 0;
  let historyQualification = 'OK';
  let historyHttpStatus = null;

  while (cursor < hyperliquidMarkets.length) {
    const now = Date.now();
    if (now - rateWindowStartedAt >= RATE_WINDOW_MS) {
      rateWindowStartedAt = now;
      usedUnits = 0;
    }
    const available = Math.max(1, RATE_BUDGET_UNITS - usedUnits);
    const batchSize = Math.min(20, available, hyperliquidMarkets.length - cursor);
    if (available <= 0) {
      await reserveUnits(1);
      usedUnits -= 1;
      continue;
    }
    const batch = hyperliquidMarkets.slice(cursor, cursor + batchSize);

    let payload;
    try {
      payload = await apiGet('/liquidation-history', {
        symbols: batch.map(row => row.symbol).join(','),
        interval: HISTORY_INTERVAL,
        from,
        to,
        convert_to_usd: 'true',
      }, batch.length, 1);
    } catch (error) {
      historyQualification = error.message;
      historyHttpStatus = error.status || null;
      break;
    }

    if (!Array.isArray(payload)) throw new Error('unexpected liquidation-history contract');
    const returned = new Set();
    for (const item of payload) {
      const symbol = String(item?.symbol || '');
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
      if (symbolPoints > 0) nonEmptyMarkets += 1;
      points += symbolPoints;
      totalsBySymbol.set(symbol, {
        longUsd: round2(longUsd),
        shortUsd: round2(shortUsd),
        totalUsd: round2(longUsd + shortUsd),
        points: symbolPoints,
      });
    }

    queried += batch.length;
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

  const completeSweep = historyQualification === 'OK' && queried === hyperliquidMarkets.length;
  console.log(JSON.stringify({
    checkedAt,
    provider: 'Coinalyze',
    vantage: 'github-hosted-public-runner',
    qualification: completeSweep ? 'COMPLETE_DISCOVERED_MARKET_SWEEP' : historyQualification,
    historyHttpStatus,
    discovery,
    history: {
      interval: HISTORY_INTERVAL,
      from,
      to,
      convertToUsd: true,
      marketsQueried: queried,
      marketsDiscovered: hyperliquidMarkets.length,
      nonEmptyMarkets,
      points,
      longUsd: round2(totalLongUsd),
      shortUsd: round2(totalShortUsd),
      totalUsd: round2(totalLongUsd + totalShortUsd),
      topMarkets,
      completeDiscoveredMarketSweep: completeSweep,
    },
    apiKeyPrinted: false,
    rawPayloadPrinted: false,
    productionEligible: false,
    aggregateEligible: false,
    nextGate: completeSweep
      ? 'Compare this discovered-market 24h total against independent benchmark at the same time; do not activate yet.'
      : 'Do not infer provider quality from an incomplete or vantage-blocked sweep.',
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
