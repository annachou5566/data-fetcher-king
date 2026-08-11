#!/usr/bin/env node

const BASE = 'https://test-klinechart.wave-alpha.pages.dev';
const TIMEOUT_MS = 15_000;
const nonce = Date.now();

async function get(path) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const separator = path.includes('?') ? '&' : '?';
    const response = await fetch(`${BASE}${path}${separator}audit=${nonce}`, {
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        'Cache-Control': 'no-cache',
        'User-Agent': 'data-fetcher-king/wave-hyperliquid-gap',
      },
    });
    const text = await response.text();
    let body;
    try { body = JSON.parse(text); }
    catch {
      throw new Error(`non-json ${response.status} ${response.headers.get('content-type') || ''} prefix=${JSON.stringify(text.slice(0, 40))}`);
    }
    return { status: response.status, body };
  } finally {
    clearTimeout(timer);
  }
}

function sumRows(rows) {
  let totalUsd = 0;
  let longUsd = 0;
  let shortUsd = 0;
  let events = 0;
  for (const row of Array.isArray(rows) ? rows : []) {
    totalUsd += Number(row?.totalUsd) || 0;
    longUsd += Number(row?.longUsd) || 0;
    shortUsd += Number(row?.shortUsd) || 0;
    events += Number(row?.count) || 0;
  }
  return {
    totalUsd: Math.round(totalUsd * 100) / 100,
    longUsd: Math.round(longUsd * 100) / 100,
    shortUsd: Math.round(shortUsd * 100) / 100,
    events,
  };
}

const history = await get('/api/liquidations/history?range=1d&symbol=ALL&exchange=hyperliquid-perp');
const exchanges = await get('/api/liquidations/exchanges?window=24h&symbol=ALL');

const historyTotal = sumRows(history.body?.rows);
const exchangeRows = [
  ...(Array.isArray(exchanges.body?.exchanges) ? exchanges.body.exchanges : []),
  ...(Array.isArray(exchanges.body?.partialExchanges) ? exchanges.body.partialExchanges : []),
];
const exchangeRow = exchangeRows.find(row => row?.exchange === 'hyperliquid-perp') || null;

console.log(`WAVE_HYPERLIQUID_24H_GAP=${JSON.stringify({
  checkedAt: new Date().toISOString(),
  readOnly: true,
  historyStatus: history.status,
  historySelectedExchange: history.body?.selectedExchange ?? null,
  historyStorage: history.body?.storage ?? null,
  historyUsedCoordinator: history.body?.diagnostics?.usedCoordinator ?? null,
  historyError: history.body?.error ?? null,
  historyTotal,
  exchangesStatus: exchanges.status,
  exchangeRow: exchangeRow ? {
    liquidationUsd: Number(exchangeRow.liquidationUsd) || 0,
    longUsd: Number(exchangeRow.longUsd) || 0,
    shortUsd: Number(exchangeRow.shortUsd) || 0,
    count: Number(exchangeRow.count) || 0,
    shareRate: Number(exchangeRow.shareRate) || 0,
    aggregateEligible: exchangeRow.aggregateEligible ?? null,
    coverageClass: exchangeRow.coverageClass ?? null,
  } : null,
  exchangesAllUsd: Number(exchanges.body?.all?.liquidationUsd) || 0,
  exchangesStorage: exchanges.body?.storage ?? null,
  exchangesUsedCoordinator: exchanges.body?.diagnostics?.usedCoordinator ?? null,
  exchangesError: exchanges.body?.error ?? null,
})}`);
