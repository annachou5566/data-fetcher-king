#!/usr/bin/env node

const STATS_URL = 'https://stats-data.hyperliquid.xyz/Mainnet/vaults';
const INFO_URL = 'https://api.hyperliquid.xyz/info';
const WS_URL = 'wss://api.hyperliquid.xyz/ws';
const WINDOW_MS = 60_000;
const LOOKBACK_MS = 24 * 60 * 60 * 1000;
const HTTP_TIMEOUT_MS = 12_000;
const MAX_TARGETS = 4;
const MAX_HISTORY_PAGES = 6;
const MAX_SAMPLES = 12;

if (typeof WebSocket !== 'function') throw new Error('global WebSocket unavailable');

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  source: 'stats-data.hyperliquid.xyz + api.hyperliquid.xyz/info + api.hyperliquid.xyz/ws',
  lookbackHours: 24,
  targetNames: [],
  targetCount: 0,
  historyRequests: 0,
  historyRows: 0,
  historyLiquidationLedgerUpdates: 0,
  historyLiquidationNtlTotal: 0,
  historyLiquidationNtlFieldsObserved: 0,
  historyTruncatedTargets: 0,
  fillRows: 0,
  liquidationFills: 0,
  marketLiquidationFills: 0,
  backstopLiquidationFills: 0,
  backstopFillUsd: 0,
  fillTruncatedTargets: 0,
  subscriptionRequests: 0,
  subscriptionAcks: 0,
  userEventMessages: 0,
  ledgerMessages: 0,
  liquidationUserEvents: 0,
  liquidationLedgerUpdates: 0,
  liveLiquidationNtlTotal: 0,
  samples: [],
  errors: [],
};

function finiteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function safeLiquidatedPositions(value) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, 20).map(item => ({
    coin: String(item?.coin || '').slice(0, 40),
    szi: String(item?.szi ?? '').slice(0, 50),
  }));
}

function recordSample(kind, payload) {
  if (report.samples.length >= MAX_SAMPLES) return;
  if (kind.includes('fill')) {
    report.samples.push({
      kind,
      coin: String(payload?.coin || '').slice(0, 40),
      method: String(payload?.liquidation?.method || '').slice(0, 20),
      dir: String(payload?.dir || '').slice(0, 32),
      side: String(payload?.side || '').slice(0, 8),
      crossed: payload?.crossed === true,
      px: finiteNumber(payload?.px),
      sz: finiteNumber(payload?.sz),
      time: finiteNumber(payload?.time),
      keys: payload && typeof payload === 'object' ? Object.keys(payload).sort().slice(0, 30) : [],
    });
    return;
  }
  report.samples.push({
    kind,
    liquidatedNtlPos: finiteNumber(payload?.liquidated_ntl_pos ?? payload?.liquidatedNtlPos),
    liquidatedAccountValue: finiteNumber(payload?.liquidated_account_value ?? payload?.accountValue),
    leverageType: String(payload?.leverageType || '').slice(0, 32) || null,
    liquidatedPositions: safeLiquidatedPositions(payload?.liquidatedPositions),
    keys: payload && typeof payload === 'object' ? Object.keys(payload).sort().slice(0, 30) : [],
  });
}

function candidateNames(value) {
  const name = String(value || '').trim();
  return /^liquidator$/i.test(name) || /hyperliquidity provider|\bhlp\b/i.test(name);
}

function candidateRank(name) {
  if (/^liquidator$/i.test(name)) return 0;
  if (/hyperliquidity provider/i.test(name)) return 1;
  return 2;
}

async function fetchJson(url, options) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchVaultTargets() {
  const payload = await fetchJson(STATS_URL, {
    headers: { Accept: 'application/json', 'User-Agent': 'WaveAlphaResearch/1.0' },
  });
  const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.vaults) ? payload.vaults : [];
  const targets = new Map();
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue;
    const summary = row.summary && typeof row.summary === 'object' ? row.summary : row;
    const name = String(summary.name || '').trim();
    if (!candidateNames(name)) continue;
    const address = String(
      summary.vaultAddress || summary.vault_address || summary.address || '',
    ).trim().toLowerCase();
    if (/^0x[0-9a-f]{40}$/.test(address)) targets.set(address, name || 'protocol-vault');
    const children = summary?.relationship?.data?.childAddresses
      || summary?.relationship?.data?.child_addresses;
    if (Array.isArray(children)) {
      for (const child of children) {
        const value = String(child || '').trim().toLowerCase();
        if (/^0x[0-9a-f]{40}$/.test(value)) targets.set(value, `${name || 'protocol-vault'} child`);
      }
    }
  }
  const selected = [...targets.entries()]
    .sort((a, b) => candidateRank(a[1]) - candidateRank(b[1]) || a[1].localeCompare(b[1]))
    .slice(0, MAX_TARGETS);
  report.targetNames = [...new Set(selected.map(([, name]) => name))].sort();
  report.targetCount = selected.length;
  return selected.map(([address]) => address);
}

async function info(payload) {
  report.historyRequests += 1;
  return fetchJson(INFO_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'WaveAlphaResearch/1.0' },
    body: JSON.stringify(payload),
  });
}

function liquidationDelta(update) {
  const delta = update?.delta || update;
  return delta && typeof delta === 'object' && String(delta.type || '').toLowerCase() === 'liquidation'
    ? delta
    : null;
}

async function scanLedgerHistory(targets) {
  const now = Date.now();
  const from = now - LOOKBACK_MS;
  const seen = new Set();

  for (const user of targets) {
    let cursor = from;
    let truncated = false;
    for (let page = 0; page < MAX_HISTORY_PAGES && cursor <= now; page += 1) {
      const rows = await info({
        type: 'userNonFundingLedgerUpdates',
        user,
        startTime: cursor,
        endTime: now,
      });
      if (!Array.isArray(rows)) throw new Error('ledger history response is not an array');
      report.historyRows += rows.length;
      let maxTime = cursor - 1;
      for (const update of rows) {
        const time = finiteNumber(update?.time);
        if (time != null) maxTime = Math.max(maxTime, time);
        const delta = liquidationDelta(update);
        if (!delta) continue;
        const identity = `${String(update?.hash || '')}:${time ?? ''}:${JSON.stringify(delta)}`;
        if (seen.has(identity)) continue;
        seen.add(identity);
        report.historyLiquidationLedgerUpdates += 1;
        const ntl = finiteNumber(delta.liquidatedNtlPos ?? delta.liquidated_ntl_pos);
        if (ntl != null && ntl >= 0) {
          report.historyLiquidationNtlFieldsObserved += 1;
          report.historyLiquidationNtlTotal += ntl;
        }
        recordSample('history.ledger.liquidation', delta);
      }
      if (rows.length < 500) break;
      if (maxTime < cursor) {
        report.errors.push('ledger-pagination-no-progress');
        break;
      }
      cursor = maxTime + 1;
      if (page === MAX_HISTORY_PAGES - 1) truncated = true;
    }
    if (truncated) report.historyTruncatedTargets += 1;
  }
}

async function scanFillHistory(targets) {
  const now = Date.now();
  const from = now - LOOKBACK_MS;
  const seen = new Set();

  for (const user of targets) {
    let cursor = from;
    let truncated = false;
    for (let page = 0; page < MAX_HISTORY_PAGES && cursor <= now; page += 1) {
      const rows = await info({
        type: 'userFillsByTime',
        user,
        startTime: cursor,
        endTime: now,
        aggregateByTime: false,
      });
      if (!Array.isArray(rows)) throw new Error('fill history response is not an array');
      report.fillRows += rows.length;
      let maxTime = cursor - 1;
      for (const fill of rows) {
        const time = finiteNumber(fill?.time);
        if (time != null) maxTime = Math.max(maxTime, time);
        const method = String(fill?.liquidation?.method || '').toLowerCase();
        if (method !== 'market' && method !== 'backstop') continue;
        const identity = [fill?.time, fill?.coin, fill?.tid, method, fill?.liquidation?.liquidatedUser].join(':');
        if (seen.has(identity)) continue;
        seen.add(identity);
        report.liquidationFills += 1;
        if (method === 'market') report.marketLiquidationFills += 1;
        if (method === 'backstop') {
          report.backstopLiquidationFills += 1;
          const px = finiteNumber(fill?.px);
          const sz = finiteNumber(fill?.sz);
          if (px != null && px > 0 && sz != null && sz > 0) report.backstopFillUsd += px * sz;
          recordSample('history.fill.backstop', fill);
        }
      }
      if (rows.length < 2000) break;
      if (maxTime < cursor) {
        report.errors.push('fill-pagination-no-progress');
        break;
      }
      cursor = maxTime + 1;
      if (page === MAX_HISTORY_PAGES - 1) truncated = true;
    }
    if (truncated) report.fillTruncatedTargets += 1;
  }
}

function inspectUserEvent(data) {
  report.userEventMessages += 1;
  const liq = data?.liquidation;
  if (!liq || typeof liq !== 'object') return;
  report.liquidationUserEvents += 1;
  const ntl = finiteNumber(liq.liquidated_ntl_pos);
  if (ntl != null && ntl >= 0) report.liveLiquidationNtlTotal += ntl;
  recordSample('live.userEvents.liquidation', liq);
}

function inspectLedger(data) {
  report.ledgerMessages += 1;
  const updates = Array.isArray(data) ? data : Array.isArray(data?.updates) ? data.updates : [data];
  for (const update of updates) {
    const delta = liquidationDelta(update);
    if (!delta) continue;
    report.liquidationLedgerUpdates += 1;
    const ntl = finiteNumber(delta.liquidatedNtlPos ?? delta.liquidated_ntl_pos);
    if (ntl != null && ntl >= 0) report.liveLiquidationNtlTotal += ntl;
    recordSample('live.ledger.liquidation', delta);
  }
}

async function main() {
  let targets;
  try {
    targets = await fetchVaultTargets();
  } catch (error) {
    report.errors.push(`vault-discovery:${String(error?.message || error)}`);
    console.log(`HYPERLIQUID_PROTOCOL_VAULT_RESEARCH=${JSON.stringify(report)}`);
    process.exit(1);
  }
  if (!targets.length) {
    report.errors.push('no-protocol-vault-targets');
    console.log(`HYPERLIQUID_PROTOCOL_VAULT_RESEARCH=${JSON.stringify(report)}`);
    process.exit(1);
  }

  try {
    await scanLedgerHistory(targets);
  } catch (error) {
    report.errors.push(`ledger-history:${String(error?.message || error)}`);
  }
  try {
    await scanFillHistory(targets);
  } catch (error) {
    report.errors.push(`fill-history:${String(error?.message || error)}`);
  }

  const ws = new WebSocket(WS_URL);
  let finished = false;
  const finish = code => {
    if (finished) return;
    finished = true;
    try { ws.close(); } catch {}
    report.historyLiquidationNtlTotal = Math.round(report.historyLiquidationNtlTotal * 100) / 100;
    report.backstopFillUsd = Math.round(report.backstopFillUsd * 100) / 100;
    report.liveLiquidationNtlTotal = Math.round(report.liveLiquidationNtlTotal * 100) / 100;
    report.backstopSurfaceObserved = (
      report.historyLiquidationLedgerUpdates
      + report.backstopLiquidationFills
      + report.liquidationUserEvents
      + report.liquidationLedgerUpdates
    ) > 0;
    console.log(`HYPERLIQUID_PROTOCOL_VAULT_RESEARCH=${JSON.stringify(report)}`);
    process.exit(code);
  };

  const timer = setTimeout(() => finish(0), WINDOW_MS);
  ws.addEventListener('open', () => {
    for (const user of targets) {
      for (const type of ['userEvents', 'userNonFundingLedgerUpdates']) {
        report.subscriptionRequests += 1;
        ws.send(JSON.stringify({ method: 'subscribe', subscription: { type, user } }));
      }
    }
  });
  ws.addEventListener('message', event => {
    let message;
    try { message = JSON.parse(String(event.data)); }
    catch {
      report.errors.push('non-json-message');
      return;
    }
    if (message?.channel === 'subscriptionResponse') {
      report.subscriptionAcks += 1;
      return;
    }
    if (message?.channel === 'userEvents' || message?.channel === 'user') inspectUserEvent(message.data);
    if (message?.channel === 'userNonFundingLedgerUpdates') inspectLedger(message.data);
    if (message?.channel === 'error') report.errors.push('channel-error');
  });
  ws.addEventListener('error', () => report.errors.push('websocket-error'));
  ws.addEventListener('close', event => {
    if (!finished && event.code !== 1000) {
      clearTimeout(timer);
      report.errors.push(`early-close:${event.code}`);
      finish(report.subscriptionAcks > 0 ? 0 : 1);
    }
  });
}

main().catch(error => {
  report.errors.push(String(error?.message || error));
  console.log(`HYPERLIQUID_PROTOCOL_VAULT_RESEARCH=${JSON.stringify(report)}`);
  process.exit(1);
});
