#!/usr/bin/env node

const STATS_URL = 'https://stats-data.hyperliquid.xyz/Mainnet/vaults';
const WS_URL = 'wss://api.hyperliquid.xyz/ws';
const WINDOW_MS = 120_000;
const HTTP_TIMEOUT_MS = 12_000;
const MAX_TARGETS = 12;
const MAX_SAMPLES = 8;

if (typeof WebSocket !== 'function') throw new Error('global WebSocket unavailable');

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  source: 'stats-data.hyperliquid.xyz + api.hyperliquid.xyz/ws',
  targetNames: [],
  targetCount: 0,
  subscriptionRequests: 0,
  subscriptionAcks: 0,
  userEventMessages: 0,
  ledgerMessages: 0,
  liquidationUserEvents: 0,
  liquidationLedgerUpdates: 0,
  liquidationNtlTotal: 0,
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
  return /hyperliquidity provider|\bhlp\b|\bliquidator\b/i.test(String(value || ''));
}

async function fetchVaultTargets() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    const response = await fetch(STATS_URL, {
      signal: controller.signal,
      headers: { Accept: 'application/json', 'User-Agent': 'WaveAlphaResearch/1.0' },
    });
    if (!response.ok) throw new Error(`stats HTTP ${response.status}`);
    const payload = await response.json();
    const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.vaults) ? payload.vaults : [];
    const targets = new Map();
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      const name = String(row.name || row.summary?.name || '').trim();
      if (!candidateNames(name)) continue;
      const address = String(row.vaultAddress || row.vault_address || row.address || '').trim().toLowerCase();
      if (/^0x[0-9a-f]{40}$/.test(address)) targets.set(address, name || 'protocol-vault');
      const children = row?.relationship?.data?.childAddresses;
      if (Array.isArray(children)) {
        for (const child of children) {
          const value = String(child || '').trim().toLowerCase();
          if (/^0x[0-9a-f]{40}$/.test(value)) targets.set(value, `${name || 'protocol-vault'} child`);
        }
      }
    }
    const selected = [...targets.entries()].slice(0, MAX_TARGETS);
    report.targetNames = [...new Set(selected.map(([, name]) => name))].sort();
    report.targetCount = selected.length;
    return selected.map(([address]) => address);
  } finally {
    clearTimeout(timer);
  }
}

function inspectUserEvent(data) {
  report.userEventMessages += 1;
  const liq = data?.liquidation;
  if (!liq || typeof liq !== 'object') return;
  report.liquidationUserEvents += 1;
  const ntl = finiteNumber(liq.liquidated_ntl_pos);
  if (ntl != null && ntl >= 0) report.liquidationNtlTotal += ntl;
  recordSample('userEvents.liquidation', liq);
}

function inspectLedger(data) {
  report.ledgerMessages += 1;
  const updates = Array.isArray(data) ? data : Array.isArray(data?.updates) ? data.updates : [data];
  for (const update of updates) {
    const delta = update?.delta || update;
    if (!delta || typeof delta !== 'object' || String(delta.type || '').toLowerCase() !== 'liquidation') continue;
    report.liquidationLedgerUpdates += 1;
    const ntl = finiteNumber(delta.liquidatedNtlPos);
    if (ntl != null && ntl >= 0) report.liquidationNtlTotal += ntl;
    recordSample('userNonFundingLedgerUpdates.liquidation', delta);
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

  const ws = new WebSocket(WS_URL);
  let finished = false;
  const finish = code => {
    if (finished) return;
    finished = true;
    try { ws.close(); } catch {}
    report.liquidationNtlTotal = Math.round(report.liquidationNtlTotal * 100) / 100;
    report.backstopSurfaceObserved = report.liquidationUserEvents + report.liquidationLedgerUpdates > 0;
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
    if (message?.channel === 'userEvents') inspectUserEvent(message.data);
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
