#!/usr/bin/env node

const URL = 'wss://rpc.hyperliquid.xyz/ws';
const WINDOW_MS = 60_000;
const MAX_SAMPLES = 8;

if (typeof WebSocket !== 'function') throw new Error('global WebSocket unavailable');

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  subscription: 'explorerTxs',
  subscribed: false,
  messages: 0,
  transactions: 0,
  actionTypes: {},
  liquidationLikeTypes: {},
  liquidationLikeSamples: [],
  errors: [],
};

let ws;
let finished = false;

function looksSensitive(value) {
  if (typeof value !== 'string') return false;
  return /^0x[0-9a-fA-F]{40}$/.test(value) || /^0x[0-9a-fA-F]{64}$/.test(value);
}

function sanitize(value, depth = 0) {
  if (depth > 3) return '[depth-limit]';
  if (value == null || typeof value === 'boolean' || typeof value === 'number') return value;
  if (typeof value === 'string') {
    if (looksSensitive(value)) return '[redacted-hex]';
    return value.length > 120 ? `${value.slice(0, 117)}...` : value;
  }
  if (Array.isArray(value)) return value.slice(0, 8).map(item => sanitize(item, depth + 1));
  if (typeof value === 'object') {
    const out = {};
    for (const [key, item] of Object.entries(value).slice(0, 20)) {
      if (/user|address|signature|r$|s$/i.test(key)) {
        out[key] = '[redacted]';
      } else {
        out[key] = sanitize(item, depth + 1);
      }
    }
    return out;
  }
  return String(value);
}

function isLiquidationLike(type, action) {
  const t = String(type || '').toLowerCase();
  if (t.includes('liquid')) return true;
  const keys = action && typeof action === 'object' ? Object.keys(action) : [];
  return keys.some(key => key.toLowerCase().includes('liquid'));
}

function count(map, key) {
  map[key] = (map[key] || 0) + 1;
}

function processTransaction(tx) {
  if (!tx || typeof tx !== 'object') return;
  report.transactions += 1;
  const action = tx.action;
  const type = action && typeof action === 'object' ? String(action.type || 'UNKNOWN') : 'NO_ACTION';
  count(report.actionTypes, type);
  if (isLiquidationLike(type, action)) {
    count(report.liquidationLikeTypes, type);
    if (report.liquidationLikeSamples.length < MAX_SAMPLES) {
      report.liquidationLikeSamples.push({
        type,
        actionKeys: action && typeof action === 'object' ? Object.keys(action).sort() : [],
        action: sanitize(action),
        txKeys: Object.keys(tx).sort(),
      });
    }
  }
}

function processMessage(message) {
  if (!message || typeof message !== 'object') return;
  if (message.channel === 'subscriptionResponse') {
    const sub = message?.data?.subscription;
    if (sub?.type === 'explorerTxs') report.subscribed = true;
    return;
  }
  if (message.channel === 'pong') return;
  if (message.channel === 'error') {
    report.errors.push(`channel-error:${JSON.stringify(sanitize(message.data ?? null))}`);
    return;
  }
  if (message.channel !== 'explorerTxs') return;
  report.messages += 1;
  const data = message.data;
  if (Array.isArray(data)) {
    for (const tx of data) processTransaction(tx);
  } else {
    processTransaction(data);
  }
}

function finish(code = 0) {
  if (finished) return;
  finished = true;
  try { ws?.close(); } catch {}
  report.actionTypes = Object.fromEntries(Object.entries(report.actionTypes).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
  report.liquidationLikeTypes = Object.fromEntries(Object.entries(report.liquidationLikeTypes).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
  report.globalLiquidationActionObserved = Object.keys(report.liquidationLikeTypes).length > 0;
  console.log(`HYPERLIQUID_GLOBAL_EXPLORER_RESEARCH=${JSON.stringify(report)}`);
  process.exit(code);
}

const timer = setTimeout(() => {
  if (!report.subscribed) {
    report.errors.push('subscription-timeout');
    finish(1);
  } else {
    finish(0);
  }
}, WINDOW_MS);

try {
  ws = new WebSocket(URL);
  ws.addEventListener('open', () => {
    ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'explorerTxs' },
    }));
  });
  ws.addEventListener('message', event => {
    try {
      processMessage(JSON.parse(String(event.data)));
    } catch (error) {
      report.errors.push(`parse:${String(error?.message || error)}`);
    }
  });
  ws.addEventListener('error', () => report.errors.push('websocket-error'));
  ws.addEventListener('close', event => {
    if (!finished && event.code !== 1000) {
      clearTimeout(timer);
      report.errors.push(`early-close:${event.code}`);
      finish(report.subscribed ? 0 : 1);
    }
  });
} catch (error) {
  clearTimeout(timer);
  report.errors.push(String(error?.message || error));
  finish(1);
}
