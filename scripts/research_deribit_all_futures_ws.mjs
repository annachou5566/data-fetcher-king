#!/usr/bin/env node

const URL = 'wss://www.deribit.com/ws/api/v2';
const CHANNEL = 'trades.future.any.100ms';
const WINDOW_MS = 15_000;

if (typeof WebSocket !== 'function') throw new Error('global WebSocket unavailable');

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  channel: CHANNEL,
  subscribed: false,
  notifications: 0,
  trades: 0,
  liquidationRows: 0,
  liquidationCodes: { M: 0, T: 0, MT: 0 },
  instruments: [],
  fieldKeys: [],
  errors: [],
};

const instruments = new Set();
const keys = new Set();
let finished = false;
let ws;

function finish(code = 0) {
  if (finished) return;
  finished = true;
  report.instruments = [...instruments].sort().slice(0, 100);
  report.fieldKeys = [...keys].sort();
  try { ws?.close(); } catch {}
  console.log(`DERIBIT_ALL_FUTURES_WS=${JSON.stringify(report)}`);
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
      jsonrpc: '2.0',
      id: 1,
      method: 'public/subscribe',
      params: { channels: [CHANNEL] },
    }));
  });

  ws.addEventListener('message', event => {
    let message;
    try { message = JSON.parse(String(event.data)); }
    catch {
      report.errors.push('non-json-message');
      return;
    }

    if (message?.id === 1) {
      const accepted = Array.isArray(message?.result) && message.result.includes(CHANNEL);
      report.subscribed = accepted;
      if (!accepted) report.errors.push(`subscription-rejected:${JSON.stringify(message?.error || message?.result || null)}`);
      return;
    }

    if (message?.method !== 'subscription' || message?.params?.channel !== CHANNEL) return;
    report.notifications += 1;
    const rows = Array.isArray(message?.params?.data) ? message.params.data : [];
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      report.trades += 1;
      for (const key of Object.keys(row)) keys.add(key);
      if (row.instrument_name) instruments.add(String(row.instrument_name));
      const code = String(row.liquidation || '');
      if (['M', 'T', 'MT'].includes(code)) {
        report.liquidationRows += 1;
        report.liquidationCodes[code] += 1;
      }
    }
  });

  ws.addEventListener('error', () => {
    report.errors.push('websocket-error');
  });

  ws.addEventListener('close', event => {
    if (!finished && event.code !== 1000) {
      report.errors.push(`early-close:${event.code}`);
      clearTimeout(timer);
      finish(report.subscribed ? 0 : 1);
    }
  });
} catch (error) {
  clearTimeout(timer);
  report.errors.push(String(error?.message || error));
  finish(1);
}
