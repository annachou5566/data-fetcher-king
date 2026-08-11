#!/usr/bin/env node

const URL = 'wss://www.deribit.com/ws/api/v2';
const CHANNELS = [
  'trades.future.any.100ms',
  'trades.future.BTC.100ms',
  'trades.future.ETH.100ms',
];
const WINDOW_MS = 45_000;

if (typeof WebSocket !== 'function') throw new Error('global WebSocket unavailable');

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  channels: Object.fromEntries(CHANNELS.map(channel => [channel, {
    subscribed: false,
    notifications: 0,
    trades: 0,
    liquidationRows: 0,
    liquidationCodes: { M: 0, T: 0, MT: 0 },
    instruments: [],
    fieldKeys: [],
  }])),
  errors: [],
};

const instruments = Object.fromEntries(CHANNELS.map(channel => [channel, new Set()]));
const keys = Object.fromEntries(CHANNELS.map(channel => [channel, new Set()]));
let finished = false;
let ws;

function finish(code = 0) {
  if (finished) return;
  finished = true;
  for (const channel of CHANNELS) {
    report.channels[channel].instruments = [...instruments[channel]].sort().slice(0, 100);
    report.channels[channel].fieldKeys = [...keys[channel]].sort();
  }
  report.allSubscribed = CHANNELS.every(channel => report.channels[channel].subscribed);
  report.anyChannelDelivered = report.channels['trades.future.any.100ms'].trades > 0;
  report.specificChannelsDelivered = report.channels['trades.future.BTC.100ms'].trades > 0
    || report.channels['trades.future.ETH.100ms'].trades > 0;
  try { ws?.close(); } catch {}
  console.log(`DERIBIT_FUTURES_WS_COMPARISON=${JSON.stringify(report)}`);
  process.exit(code);
}

const timer = setTimeout(() => {
  const allSubscribed = CHANNELS.every(channel => report.channels[channel].subscribed);
  if (!allSubscribed) report.errors.push('subscription-timeout-or-rejection');
  finish(allSubscribed ? 0 : 1);
}, WINDOW_MS);

try {
  ws = new WebSocket(URL);
  ws.addEventListener('open', () => {
    ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'public/subscribe',
      params: { channels: CHANNELS },
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
      const accepted = Array.isArray(message?.result) ? message.result : [];
      for (const channel of CHANNELS) report.channels[channel].subscribed = accepted.includes(channel);
      if (message?.error) report.errors.push(`subscription-error:${JSON.stringify(message.error)}`);
      return;
    }

    if (message?.method !== 'subscription') return;
    const channel = String(message?.params?.channel || '');
    if (!CHANNELS.includes(channel)) return;
    const stats = report.channels[channel];
    stats.notifications += 1;
    const rows = Array.isArray(message?.params?.data) ? message.params.data : [];
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      stats.trades += 1;
      for (const key of Object.keys(row)) keys[channel].add(key);
      if (row.instrument_name) instruments[channel].add(String(row.instrument_name));
      const code = String(row.liquidation || '');
      if (['M', 'T', 'MT'].includes(code)) {
        stats.liquidationRows += 1;
        stats.liquidationCodes[code] += 1;
      }
    }
  });

  ws.addEventListener('error', () => report.errors.push('websocket-error'));
  ws.addEventListener('close', event => {
    if (!finished && event.code !== 1000) {
      report.errors.push(`early-close:${event.code}`);
      clearTimeout(timer);
      finish(CHANNELS.every(channel => report.channels[channel].subscribed) ? 0 : 1);
    }
  });
} catch (error) {
  clearTimeout(timer);
  report.errors.push(String(error?.message || error));
  finish(1);
}
