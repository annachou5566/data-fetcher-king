#!/usr/bin/env node

const API_KEY = String(process.env.HYDROMANCER_API_KEY || '').trim();
const WS_URL = 'wss://api.hydromancer.xyz/ws';
const MAX_RUNTIME_MS = 180_000;
const MIN_RUNTIME_AFTER_FIRST_LIQ_MS = 20_000;
const TARGET_LIQUIDATIONS = 5;

const nowIso = () => new Date().toISOString();
const finitePositive = value => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
};
const lower = value => String(value ?? '').trim().toLowerCase();
const sortedKeys = value => value && typeof value === 'object' && !Array.isArray(value)
  ? Object.keys(value).sort()
  : [];

if (!API_KEY) {
  console.log(JSON.stringify({
    checkedAt: nowIso(),
    provider: 'Hydromancer',
    qualification: 'SECRET_UNAVAILABLE',
    apiKeyPrinted: false,
    rawPayloadPrinted: false,
    addressValuesPrinted: false,
    productionEligible: false,
    aggregateEligible: false,
  }, null, 2));
  process.exit(2);
}

const state = {
  checkedAt: nowIso(),
  provider: 'Hydromancer',
  vantage: 'github-hosted-public-runner',
  endpoint: 'allUserNonFundingLedgerEvents',
  transportReachable: false,
  connectedMessageSeen: false,
  subscriptionRequested: false,
  subscriptionAckSeen: false,
  pingCount: 0,
  pongCount: 0,
  messageCount: 0,
  ledgerBatchCount: 0,
  ledgerEventCount: 0,
  eventTypes: {},
  liquidationEvents: 0,
  liquidationUniqueEvents: 0,
  duplicateLiquidationEvents: 0,
  liquidationNotionalCandidateUsd: 0,
  liquidationNotionalMissing: 0,
  liquidationNotionalFields: {},
  liquidationTopLevelKeySets: {},
  liquidationDeltaKeySets: {},
  liquidatedPositionShapes: {},
  positionSigns: { positive: 0, negative: 0, zero: 0, invalid: 0 },
  firstLiquidationObservedAt: null,
  lastLiquidationObservedAt: null,
  errorTypes: {},
  closeCode: null,
  closeReasonPresent: false,
  apiKeyPrinted: false,
  rawPayloadPrinted: false,
  addressValuesPrinted: false,
  productionEligible: false,
  aggregateEligible: false,
};

const liquidationIds = new Set();
let websocket;
let settled = false;
let firstLiquidationWallMs = null;
let hardTimer;
let softTimer;

function bump(obj, key) {
  const normalized = String(key || 'UNKNOWN');
  obj[normalized] = (obj[normalized] || 0) + 1;
}

function bumpKeySet(obj, keys) {
  const signature = keys.join(',') || '(none)';
  obj[signature] = (obj[signature] || 0) + 1;
}

function liquidationPayload(event) {
  const delta = event?.delta && typeof event.delta === 'object' && !Array.isArray(event.delta)
    ? event.delta
    : null;
  const directType = lower(event?.eventType || event?.type);
  const deltaType = lower(delta?.eventType || delta?.type);
  if (directType === 'liquidation') return { root: event, payload: event, delta };
  if (deltaType === 'liquidation') return { root: event, payload: delta, delta };
  return null;
}

function extractNotional(payload, root, delta) {
  const candidates = [
    ['liquidatedNtlPos', payload?.liquidatedNtlPos],
    ['liquidatedNtl', payload?.liquidatedNtl],
    ['root.liquidatedNtlPos', root?.liquidatedNtlPos],
    ['root.liquidatedNtl', root?.liquidatedNtl],
    ['delta.liquidatedNtlPos', delta?.liquidatedNtlPos],
    ['delta.liquidatedNtl', delta?.liquidatedNtl],
  ];
  for (const [field, value] of candidates) {
    const parsed = finitePositive(value);
    if (parsed !== null) return { field, value: parsed };
  }
  return null;
}

function inspectPositions(payload) {
  const positions = payload?.liquidatedPositions;
  if (!Array.isArray(positions)) {
    bump(state.liquidatedPositionShapes, positions == null ? 'missing' : typeof positions);
    return;
  }
  for (const position of positions) {
    let signedSize;
    if (Array.isArray(position)) {
      bump(state.liquidatedPositionShapes, 'tuple');
      signedSize = position[1];
    } else if (position && typeof position === 'object') {
      bump(state.liquidatedPositionShapes, 'object');
      signedSize = position.szi ?? position.size ?? position.sz;
    } else {
      bump(state.liquidatedPositionShapes, typeof position);
    }
    const n = Number(signedSize);
    if (!Number.isFinite(n)) state.positionSigns.invalid += 1;
    else if (n > 0) state.positionSigns.positive += 1;
    else if (n < 0) state.positionSigns.negative += 1;
    else state.positionSigns.zero += 1;
  }
}

function inspectLiquidation(event) {
  state.liquidationEvents += 1;
  const wrapped = liquidationPayload(event);
  if (!wrapped) return;
  const { root, payload, delta } = wrapped;

  const identity = [root?.time ?? payload?.time ?? '', root?.txIndex ?? payload?.txIndex ?? '', root?.role ?? payload?.role ?? ''].join(':');
  if (liquidationIds.has(identity)) {
    state.duplicateLiquidationEvents += 1;
    return;
  }
  liquidationIds.add(identity);
  state.liquidationUniqueEvents += 1;

  bumpKeySet(state.liquidationTopLevelKeySets, sortedKeys(root));
  if (delta) bumpKeySet(state.liquidationDeltaKeySets, sortedKeys(delta));

  const notional = extractNotional(payload, root, delta);
  if (notional) {
    bump(state.liquidationNotionalFields, notional.field);
    state.liquidationNotionalCandidateUsd += notional.value;
  } else {
    state.liquidationNotionalMissing += 1;
  }

  inspectPositions(payload);
  const observedAt = nowIso();
  if (!state.firstLiquidationObservedAt) state.firstLiquidationObservedAt = observedAt;
  state.lastLiquidationObservedAt = observedAt;

  if (firstLiquidationWallMs === null) firstLiquidationWallMs = Date.now();
  if (state.liquidationUniqueEvents >= TARGET_LIQUIDATIONS && !softTimer) {
    const elapsed = Date.now() - firstLiquidationWallMs;
    softTimer = setTimeout(() => finish('TARGET_LIQUIDATIONS_REACHED'), Math.max(0, MIN_RUNTIME_AFTER_FIRST_LIQ_MS - elapsed));
  }
}

function inspectLedgerMessage(msg) {
  const events = Array.isArray(msg?.events)
    ? msg.events
    : Array.isArray(msg?.data?.events)
      ? msg.data.events
      : [];
  if (!events.length) return;
  state.ledgerBatchCount += 1;
  state.ledgerEventCount += events.length;
  for (const event of events) {
    if (!event || typeof event !== 'object') continue;
    const delta = event.delta && typeof event.delta === 'object' && !Array.isArray(event.delta) ? event.delta : null;
    const eventType = event.eventType || event.type || delta?.eventType || delta?.type || 'UNKNOWN';
    bump(state.eventTypes, eventType);
    if (lower(eventType) === 'liquidation') inspectLiquidation(event);
  }
}

function sanitizeError(error) {
  const name = String(error?.name || error?.constructor?.name || 'Error');
  bump(state.errorTypes, name);
}

function finish(reason) {
  if (settled) return;
  settled = true;
  clearTimeout(hardTimer);
  clearTimeout(softTimer);
  state.finishedAt = nowIso();
  state.finishReason = reason;
  state.liquidationNotionalCandidateUsd = Math.round(state.liquidationNotionalCandidateUsd * 100) / 100;
  state.qualification = !state.transportReachable
    ? 'TRANSPORT_NOT_REACHED'
    : !state.connectedMessageSeen
      ? 'CONNECTED_PROTOCOL_NOT_CONFIRMED'
      : !state.subscriptionRequested
        ? 'SUBSCRIPTION_NOT_REQUESTED'
        : state.liquidationUniqueEvents > 0
          ? 'LIVE_LIQUIDATION_SCHEMA_OBSERVED'
          : state.ledgerEventCount > 0
            ? 'GLOBAL_LEDGER_LIVE_NO_LIQUIDATION_IN_WINDOW'
            : 'SUBSCRIBED_NO_LEDGER_EVENTS_IN_WINDOW';
  state.nextGate = state.liquidationUniqueEvents > 0
    ? 'Compare live liquidation notional/schema against independent benchmarks; do not activate production.'
    : 'Do not infer provider coverage from a no-liquidation sample; collect a bounded longer sample after access is confirmed.';
  console.log(JSON.stringify(state, null, 2));
  try { websocket?.close(); } catch {}
  setTimeout(() => process.exit(0), 50);
}

try {
  websocket = new WebSocket(`${WS_URL}?token=${encodeURIComponent(API_KEY)}`);
  websocket.addEventListener('open', () => {
    state.transportReachable = true;
  });
  websocket.addEventListener('message', event => {
    state.messageCount += 1;
    let msg;
    try { msg = JSON.parse(String(event.data)); } catch {
      bump(state.errorTypes, 'NonJsonMessage');
      return;
    }
    const type = lower(msg?.type || msg?.channel);
    if (type === 'connected') {
      state.connectedMessageSeen = true;
      websocket.send(JSON.stringify({
        type: 'subscribe',
        subscription: { type: 'allUserNonFundingLedgerEvents' },
      }));
      state.subscriptionRequested = true;
      return;
    }
    if (type === 'ping') {
      state.pingCount += 1;
      websocket.send(JSON.stringify({ type: 'pong' }));
      state.pongCount += 1;
      return;
    }
    if (type === 'subscriptionupdate' || type === 'subscribed') {
      state.subscriptionAckSeen = true;
    }
    if (type === 'allusernonfundingledgerevents') inspectLedgerMessage(msg);
    if (type === 'error') bump(state.errorTypes, 'ProviderErrorMessage');
  });
  websocket.addEventListener('error', event => sanitizeError(event?.error || event));
  websocket.addEventListener('close', event => {
    state.closeCode = Number.isFinite(Number(event?.code)) ? Number(event.code) : null;
    state.closeReasonPresent = Boolean(String(event?.reason || '').trim());
    if (!settled) finish('WEBSOCKET_CLOSED');
  });
  hardTimer = setTimeout(() => finish('BOUNDED_TIMEOUT'), MAX_RUNTIME_MS);
} catch (error) {
  sanitizeError(error);
  finish('CLIENT_SETUP_ERROR');
}
