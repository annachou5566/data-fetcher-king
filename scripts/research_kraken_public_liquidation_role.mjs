#!/usr/bin/env node

const TIMEOUT_MS = 15_000;
const SYMBOLS = ['PI_XBTUSD', 'PI_ETHUSD'];
const ORDER_BASE = 'https://futures.kraken.com/api/history/v3/market';
const TRADE_BASE = 'https://futures.kraken.com/derivatives/api/v3/history';
const LOOKBACK_MS = 6 * 60 * 60 * 1000;

const text = value => String(value ?? '').trim();

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'Cache-Control': 'no-cache',
      'User-Agent': 'data-fetcher-king/kraken-public-liquidation-role-research',
    },
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const body = await response.text();
  let payload;
  try { payload = JSON.parse(body); }
  catch { throw new Error(`non-json ${response.status} from ${new URL(url).hostname}`); }
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
  return payload;
}

function increment(map, key) {
  const safe = text(key) || '<empty>';
  map[safe] = (map[safe] || 0) + 1;
}

function inspectOrderElement(element) {
  const event = element?.event && typeof element.event === 'object' ? element.event : {};
  const variants = Object.keys(event);
  const variant = variants.length === 1 ? variants[0] : variants.join('+') || '<unknown>';
  const body = variants.length === 1 && event[variant] && typeof event[variant] === 'object' ? event[variant] : event;
  const order = body?.order || body?.newOrder || body?.oldOrder || null;
  const reason = text(body?.reason || order?.reason);
  const orderType = text(order?.orderType);
  const direction = text(order?.direction);
  const reduceOnly = typeof order?.reduceOnly === 'boolean' ? order.reduceOnly : null;
  const liquidMarker = `${reason} ${orderType}`.toLowerCase().includes('liquid') || JSON.stringify(event).toLowerCase().includes('liquid');
  return { variant, reason, orderType, direction, reduceOnly, liquidMarker };
}

async function scanOrders(symbol) {
  const since = Date.now() - LOOKBACK_MS;
  const query = new URLSearchParams({ since: String(since), sort: 'desc', count: '500' });
  const payload = await fetchJson(`${ORDER_BASE}/${encodeURIComponent(symbol)}/orders?${query}`);
  const elements = Array.isArray(payload?.elements) ? payload.elements : [];
  const counts = { variants: {}, reasons: {}, orderTypes: {}, directions: {}, reduceOnly: { true: 0, false: 0, unknown: 0 } };
  const liquidationLike = [];

  for (const element of elements) {
    const row = inspectOrderElement(element);
    increment(counts.variants, row.variant);
    increment(counts.reasons, row.reason);
    increment(counts.orderTypes, row.orderType);
    increment(counts.directions, row.direction);
    counts.reduceOnly[row.reduceOnly === true ? 'true' : row.reduceOnly === false ? 'false' : 'unknown'] += 1;
    if (row.liquidMarker && liquidationLike.length < 20) {
      liquidationLike.push({
        timestamp: Number.isFinite(Number(element?.timestamp)) ? Number(element.timestamp) : null,
        variant: row.variant,
        reason: row.reason || null,
        orderType: row.orderType || null,
        direction: row.direction || null,
        reduceOnly: row.reduceOnly,
      });
    }
  }

  return {
    ok: Array.isArray(payload?.elements),
    rows: elements.length,
    continuation: Boolean(payload?.continuationToken),
    counts,
    liquidationLike,
  };
}

async function scanTrades(symbol) {
  const payload = await fetchJson(`${TRADE_BASE}?${new URLSearchParams({ symbol })}`);
  const rows = Array.isArray(payload?.history) ? payload.history : [];
  const liquidations = rows.filter(row => text(row?.type).toLowerCase() === 'liquidation');
  return {
    ok: payload?.result === 'success' && Array.isArray(payload?.history),
    rows: rows.length,
    liquidationRows: liquidations.length,
    samples: liquidations.slice(0, 10).map(row => ({
      tradeId: row.trade_id ?? null,
      time: row.time ?? null,
      takerSide: row.side ?? null,
      price: row.price ?? null,
      size: row.size ?? null,
      notionalAmount: row.notional_amount ?? null,
      notionalCurrency: row.notional_currency ?? null,
    })),
  };
}

const report = {
  checkedAt: new Date().toISOString(),
  readOnly: true,
  credentialsUsed: false,
  runtimeMutation: false,
  symbols: {},
};

for (const symbol of SYMBOLS) {
  try {
    const [orders, trades] = await Promise.all([scanOrders(symbol), scanTrades(symbol)]);
    report.symbols[symbol] = { orders, trades };
  } catch (error) {
    report.symbols[symbol] = { error: String(error?.message || error) };
  }
}

report.orderEndpointGate = SYMBOLS.every(symbol => report.symbols[symbol]?.orders?.ok === true);
report.tradeEndpointGate = SYMBOLS.every(symbol => report.symbols[symbol]?.trades?.ok === true);
report.publicLiquidationRoleMarkerFound = SYMBOLS.some(symbol => (report.symbols[symbol]?.orders?.liquidationLike?.length || 0) > 0);
report.krakenWaveSideReady = false;
report.reason = report.publicLiquidationRoleMarkerFound
  ? 'public order events contain a liquidation-like marker; exact trade/order role join still requires proof'
  : 'no public order-event liquidation marker observed; trade side remains taker-side only';

console.log(`KRAKEN_PUBLIC_ROLE_RESEARCH=${JSON.stringify(report)}`);
if (!report.orderEndpointGate || !report.tradeEndpointGate) process.exit(1);
