#!/usr/bin/env node

const TIMEOUT_MS = 15_000;
const COORDINATOR = 'https://wave-alpha-liquidation-coordinator.wavealpha.workers.dev';
const PAGES = 'https://test-klinechart.wave-alpha.pages.dev';
const PHASE_B = ['pacifica-perp', 'backpack-perp', 'bitfinex-derivatives'];

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'Cache-Control': 'no-cache',
      'User-Agent': 'data-fetcher-king/wave-alpha-public-health',
    },
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const body = await response.text();
  let payload;
  try { payload = JSON.parse(body); }
  catch { throw new Error(`non-json ${response.status} from ${new URL(url).hostname}`); }
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
  return { status: response.status, payload };
}

const nonce = Date.now();
const health = await fetchJson(`${COORDINATOR}/health?audit=${nonce}`);
const summary = await fetchJson(`${PAGES}/api/liquidations/summary?window=1h&top=1&audit=${nonce}`);

const report = {
  checkedAt: new Date().toISOString(),
  publicReadOnly: true,
  coordinator: {
    status: health.status,
    ok: health.payload?.ok === true,
    trafficEnabled: health.payload?.trafficEnabled === true,
    ingestReady: health.payload?.ingestReady === true,
    adminReady: health.payload?.adminReady === true,
  },
  pagesSummary: {
    status: summary.status,
    storage: summary.payload?.storage ?? null,
    freshnessMs: summary.payload?.freshnessMs ?? null,
    error: summary.payload?.error ?? null,
  },
  phaseB: {},
};

for (const exchange of PHASE_B) {
  const result = await fetchJson(`${PAGES}/api/liquidations/history?range=1d&symbol=ALL&exchange=${encodeURIComponent(exchange)}&audit=${nonce}`);
  const payload = result.payload || {};
  const diagnostics = payload.diagnostics || {};
  report.phaseB[exchange] = {
    status: result.status,
    selectedExchange: payload.selectedExchange ?? null,
    storage: payload.storage ?? null,
    realtimeStorage: diagnostics.realtimeStorage ?? null,
    usedCoordinator: diagnostics.usedCoordinator === true,
    error: payload.error ?? null,
  };
}

const coordinatorGate = report.coordinator.ok && report.coordinator.trafficEnabled && report.coordinator.ingestReady;
const summaryGate = report.pagesSummary.status === 200 && report.pagesSummary.storage === 'durable-object' && report.pagesSummary.error == null;
const phaseBGate = PHASE_B.every(exchange => {
  const row = report.phaseB[exchange];
  return row.status === 200
    && row.selectedExchange === exchange
    && row.storage === 'durable-object'
    && row.realtimeStorage === 'durable-object'
    && row.usedCoordinator
    && row.error == null;
});

report.gates = {
  coordinator: coordinatorGate ? 'PASS' : 'FAIL',
  pagesSummary: summaryGate ? 'PASS' : 'FAIL',
  phaseBSources: phaseBGate ? 'PASS' : 'FAIL',
  publicRuntime: coordinatorGate && summaryGate && phaseBGate ? 'PASS' : 'FAIL',
};

console.log(`WAVE_ALPHA_PUBLIC_HEALTH=${JSON.stringify(report)}`);
if (report.gates.publicRuntime !== 'PASS') process.exit(1);
