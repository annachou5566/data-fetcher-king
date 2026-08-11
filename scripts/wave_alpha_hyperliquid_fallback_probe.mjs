const TIMEOUT_MS = 12_000;

const CANDIDATES = Object.freeze([
  'https://api-hyperliquid.asxn.xyz/api/node/liquidations/summary',
  'https://api-hyperliquid.asxn.xyz/api/node/liquidations',
]);

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function topLevelKeys(value) {
  if (Array.isArray(value)) return ['<array>'];
  if (!isRecord(value)) return [];
  return Object.keys(value).slice(0, 80).sort();
}

function collectSchemaSignals(value, prefix = '', depth = 0, out = { numeric: [], timestamps: [] }) {
  if (depth > 4 || value == null) return out;

  if (Array.isArray(value)) {
    for (const item of value.slice(0, 5)) collectSchemaSignals(item, `${prefix}[]`, depth + 1, out);
    return out;
  }
  if (!isRecord(value)) return out;

  for (const [key, child] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key;
    const keyLower = key.toLowerCase();

    if (
      typeof child === 'number'
      && Number.isFinite(child)
      && /(liq|liquid|long|short|total|volume|notional|ntl|usd|value|amount)/i.test(key)
      && out.numeric.length < 80
    ) {
      out.numeric.push({ path, value: child });
    }

    if (
      (typeof child === 'number' || typeof child === 'string')
      && /(time|timestamp|updated|update|date|observed|created)/i.test(keyLower)
      && out.timestamps.length < 40
    ) {
      const rendered = String(child);
      out.timestamps.push({ path, value: rendered.slice(0, 64) });
    }

    if (depth < 4 && (Array.isArray(child) || isRecord(child))) {
      collectSchemaSignals(child, path, depth + 1, out);
    }
  }

  return out;
}

async function fetchCandidate(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  const startedAt = Date.now();

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        'User-Agent': 'WaveAlpha-Hyperliquid-Fallback-Qualification/1.0',
      },
      signal: controller.signal,
    });

    const contentType = response.headers.get('content-type') || '';
    const text = await response.text();
    let parsed = null;
    let json = false;
    try {
      parsed = text ? JSON.parse(text) : null;
      json = true;
    } catch {
      // Deliberately do not print raw third-party bodies.
    }

    const signals = json ? collectSchemaSignals(parsed) : { numeric: [], timestamps: [] };
    const liquidationSignals = signals.numeric.filter(({ path }) => /liq|liquid|long|short|notional|ntl/i.test(path));

    return {
      endpoint: url,
      httpStatus: response.status,
      ok: response.ok,
      contentType,
      elapsedMs: Date.now() - startedAt,
      json,
      topLevelType: Array.isArray(parsed) ? 'array' : parsed === null ? 'null' : typeof parsed,
      topLevelKeys: topLevelKeys(parsed),
      liquidationNumericSignals: liquidationSignals.slice(0, 30),
      timestampSignals: signals.timestamps.slice(0, 20),
      parserCandidate: response.ok && json && liquidationSignals.length > 0,
      rawPayloadPrinted: false,
      walletValuesPrinted: false,
    };
  } catch (error) {
    return {
      endpoint: url,
      httpStatus: null,
      ok: false,
      elapsedMs: Date.now() - startedAt,
      json: false,
      error: error?.name === 'AbortError' ? 'timeout' : String(error?.message || error).slice(0, 200),
      parserCandidate: false,
      rawPayloadPrinted: false,
      walletValuesPrinted: false,
    };
  } finally {
    clearTimeout(timer);
  }
}

const results = [];
for (const url of CANDIDATES) {
  const result = await fetchCandidate(url);
  results.push(result);
  if (result.parserCandidate) break;
}

const selected = results.find(item => item.parserCandidate) || null;
const output = {
  checkedAt: new Date().toISOString(),
  provider: 'ASXN HyperScreener',
  qualificationClass: 'third-party-aggregate-research-only',
  sourceListedByOfficialHyperliquidDocs: true,
  expectedCadence: 'daily-benchmark',
  credentialsUsed: false,
  rawPayloadPrinted: false,
  walletValuesPrinted: false,
  results,
  selectedEndpoint: selected?.endpoint || null,
  parserCandidate: Boolean(selected),
  productionEligible: false,
  aggregateEligible: false,
  nextGate: selected
    ? 'review observed schema and benchmark totals against independent Hyperliquid liquidation references before writing a parser'
    : 'keep ASXN research-only; do not infer or activate third-party totals',
};

console.log(JSON.stringify(output, null, 2));
console.log('hyperliquidFallbackProbe=COMPLETE');
console.log(`asxnParserCandidate=${output.parserCandidate}`);

// Endpoint availability/schema discovery is research evidence, not a production
// health gate. Fail-open for the public qualification runner while keeping
// production eligibility fail-closed above.
process.exitCode = 0;
