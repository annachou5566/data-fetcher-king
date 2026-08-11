#!/usr/bin/env node

const PACIFICA_HISTORY = 'https://api.pacifica.fi/api/v1/trades/history';
const PACIFICA_START_MS = 1786405746425;
const PACIFICA_ANCHOR_HISTORY_ID = 249630609;
const PACIFICA_LIMIT_CANDIDATES = [10000, 5000, 2000, 1000];
const PACIFICA_MAX_PAGES = 500;
const PACIFICA_LIQ_CAUSES = new Set([
  'market_liquidation',
  'backstop_liquidation',
  'insolvency_liquidation',
]);

const BACKPACK_MARKETS = 'https://api.backpack.exchange/api/v1/markets';
const BACKPACK_OPEN_INTEREST = 'https://api.backpack.exchange/api/v1/openInterest';
const BACKPACK_WS = 'wss://ws.backpack.exchange';
const BACKPACK_OBSERVE_MS = 180_000;
const USD_QUOTES = new Set(['USD', 'USDT', 'USDC']);
const HTTP_TIMEOUT_MS = 20_000;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const text = value => String(value ?? '').trim();

function parseRateHeader(value) {
  const result = {};
  for (const match of text(value).matchAll(/(?:^|;)([rqtw])=([0-9]+(?:\.[0-9]+)?)/g)) {
    result[match[1]] = Number(match[2]);
  }
  return result;
}

async function fetchJson(url, { userAgent, allow400 = false, max429Retries = 8 } = {}) {
  let attempt = 0;
  while (true) {
    let response;
    try {
      response = await fetch(url, {
        headers: {
          Accept: 'application/json',
          'Cache-Control': 'no-cache',
          'User-Agent': userAgent || 'data-fetcher-king/phase-b-source-audit',
        },
        signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
      });
    } catch (error) {
      throw new Error(`network:${new URL(url).hostname}:${String(error?.message || error)}`);
    }

    const bodyText = await response.text();
    const headers = {
      ratelimit: response.headers.get('ratelimit') || '',
      ratelimitPolicy: response.headers.get('ratelimit-policy') || '',
      retryAfter: response.headers.get('retry-after') || '',
    };

    if (response.status === 429 && attempt < max429Retries) {
      const rate = parseRateHeader(headers.ratelimit);
      const retryHeader = Number(headers.retryAfter);
      const waitSeconds = Number.isFinite(retryHeader) && retryHeader > 0
        ? Math.min(90, retryHeader)
        : Number.isFinite(rate.t) && rate.t > 0
          ? Math.min(90, rate.t + 0.75)
          : Math.min(90, 5 * (2 ** attempt));
      attempt += 1;
      console.log(`pacifica429Retry=${attempt}/${max429Retries} waitSeconds=${waitSeconds.toFixed(2)} ratelimit=${JSON.stringify(headers.ratelimit)}`);
      await sleep(waitSeconds * 1000);
      continue;
    }

    let payload = null;
    if (bodyText) {
      try { payload = JSON.parse(bodyText); }
      catch { throw new Error(`non-json:${response.status}:${new URL(url).hostname}`); }
    }

    if (allow400 && response.status === 400) return { response, payload, headers };
    if (!response.ok) throw new Error(`HTTP ${response.status}:${new URL(url).hostname}`);
    return { response, payload, headers };
  }
}

function pacificaHistoryId(row) {
  const value = row?.h ?? row?.history_id;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function pacificaTimestamp(row) {
  const value = row?.t ?? row?.created_at;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : null;
}

function pacificaCause(row) {
  return text(row?.tc ?? row?.cause).toLowerCase();
}

async function requestPacificaPage({ endMs, cursor, limit, allow400 = false }) {
  const params = new URLSearchParams({
    start_time: String(PACIFICA_START_MS),
    end_time: String(endMs),
    limit: String(limit),
  });
  if (cursor) params.set('cursor', cursor);
  return fetchJson(`${PACIFICA_HISTORY}?${params}`, {
    userAgent: 'data-fetcher-king/pacifica-retrospective-audit-v3',
    allow400,
  });
}

async function scanPacifica() {
  const endMs = Date.now();
  const causes = new Map();
  const liquidationRows = new Map();
  const seenIds = new Set();
  const seenCursors = new Set();
  let anchorFound = false;
  let selectedLimit = null;
  let firstPage = null;
  let firstHeaders = null;

  console.log(`PACIFICA_AUDIT_START=${JSON.stringify({
    readOnly: true,
    startMs: PACIFICA_START_MS,
    endMs,
    anchorHistoryId: PACIFICA_ANCHOR_HISTORY_ID,
  })}`);

  for (const candidate of PACIFICA_LIMIT_CANDIDATES) {
    const result = await requestPacificaPage({ endMs, cursor: null, limit: candidate, allow400: true });
    if (result.response.status === 400) {
      console.log(`pacificaPageLimitRejected=${candidate}`);
      continue;
    }
    if (result.payload?.success !== true || !Array.isArray(result.payload?.data)) {
      throw new Error(`Pacifica first-page contract failed for limit=${candidate}`);
    }
    selectedLimit = candidate;
    firstPage = result.payload;
    firstHeaders = result.headers;
    console.log(`pacificaPageLimitSelected=${candidate} firstRows=${result.payload.data.length} hasMore=${result.payload.has_more === true}`);
    break;
  }

  if (!selectedLimit || !firstPage) throw new Error('Pacifica rejected all bounded page sizes');

  let payload = firstPage;
  let headers = firstHeaders;
  let cursor = null;
  let pages = 0;
  let totalRows = 0;

  while (true) {
    pages += 1;
    const rows = Array.isArray(payload.data) ? payload.data : [];
    totalRows += rows.length;

    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      const cause = pacificaCause(row);
      if (cause) causes.set(cause, (causes.get(cause) || 0) + 1);
      const historyId = pacificaHistoryId(row);
      const timestampMs = pacificaTimestamp(row);
      if (historyId == null || timestampMs == null) continue;
      if (seenIds.has(historyId)) continue;
      seenIds.add(historyId);
      if (historyId === PACIFICA_ANCHOR_HISTORY_ID) anchorFound = true;
      if (PACIFICA_LIQ_CAUSES.has(cause)) {
        liquidationRows.set(historyId, {
          historyId,
          timestampMs,
          cause,
          symbol: text(row?.s ?? row?.symbol),
          direction: text(row?.d ?? row?.side),
          price: text(row?.p ?? row?.price),
          amount: text(row?.a ?? row?.amount),
        });
      }
    }

    if (pages === 1 || pages % 5 === 0) {
      console.log(`pacificaProgress pages=${pages} rows=${totalRows} uniqueHistoryIds=${seenIds.size} anchorFound=${anchorFound} officialLiquidations=${liquidationRows.size} ratelimit=${JSON.stringify(headers?.ratelimit || '')}`);
    }

    if (payload.has_more !== true) break;
    if (pages >= PACIFICA_MAX_PAGES) throw new Error(`Pacifica pagination cap ${PACIFICA_MAX_PAGES}`);
    const nextCursor = text(payload.next_cursor);
    if (!nextCursor) throw new Error('Pacifica has_more=true without next_cursor');
    if (seenCursors.has(nextCursor)) throw new Error('Pacifica cursor loop');
    seenCursors.add(nextCursor);
    cursor = nextCursor;

    const rate = parseRateHeader(headers?.ratelimit || '');
    if (Number.isFinite(rate.r) && Number.isFinite(rate.t) && rate.r <= 180 && rate.t > 0) {
      const waitMs = Math.min(90_000, (rate.t + 0.75) * 1000);
      console.log(`pacificaQuotaWait remainingUnits=${rate.r} waitMs=${Math.round(waitMs)}`);
      await sleep(waitMs);
    } else if (!headers?.ratelimit) {
      await sleep(6000);
    }

    const result = await requestPacificaPage({ endMs, cursor, limit: selectedLimit });
    payload = result.payload;
    headers = result.headers;
    if (payload?.success !== true || !Array.isArray(payload?.data)) {
      throw new Error('Pacifica page contract failed');
    }
  }

  const causeCounts = Object.fromEntries([...causes.entries()].sort(([a], [b]) => a.localeCompare(b)));
  const liquidations = [...liquidationRows.values()].sort((a, b) => a.timestampMs - b.timestampMs || a.historyId - b.historyId);
  const result = {
    checkedAt: new Date().toISOString(),
    readOnly: true,
    startMs: PACIFICA_START_MS,
    endMs,
    pageLimit: selectedLimit,
    pages,
    rows: totalRows,
    uniqueHistoryIds: seenIds.size,
    anchorHistoryId: PACIFICA_ANCHOR_HISTORY_ID,
    anchorFound,
    causeCounts,
    officialLiquidationRows: liquidations.length,
    officialLiquidations: liquidations,
    retrospectiveSourceGate: anchorFound ? 'PASS_SOURCE_HISTORY_COMPLETE' : 'INCONCLUSIVE_ANCHOR_NOT_FOUND',
  };
  console.log(`PACIFICA_AUDIT_RESULT=${JSON.stringify(result)}`);
  return result;
}

function backpackMarketMap(payload) {
  if (!Array.isArray(payload)) return new Map();
  const markets = new Map();
  for (const row of payload) {
    if (!row || typeof row !== 'object') continue;
    const sourceSymbol = text(row.symbol);
    const canonical = sourceSymbol.toUpperCase();
    const quote = text(row.quoteSymbol).toUpperCase();
    const base = text(row.baseSymbol).toUpperCase();
    const marketType = text(row.marketType).toUpperCase();
    if (!sourceSymbol || !base || !USD_QUOTES.has(quote) || marketType !== 'PERP' || row.visible === false) continue;
    markets.set(canonical, { ...row, sourceSymbol });
  }
  return markets;
}

function backpackFingerprint(raw) {
  return `backpack:${text(raw?.s).toUpperCase()}:${text(raw?.E)}:${text(raw?.T)}:${text(raw?.S).toLowerCase()}:${text(raw?.p)}:${text(raw?.q)}`;
}

async function scanBackpack() {
  if (typeof WebSocket !== 'function') throw new Error('Node global WebSocket unavailable');
  const [marketsResult, oiResult] = await Promise.all([
    fetchJson(BACKPACK_MARKETS, { userAgent: 'data-fetcher-king/backpack-independent-audit' }),
    fetchJson(BACKPACK_OPEN_INTEREST, { userAgent: 'data-fetcher-king/backpack-independent-audit' }),
  ]);
  const markets = backpackMarketMap(marketsResult.payload);
  if (!markets.size) throw new Error('Backpack metadata returned zero eligible PERP markets');

  const oiRows = Array.isArray(oiResult.payload) ? oiResult.payload : [];
  const restOiSymbols = new Set(oiRows.map(row => text(row?.symbol).toUpperCase()).filter(Boolean));
  const expectedOi = new Set([...markets.keys()].filter(symbol => restOiSymbols.has(symbol)));
  const seenOi = new Set();
  const liquidations = new Map();
  const errors = [];
  let oiMessages = 0;
  let liquidationMessages = 0;
  let opened = false;
  let subscriptionSent = false;
  let finished = false;
  let ws;

  const liquidationStreams = [...markets.values()].map(meta => `liquidation.${meta.sourceSymbol}`);
  const oiStreams = [...markets.values()].map(meta => `openInterest.${meta.sourceSymbol}`);

  console.log(`BACKPACK_AUDIT_START=${JSON.stringify({
    readOnly: true,
    metadataMarkets: markets.size,
    restOpenInterestRows: restOiSymbols.size,
    expectedOiControls: expectedOi.size,
    liquidationStreams: liquidationStreams.length,
    openInterestStreams: oiStreams.length,
    observeSeconds: BACKPACK_OBSERVE_MS / 1000,
  })}`);

  return await new Promise(resolve => {
    const finish = () => {
      if (finished) return;
      finished = true;
      try { ws?.close(); } catch {}
      const missingOi = [...expectedOi].filter(symbol => !seenOi.has(symbol)).sort();
      const result = {
        checkedAt: new Date().toISOString(),
        readOnly: true,
        metadataMarkets: markets.size,
        restOpenInterestRows: restOiSymbols.size,
        expectedOiControls: expectedOi.size,
        seenOiControls: [...seenOi].filter(symbol => expectedOi.has(symbol)).length,
        missingOiControls: missingOi,
        oiMessages,
        liquidationMessages,
        liquidationEvents: [...liquidations.values()].sort((a, b) => Number(a.eventUs) - Number(b.eventUs)),
        websocketOpened: opened,
        subscriptionSent,
        errors,
        livenessGate: opened && subscriptionSent && expectedOi.size > 0 && missingOi.length === 0 && errors.length === 0 ? 'PASS' : 'FAIL',
        liquidationEvidenceGate: liquidationMessages > 0 ? 'PASS_REAL_EVENT_OBSERVED' : 'PENDING_NO_LIQUIDATION_DURING_SAMPLE',
      };
      console.log(`BACKPACK_AUDIT_RESULT=${JSON.stringify(result)}`);
      resolve(result);
    };

    const timer = setTimeout(finish, BACKPACK_OBSERVE_MS);
    ws = new WebSocket(BACKPACK_WS);
    ws.addEventListener('open', () => {
      opened = true;
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: liquidationStreams }));
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: oiStreams }));
      subscriptionSent = true;
    });

    ws.addEventListener('message', event => {
      let message;
      try { message = JSON.parse(String(event.data)); }
      catch { return; }
      if (!message || typeof message !== 'object') return;
      if (text(message.event).toLowerCase() === 'error' || message.error != null) {
        errors.push(`server:${JSON.stringify(message).slice(0, 300)}`);
        return;
      }
      const stream = text(message.stream);
      const data = message.data;
      if (!data || typeof data !== 'object') return;

      if (stream.startsWith('openInterest.')) {
        oiMessages += 1;
        const symbol = text(data.s || stream.slice('openInterest.'.length)).toUpperCase();
        if (markets.has(symbol)) seenOi.add(symbol);
        if (oiMessages === 1 || oiMessages % 25 === 0) {
          console.log(`backpackOiProgress expectedSeen=${[...seenOi].filter(s => expectedOi.has(s)).length}/${expectedOi.size} messages=${oiMessages}`);
        }
        return;
      }

      if (stream.startsWith('liquidation.')) {
        liquidationMessages += 1;
        const symbol = text(data.s || stream.slice('liquidation.'.length)).toUpperCase();
        const side = text(data.S).toLowerCase();
        const eventUs = text(data.E);
        const engineUs = text(data.T);
        const price = Number(data.p);
        const qty = Number(data.q);
        if (!markets.has(symbol) || text(data.e) !== 'liquidation' || !['bid', 'ask'].includes(side)
            || !Number.isFinite(price) || price <= 0 || !Number.isFinite(qty) || qty <= 0 || !eventUs) {
          errors.push(`invalid-liquidation-shape:${symbol}`);
          return;
        }
        const fp = backpackFingerprint(data);
        liquidations.set(fp, {
          fingerprint: fp,
          symbol,
          eventUs,
          engineUs,
          sourceSide: side,
          waveSide: side === 'ask' ? 'long' : 'short',
          price,
          qty,
          usd: Math.round(price * qty * 100) / 100,
        });
        console.log(`backpackLiquidationObserved=${JSON.stringify(liquidations.get(fp))}`);
      }
    });

    ws.addEventListener('error', () => errors.push('websocket-error'));
    ws.addEventListener('close', event => {
      if (!finished && event.code !== 1000) {
        errors.push(`early-close:${event.code}`);
        clearTimeout(timer);
        finish();
      }
    });
  });
}

const startedAt = Date.now();
const [pacificaSettled, backpackSettled] = await Promise.allSettled([
  scanPacifica(),
  scanBackpack(),
]);

const report = {
  checkedAt: new Date().toISOString(),
  durationMs: Date.now() - startedAt,
  readOnly: true,
  credentialsUsed: false,
  productionMutation: false,
  pacifica: pacificaSettled.status === 'fulfilled'
    ? pacificaSettled.value
    : { gate: 'INCONCLUSIVE', error: String(pacificaSettled.reason?.message || pacificaSettled.reason) },
  backpack: backpackSettled.status === 'fulfilled'
    ? backpackSettled.value
    : { livenessGate: 'INCONCLUSIVE', error: String(backpackSettled.reason?.message || backpackSettled.reason) },
};

console.log(`PHASE_B_PUBLIC_SOURCE_AUDIT=${JSON.stringify(report)}`);
if (pacificaSettled.status === 'rejected' || backpackSettled.status === 'rejected') process.exitCode = 1;
