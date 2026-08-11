#!/usr/bin/env node

import { readFile } from 'node:fs/promises';

const token = process.env.GITHUB_TOKEN;
const repo = process.env.GITHUB_REPOSITORY;
const issueNumber = Number(process.env.WAVE_PHASE_C_ISSUE || '5');
if (!token || !repo || !Number.isInteger(issueNumber) || issueNumber <= 0) {
  throw new Error('missing GitHub issue posting context');
}

const [owner, name] = repo.split('/');
if (!owner || !name) throw new Error('invalid GITHUB_REPOSITORY');

const report = JSON.parse(await readFile('/tmp/wave-alpha-phase-c-evidence.json', 'utf8'));
const evidence = Array.isArray(report?.evidence) ? report.evidence : [];
if (!evidence.length) {
  console.log('phaseCEvidencePost=no-evidence');
  process.exit(0);
}

async function api(path, options = {}) {
  const response = await fetch(`https://api.github.com${path}`, {
    ...options,
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'data-fetcher-king/wave-alpha-phase-c-observer',
      ...(options.headers || {}),
    },
    signal: AbortSignal.timeout(15_000),
  });
  const text = await response.text();
  let payload = null;
  if (text) {
    try { payload = JSON.parse(text); }
    catch { payload = text; }
  }
  if (!response.ok) throw new Error(`GitHub API ${response.status}: ${String(text).slice(0, 200)}`);
  return payload;
}

const seen = new Set();
for (let page = 1; page <= 10; page++) {
  const comments = await api(`/repos/${owner}/${name}/issues/${issueNumber}/comments?per_page=100&page=${page}`);
  if (!Array.isArray(comments) || !comments.length) break;
  for (const comment of comments) {
    const body = String(comment?.body || '');
    for (const match of body.matchAll(/wave-phase-c-fp:([^\s`]+)/g)) seen.add(match[1]);
  }
  if (comments.length < 100) break;
}

let posted = 0;
for (const item of evidence) {
  const fp = String(item?.fingerprint || '').trim();
  if (!fp || seen.has(fp)) continue;
  const lines = [
    `### New first-party Phase C evidence`,
    '',
    `- \`wave-phase-c-fp:${fp}\``,
    `- observed by scheduled public observer: ${report.checkedAt || 'unknown'}`,
    `- exchange: \`${item.exchange || 'unknown'}\``,
    `- source: \`${item.source || 'unknown'}\``,
  ];
  if (item.symbol) lines.push(`- symbol: \`${item.symbol}\``);
  if (item.instrument) lines.push(`- instrument: \`${item.instrument}\``);
  if (item.tradeId) lines.push(`- trade id: \`${item.tradeId}\``);
  if (item.tradeSeq != null) lines.push(`- trade seq: \`${item.tradeSeq}\``);
  if (item.time) lines.push(`- time: \`${item.time}\``);
  if (item.timestamp != null) lines.push(`- timestamp: \`${item.timestamp}\``);
  if (item.liquidationMarker) lines.push(`- liquidation marker: \`${item.liquidationMarker}\``);
  if (item.takerSide) lines.push(`- taker side: \`${item.takerSide}\``);
  if (item.takerDirection) lines.push(`- taker direction: \`${item.takerDirection}\``);
  if (item.price != null) lines.push(`- price: \`${item.price}\``);
  if (item.amount != null) lines.push(`- amount: \`${item.amount}\``);
  if (item.size != null) lines.push(`- size: \`${item.size}\``);
  if (item.contracts != null) lines.push(`- contracts: \`${item.contracts}\``);
  if (item.instrumentType) lines.push(`- instrument type: \`${item.instrumentType}\``);
  if (item.quoteCurrency) lines.push(`- quote currency: \`${item.quoteCurrency}\``);
  if (item.authoritativeUsdNotional != null) lines.push(`- authoritative USD candidate: \`${item.authoritativeUsdNotional}\``);
  if (item.notionalRule) lines.push(`- notional rule: ${item.notionalRule}`);
  lines.push(`- Wave side candidate: ${item.waveSide ? `\`${item.waveSide}\`` : '**NOT NORMALIZED**'}`);
  if (item.sideReason) lines.push(`- side basis: ${item.sideReason}`);
  lines.push('', 'Evidence only. No Wave Alpha runtime/deployment authorization is implied.');

  await api(`/repos/${owner}/${name}/issues/${issueNumber}/comments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ body: lines.join('\n') }),
  });
  seen.add(fp);
  posted += 1;
}

console.log(`phaseCEvidencePost=PASS posted=${posted} total=${evidence.length}`);
