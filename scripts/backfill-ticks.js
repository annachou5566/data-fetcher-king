#!/usr/bin/env node
// ============================================================
// scripts/backfill-ticks.js
// Backfill tick history từ Binance Vision aggTrades vào R2
//
// Usage:
//   node backfill-ticks.js --from 2026-06-20 --to 2026-06-27
//   node backfill-ticks.js --from 2026-06-20 --to 2026-06-27 --symbols BTCUSDT,ETHUSDT
//   node backfill-ticks.js --from 2026-06-20 --to 2026-06-27 --force   (overwrite dù đã có)
//
// Env vars (giống index.js):
//   R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
//
// Binance Vision URL format:
//   https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-06-24.zip
// CSV columns: agg_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker
// ============================================================

require('dotenv').config();
const { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const https  = require('https');
const http   = require('http');
const zlib   = require('zlib');
const { promisify } = require('util');
const path   = require('path');
const fs     = require('fs');
const os     = require('os');
const AdmZip = require('adm-zip');

const gzipAsync   = promisify(zlib.gzip);
const gunzipAsync = promisify(zlib.gunzip);

// ── Config ──────────────────────────────────────────────────
const ALL_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'];
const BINANCE_VISION_BASE = 'https://data.binance.vision/data/spot/daily/aggTrades';

const s3 = new S3Client({
    region   : 'auto',
    endpoint : process.env.R2_ENDPOINT_URL,
    credentials: {
        accessKeyId    : process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
});
const BUCKET = process.env.R2_BUCKET_NAME;

// ── CLI args ─────────────────────────────────────────────────
function parseArgs() {
    const args = process.argv.slice(2);
    const get  = (flag) => {
        const i = args.indexOf(flag);
        return i !== -1 ? args[i + 1] : null;
    };
    const fromArg    = get('--from');
    const toArg      = get('--to');
    const symArg     = get('--symbols');
    const forceFlag  = args.includes('--force');
    const dryRun     = args.includes('--dry-run');

    if (!fromArg) { console.error('❌ --from YYYY-MM-DD required'); process.exit(1); }

    const fromDate = new Date(fromArg + 'T00:00:00Z');
    const toDate   = toArg ? new Date(toArg + 'T00:00:00Z') : new Date(Date.now() - 86400_000);

    if (isNaN(fromDate) || isNaN(toDate) || fromDate > toDate) {
        console.error('❌ Invalid date range'); process.exit(1);
    }

    const symbols = symArg ? symArg.split(',').map(s => s.trim().toUpperCase()) : ALL_SYMBOLS;
    const invalid = symbols.filter(s => !ALL_SYMBOLS.includes(s));
    if (invalid.length) { console.error(`❌ Unknown symbols: ${invalid.join(', ')}`); process.exit(1); }

    return { fromDate, toDate, symbols, force: forceFlag, dryRun };
}

// ── Build list of dates ───────────────────────────────────────
function buildDateList(from, to) {
    const dates = [];
    let cur = new Date(from);
    while (cur <= to) {
        dates.push(cur.toISOString().slice(0, 10)); // "2026-06-24"
        cur = new Date(cur.getTime() + 86400_000);
    }
    return dates;
}

// ── Check which R2 hours already exist for a symbol+date ─────
async function getExistingHours(symbol, dateStr) {
    const prefix = `tick-cache/${symbol}/${dateStr}`; // e.g. "tick-cache/BTCUSDT/2026-06-24"
    const existing = new Set();
    try {
        let continuation;
        do {
            const res = await s3.send(new ListObjectsV2Command({
                Bucket: BUCKET, Prefix: prefix, ContinuationToken: continuation,
            }));
            for (const obj of (res.Contents || [])) {
                const m = obj.Key.match(/(\d{4}-\d{2}-\d{2}T\d{2})\.json\.gz$/);
                if (m) existing.add(m[1]); // "2026-06-24T14"
            }
            continuation = res.IsTruncated ? res.NextContinuationToken : null;
        } while (continuation);
    } catch (_) {}
    return existing;
}

// ── Download file (follows redirects) ────────────────────────
function downloadToBuffer(url) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        const proto  = url.startsWith('https') ? https : http;
        const req = proto.get(url, { timeout: 120_000 }, (res) => {
            if (res.statusCode === 301 || res.statusCode === 302) {
                return downloadToBuffer(res.headers.location).then(resolve, reject);
            }
            if (res.statusCode === 404) return reject(new Error(`404: ${url}`));
            if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}: ${url}`));
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve(Buffer.concat(chunks)));
            res.on('error', reject);
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    });
}

// ── Parse CSV từ Buffer (stream line by line, không load toàn bộ vào string) ──
// CSV columns: agg_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker
function parseCsvToHourBuckets(csvBuffer) {
    // byHour: { "2026-06-24T14": [[ts,price,qty,isSell], ...] }
    const byHour = {};
    let pos = 0;
    const len = csvBuffer.length;

    // Skip BOM nếu có
    if (csvBuffer[0] === 0xEF && csvBuffer[1] === 0xBB && csvBuffer[2] === 0xBF) pos = 3;

    let lineStart = pos;
    let lineCount = 0;

    while (pos <= len) {
        const isEnd = (pos === len);
        const ch    = isEnd ? 10 : csvBuffer[pos];

        if (ch === 10 || ch === 13) { // newline
            const lineLen = pos - lineStart;
            if (lineLen > 0) {
                const line = csvBuffer.toString('ascii', lineStart, pos);
                lineCount++;

                // Skip header row (starts with non-digit)
                if (lineCount > 1 || !/^\d/.test(line)) {
                    if (/^\d/.test(line)) {
                        // Fast manual parse — avoid split() overhead on 2M+ lines
                        // Format: agg_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker
                        let c1 = line.indexOf(',');                // after agg_id
                        let c2 = line.indexOf(',', c1 + 1);        // after price
                        let c3 = line.indexOf(',', c2 + 1);        // after qty
                        let c4 = line.indexOf(',', c3 + 1);        // after first_trade_id
                        let c5 = line.indexOf(',', c4 + 1);        // after last_trade_id
                        let c6 = line.indexOf(',', c5 + 1);        // after transact_time

                        if (c6 > 0) {
                            const tsMs       = parseInt(line.slice(c5 + 1, c6), 10);
                            const price      = line.slice(c1 + 1, c2);   // keep as string
                            const qty        = line.slice(c2 + 1, c3);   // keep as string
                            const isBuyerMaker = line.charCodeAt(c6 + 1) === 84; // 'T' = true
                            const isSell     = isBuyerMaker ? 1 : 0;

                            if (!isNaN(tsMs) && tsMs > 0) {
                                const hourTag = new Date(Math.floor(tsMs / 3_600_000) * 3_600_000)
                                    .toISOString().slice(0, 13);
                                if (!byHour[hourTag]) byHour[hourTag] = [];
                                byHour[hourTag].push([tsMs, price, qty, isSell]);
                            }
                        }
                    }
                }
            }
            // Skip \r\n pair
            if (ch === 13 && pos + 1 < len && csvBuffer[pos + 1] === 10) pos++;
            lineStart = pos + 1;
        }
        pos++;
        if (isEnd) break;
    }

    return byHour;
}

// ── R2: GET existing ticks cho 1 giờ ─────────────────────────
async function getExistingTicks(key) {
    try {
        const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
        const buf = Buffer.from(await obj.Body.transformToByteArray());
        return JSON.parse((await gunzipAsync(buf)).toString('utf8'));
    } catch (_) {
        return []; // file chưa tồn tại
    }
}

// ── R2: PUT merged ticks cho 1 giờ ───────────────────────────
async function putHourTicks(key, ticks) {
    const compressed = await gzipAsync(Buffer.from(JSON.stringify(ticks)));
    await s3.send(new PutObjectCommand({
        Bucket: BUCKET, Key: key, Body: compressed, ContentType: 'application/gzip',
    }));
    return compressed.length;
}

// ── Main ──────────────────────────────────────────────────────
async function main() {
    const { fromDate, toDate, symbols, force, dryRun } = parseArgs();
    const dates = buildDateList(fromDate, toDate);

    console.log(`\n🚀 Backfill tick history`);
    console.log(`   Symbols : ${symbols.join(', ')}`);
    console.log(`   Dates   : ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} ngày)`);
    console.log(`   Mode    : ${dryRun ? 'DRY RUN' : force ? 'FORCE (overwrite)' : 'SMART (skip existing)'}`);
    console.log(`   Bucket  : ${BUCKET}\n`);

    let totalUploaded = 0, totalSkipped = 0, totalFailed = 0;

    for (const symbol of symbols) {
        console.log(`\n── ${symbol} ─────────────────────────────`);

        for (const dateStr of dates) {
            const url = `${BINANCE_VISION_BASE}/${symbol}/${symbol}-aggTrades-${dateStr}.zip`;

            // Kiểm tra R2 đã có đủ 24 hours chưa (chỉ khi không force)
            let existingHours = new Set();
            if (!force) {
                existingHours = await getExistingHours(symbol, dateStr);
                if (existingHours.size >= 24) {
                    console.log(`  ${dateStr} ⏭️  Skip (24/24 hours đã có trong R2)`);
                    totalSkipped += 24;
                    continue;
                }
            }

            // Download ZIP từ Binance Vision
            let zipBuf;
            try {
                process.stdout.write(`  ${dateStr} ⬇️  Downloading...`);
                zipBuf = await downloadToBuffer(url);
                process.stdout.write(` ${(zipBuf.length / 1024 / 1024).toFixed(1)} MB\n`);
            } catch (e) {
                if (e.message.startsWith('404')) {
                    console.log(`  ${dateStr} ⚠️  Skip (file chưa có trên Binance Vision — ngày tương lai?)`);
                } else {
                    console.error(`  ${dateStr} ❌ Download lỗi: ${e.message}`);
                    totalFailed++;
                }
                continue;
            }

            // Extract CSV từ ZIP
            let csvBuffer;
            try {
                const zip = new AdmZip(zipBuf);
                const entry = zip.getEntries().find(e => e.entryName.endsWith('.csv'));
                if (!entry) throw new Error('Không tìm thấy CSV trong ZIP');
                csvBuffer = entry.getData(); // Buffer
                console.log(`  ${dateStr} 📦 Extracted CSV: ${(csvBuffer.length / 1024 / 1024).toFixed(1)} MB`);
            } catch (e) {
                console.error(`  ${dateStr} ❌ Extract lỗi: ${e.message}`);
                totalFailed++;
                continue;
            }

            // Parse CSV → group by hour
            const byHour = parseCsvToHourBuckets(csvBuffer);
            csvBuffer = null; // GC hint
            const hours = Object.keys(byHour).sort();
            console.log(`  ${dateStr} 🔢 ${hours.length} giờ, ${Object.values(byHour).reduce((s,a) => s+a.length, 0).toLocaleString()} ticks`);

            if (dryRun) {
                for (const h of hours) console.log(`    [DRY] Would upload: tick-cache/${symbol}/${h}.json.gz (${byHour[h].length} ticks)`);
                totalSkipped += hours.length;
                continue;
            }

            // Upload từng giờ lên R2
            for (const hourTag of hours) {
                const key = `tick-cache/${symbol}/${hourTag}.json.gz`;

                // Skip nếu đã có (smart mode)
                if (!force && existingHours.has(hourTag)) {
                    totalSkipped++;
                    continue;
                }

                try {
                    // GET file cũ → merge → PUT (giống flushTickToR2)
                    const existing = await getExistingTicks(key);
                    const newTicks = byHour[hourTag];

                    let merged;
                    if (existing.length === 0) {
                        // Chỉ cần sort (CSV đã sorted theo ts)
                        merged = newTicks.sort((a, b) => a[0] - b[0]);
                    } else {
                        // Merge + dedup theo timestamp (tránh duplicate nếu chạy lại)
                        const all = [...existing, ...newTicks].sort((a, b) => a[0] - b[0]);
                        merged = [];
                        let lastTs = -1;
                        for (const t of all) {
                            if (t[0] !== lastTs) { merged.push(t); lastTs = t[0]; }
                        }
                    }

                    const bytes = await putHourTicks(key, merged);
                    console.log(`    ✅ ${hourTag} — ${merged.length.toLocaleString()} ticks, ${(bytes/1024).toFixed(1)} KB`);
                    totalUploaded++;
                } catch (e) {
                    console.error(`    ❌ ${hourTag} lỗi: ${e.message}`);
                    totalFailed++;
                }
            }

            // Free memory ngay sau mỗi ngày
            for (const k of Object.keys(byHour)) delete byHour[k];
        }
    }

    console.log(`\n${'─'.repeat(50)}`);
    console.log(`✅ Uploaded : ${totalUploaded} hour-files`);
    console.log(`⏭️  Skipped  : ${totalSkipped} hour-files`);
    if (totalFailed > 0) console.log(`❌ Failed   : ${totalFailed}`);
    console.log(`${'─'.repeat(50)}\n`);

    if (totalFailed > 0) process.exit(1);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
