# PNJ year-by-year history collector — V27 controlled live collection

This candidate is isolated from WaveAlpha Production and existing `data-fetcher-king` workflows. It recovers PNJ public-website quote facts (all products/regions/updates) **one local calendar date per upstream request**. The page-backed endpoint is not a published bulk API, and one successful date in 2016 does not prove full continuity.

## Source and identity

- Official PNJ UI: https://giavang.pnj.com.vn/ . Observed page lookup `YYYYMMDD` at `edge-cf-api.pnj.io/ecom-frontend/v1/get-gold-price-history`.
- Observed initial year selector: 2016–2026 (November 2026). Start `2016-01-01` is **only a proposed lower bound**.
- Preserve `locations[] -> gold_type[] -> data[]` as **raw quote snapshots**, including intraday `updated_at`. No fake OHLC, no fabricated volume, no zero fill, no merging of SJC-at-PNJ with direct SJC.
- Source responses may contain unexpected rows; metadata records the quality issue, full raw response kept for review. `EMPTY_UNVERIFIED` is never counted as a covered trading day.

## Safety and activation

- `python scripts/pnj_history_backfill.py` = **offline PLAN only**, no PNJ or R2 egress.
- `python -m unittest discover -s tests -p test_pnj_history_backfill.py -v` = synthetic tests only.
- **LIVE ACTIVATION 2026-11-12**: owner explicitly approved PNJ history collection and R2 writes. GitHub `main` hourly cron is enabled with an emergency pause variable `WA_PNJ_HISTORY_PAUSED=1`. Runtime approval flags are set to `1` only in this dedicated workflow; direct invocation of Python remains offline unless both explicit runtime flags are supplied.
- Public PNJ date-picker backend is undocumented as a bulk API. Single-threaded polite collection, no proxy rotation, bypass, CAPTCHA evasion, or retries on blocked 401/403/429. Access is empirical, not a legal guarantee for any volume of collection. If PNJ blocks, persistent pause; seek source approval rather than bypass.
- No bypass for 403/429/3xx, CAPTCHA, rate-limit or IP ban; stops on first failure. No proxy rotation or identity spoofing, no unknown-host redirects.
- Single-threaded, 18 dates/run default, 10 seconds between requests (hard floor 8), max 24, 15-minute timeout. Once per hour at minute 17 UTC (may be delayed), GitHub public standard runner, no private `wave-alpha` Actions. Hard upstream response limit 128 KB/date, so ~4,000 dates conservatively require <= 0.52 GB raw response plus receipts.
- Only existing R2 bucket, isolated namespace `metal/pnj/history-v1/`. Exactly one writer `data-fetcher-king`; no new bucket, no Cloudflare worker, no Git commit of quote values, no public dataset endpoint.
- Each day is saved before state moves. On crash after saving a day, retry detects existing exact day object and advances without refetching. No manual overwrite for malformed or conflict. State is separate `_state.json`, plus individual `days/YYYY/MM/YYYY-MM-DD.json` objects.
- On first 401/403/429, the state is **PAUSED** persistently so later hourly jobs do not request again. Human review is needed to clear pause; the code does not include a reset path.
- Output marked `INTERNAL_REVIEW_ONLY`. Serving on WaveAlpha requires separate product/data/security approval.

## V26: chronological annual partitions + durable day-first commit

The bot intentionally **does not download an entire 365/366-day year in one GitHub Actions run**. That would violate timeout and lose progress. Each invocation processes a bounded slice of **one calendar year only**, maximum 18 days by default and hard maximum 24. It stops on December 31 and the next scheduled invocation starts on January 1 of the following year.

**Commit sequence for each calendar date**:
1. Read the exact day object (idempotent if it already exists).
2. If missing, make one PNJ request and write `metal/pnj/history-v1/days/YYYY/MM/YYYY-MM-DD.json` immediately; this object contains all source regions, product categories, and intraday quotes.
3. Read/update `metal/pnj/history-v1/years/YYYY/_progress.json`; the receipt carries `next_date`, processed counts, `EMPTY_UNVERIFIED` counts, quarantined row counts, and whether the scan genuinely started on January 1.
4. **Only after both writes succeed and R2 strong read-after-write verification passes for each object**, advance global `metal/pnj/history-v1/_state.json`. Global cursor writes are also read back before the job reports success.
5. On crash at any point, replay reads the existing day and year receipt and **does not fetch or double-count** a completed day.

Year terminal states are explicitly scan states, NOT a certificate of complete series: `SCANNED_YEAR_DAYS_OBSERVED`, `SCANNED_YEAR_HAS_EMPTY_UNVERIFIED_DAYS`, or `PARTIAL_YEAR_PRIOR_DAYS_UNVERIFIED` when an old V25 cursor started in the middle of a year. Even a scanned date may lack an individual product such as SJC or 999.9 rings; product-level completeness is a future independent audit.

If the official source returns 403/429, the bot pauses persistently. **No automatic retry, anti-bot bypass, proxy switching or concurrency increase** is allowed. Missing `robots.txt` evidence is not permission for bulk extraction.

`data-fetcher-king/main` is the only writer for this dataset. Initial R2 2026-11-12 observed usage ~2.48 GB account-wide across 3 buckets; November operations ~70,739 PutObject and 247,113 GetObject at check. R2 Standard Free 10 GB-month/1M Class A/10M Class B, but changing usage from other jobs means future 100% free cannot be mathematically guaranteed. Keep account dashboard monitored; set emergency pause before reaching allowances.

## Data quality gate V27

- Source raw is kept byte-equivalent in its structured JSON form; price strings are normalized only for validation (`35.850` -> 35,850 thousand VND/lượng), not silently rewritten or mistaken for VND/chỉ.
- Rows with wrong date, malformed price, inverted buy/sell are flagged and excluded from `updates_with_valid_date` and `target_coverage` but preserved in raw source payload.
- Target product coverage is tracked separately for PNJ-listed SJC and `Nhẫn Trơn PNJ 999.9`. Year statuses distinguish **missing product**, **empty days**, and **price quarantine** from fully observed dates.
- One live source example **2016-01-01**: 8 locations, 19 products, 21 valid observations, 5 SJC and 1 PNJ 999.9 ring, 0 quarantined; verified 3 R2 receipts and next cursor.
- **Full dataset is NOT yet verified complete, correct or clean.** Explicit year-by-year per-product coverage and independent sample comparisons are required before public website exposure.

## Run-plan maths

From 2016-01-01 through 2026-11-11: 3,968 calendar dates. At 18 dates/hour, a fully available sequence needs ~221 successful hourly runs (~9.2 days), *not guaranteed* due to blocked requests, skipped Actions and permission holds. Backfill cutoff is **yesterday ICT** to avoid incomplete intraday dates.

## Source truth

- Repo `annachou5566/data-fetcher-king`, default `main` read-only baseline `5091b0b5ac2f61f58fc010abc6bcf2c45e2ec0d3` on 2026-11-12.
- WaveAlpha private `test-wavealpha` unchanged; V24 approved UI SHA and offline adapter unchanged.
- LIVE: GitHub `main` cron definition is active after owner GO. Repository `WA_PNJ_HISTORY_PAUSED=1` stops subsequent jobs. Date 2016-01-01 was collected and verified by GET readback across day object, year receipt and global cursor; next_date=2016-01-02. No other date is claimed as collected until confirmed in R2.

**STATUS:** V27 SOURCE & OFFLINE QA PASS / ONE REAL DAY VERIFIED IN R2 / HOURLY CRON CONFIGURED; ongoing coverage and hosted run results must be checked separately.
**REMAINING RISK:** PNJ undocumented backend policy, real history coverage, source response drift, R2 Free quota/authorization, PNJ rate limit.
**NEXT BEST ACTION:** Review terms/robot access and get explicit activation + R2 write authorization, then controlled one-day live trial, assess source/status/coverage, finally enable bounded backfill.

## V27 acceptance checkpoint — 2026-11-12

- `31/31` exact `main` Python tests PASS in an isolated runtime after source-quality and R2 readback hardening.
- One real PNJ source date `2016-01-01` persisted and individually read back from existing Standard R2 bucket: day object, year receipt, global cursor. `next_date=2016-01-02`; no other data is claimed until hosted receipts are checked.
- GitHub `main` hourly scheduled source is deployed with owner GO; GitHub-hosted PNJ workflow RUN SUCCESS is **not yet verified**. Scheduled jobs may be delayed/held, so inspect Action runs and R2 cursor before claiming ongoing progress.
- Rate-limit (401/403/429), schema drift, invalid date, abnormal prices and missing target products retain fail-closed/quarantine semantics. Results stay `INTERNAL_REVIEW_ONLY` until coverage audited.
- R2 account Standard usage snapshot at audit: approximately 2.48 GB storage, November 2026 70,739 PutObject / 247,113 GetObject. This demonstrates Free headroom **at audit**, not a perpetual no-charge guarantee if unrelated traffic or data grows.
