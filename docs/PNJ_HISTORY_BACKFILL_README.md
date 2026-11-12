# PNJ year-by-year history collector — V26 candidate, OFF by default

This candidate is isolated from WaveAlpha Production and existing `data-fetcher-king` workflows. It recovers PNJ public-website quote facts (all products/regions/updates) **one local calendar date per upstream request**. The page-backed endpoint is not a published bulk API, and one successful date in 2016 does not prove full continuity.

## Source and identity

- Official PNJ UI: https://giavang.pnj.com.vn/ . Observed page lookup `YYYYMMDD` at `edge-cf-api.pnj.io/ecom-frontend/v1/get-gold-price-history`.
- Observed initial year selector: 2016–2026 (November 2026). Start `2016-01-01` is **only a proposed lower bound**.
- Preserve `locations[] -> gold_type[] -> data[]` as **raw quote snapshots**, including intraday `updated_at`. No fake OHLC, no fabricated volume, no zero fill, no merging of SJC-at-PNJ with direct SJC.
- Source responses may contain unexpected rows; metadata records the quality issue, full raw response kept for review. `EMPTY_UNVERIFIED` is never counted as a covered trading day.

## Safety and activation

- `python scripts/pnj_history_backfill.py` = **offline PLAN only**, no PNJ or R2 egress.
- `python -m unittest discover -s tests -p test_pnj_history_backfill.py -v` = synthetic tests only.
- Workflow has `schedule` and `workflow_dispatch` but job is **OFF** until **three** exact repository variables are explicitly enabled: `WA_PNJ_HISTORY_COLLECT_APPROVED=1`, `WA_PNJ_HISTORY_R2_WRITE_APPROVED=1`, AND `WA_PNJ_YEARLY_V26_ARMED=1`. This third new arm gate prevents accidental launch from earlier experiments. Runtime also requires `PNJ_HISTORY_COLLECT_APPROVED=1` + `PNJ_HISTORY_R2_WRITE_APPROVED=1` (passed only from those vars). None is enabled by this source candidate.
- **Separate authorization required** before enabling collection of multiple years or writing into R2. PNJ robots/Terms/bulk method, current R2 namespace owner, Cloudflare Free headroom, and user approval must be reconciled first.
- No bypass for 403/429/3xx, CAPTCHA, rate-limit or IP ban; stops on first failure. No proxy rotation or identity spoofing, no unknown-host redirects.
- Single-threaded, 18 dates/run default, 10 seconds minimum spacing (hard floor 8), at most 24, 15-minute job timeout; once/hour at minute 17 when eventually activated, GitHub timing not exact. Public-repo hosted Action is Free subject to GitHub policy; no private `wave-alpha` Actions.
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
4. **Only after both writes succeed**, advance global `metal/pnj/history-v1/_state.json`.
5. On crash at any point, replay reads the existing day and year receipt and **does not fetch or double-count** a completed day.

Year terminal states are explicitly scan states, NOT a certificate of complete series: `SCANNED_YEAR_DAYS_OBSERVED`, `SCANNED_YEAR_HAS_EMPTY_UNVERIFIED_DAYS`, or `PARTIAL_YEAR_PRIOR_DAYS_UNVERIFIED` when an old V25 cursor started in the middle of a year. Even a scanned date may lack an individual product such as SJC or 999.9 rings; product-level completeness is a future independent audit.

If the official source returns 403/429, the bot pauses persistently. **No automatic retry, anti-bot bypass, proxy switching or concurrency increase** is allowed. Missing `robots.txt` evidence is not permission for bulk extraction.

Existing `data-fetcher-king/main` jobs and protected WaveAlpha are independent; one new hourly cron should only be armed after confirming GitHub Actions Free, R2 Standard Free headroom, PNJ access method, and owner acceptance.

## Run-plan maths

From 2016-01-01 through 2026-11-11: 3,968 calendar dates. At 18 dates/hour, a fully available sequence needs ~221 successful hourly runs (~9.2 days), *not guaranteed* due to blocked requests, skipped Actions and permission holds. Backfill cutoff is **yesterday ICT** to avoid incomplete intraday dates.

## Source truth

- Repo `annachou5566/data-fetcher-king`, default `main` read-only baseline `5091b0b5ac2f61f58fc010abc6bcf2c45e2ec0d3` on 2026-11-12.
- WaveAlpha private `test-wavealpha` unchanged; V24 approved UI SHA and offline adapter unchanged.
- A candidate branch does not activate GitHub cron because scheduled workflows only execute on the default branch. Even if a candidate is promoted to `main`, the exact three-variable V26 approval gate stays OFF by default. Do not infer it ran from the presence of YAML.

**STATUS:** V26 SOURCE CANDIDATE / YEARLY SYNTHETIC QA PASS; LIVE PNJ/R2/HOSTED NOT RUN.
**REMAINING RISK:** PNJ undocumented backend policy, real history coverage, source response drift, R2 Free quota/authorization, PNJ rate limit.
**NEXT BEST ACTION:** Review terms/robot access and get explicit activation + R2 write authorization, then controlled one-day live trial, assess source/status/coverage, finally enable bounded backfill.
