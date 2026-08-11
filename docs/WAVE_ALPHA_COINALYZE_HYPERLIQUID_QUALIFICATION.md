# Wave Alpha — Coinalyze Hyperliquid qualification

Status: **research-only / not production eligible**.

Purpose: determine whether Coinalyze can serve as a zero-cost delayed aggregate fallback for Hyperliquid after Wave Alpha paused its incomplete first-party market-only collectors.

Hard rules:

- GitHub-hosted runner HTTP 403 is classified only as `RUNNER_VANTAGE_BLOCKED_OR_FORBIDDEN`; it does **not** prove the provider is unusable from Oracle, Cloudflare or another egress.
- The API key is read only from the repository secret `COINALYZE_API_KEY` and is never printed.
- Discovery must prove which Coinalyze exchange code represents Hyperliquid and how many futures/perpetual markets it exposes.
- Liquidation qualification uses `liquidation-history` with `convert_to_usd=true` and sweeps every discovered Hyperliquid future market while respecting the documented 40 call-unit/minute limit.
- A successful sweep is still not production authorization. Its rolling 24h Long/Short/Total must be compared with an independent same-time benchmark before any Wave Alpha aggregate activation.
- No schedule is included in this qualification workflow.
