# BTCUSDT USDⓈ-M futures — FROZEN historical research (NO-GO)

**Research outcome: NO-GO.** The only blind test (2025-01-01 through 2026-11-15) has already been consumed once. Do not reoptimize against it. See `REPORT_NO_GO_20261116.md`, `PREREGISTRATION_V1.md`, and `FINAL_REPORT.json`.

- Read-only downloader and auditor; no private credentials, no trading, no Cloudflare/Oracle, no WaveAlpha interaction.
- Only the `BTCUSDT` perpetual Binance Vision public data and Binance public funding API were used. `funding_nov_api.json` is checked in as the 45-event public Binance November 2026 funding snapshot, taken at the time of the original audit. The exact audited dataset ZIP is separately attached to the ChatGPT handoff. The standard Binance Vision `daily/fundingRate` file was not published at the checked URL.
- Original SHA-256 pre-registration document is embedded in `research.py` and verified before any PnL computation.
- 18 predeclared candidates plus two robustness deviations = 20/36 total configurations; OOS tested on exactly one candidate. No further parameter search is authorized.

## Run using the audited ZIP

```
python -m pip install 'numpy>=2,<3' 'pandas>=2,<3'
python quant_btc/research.py \
  --zip /path/to/BTCUSDT_BINANCE_VISION_AUDITED_20261116.zip \
  --prereg quant_btc/PREREGISTRATION_V1.md \
  --output /tmp/quant-validation
```

**Default is train+validation only.** The `--allow-blind` flag exists solely to reproduce the historically completed one-time evaluation, **not** for routine scheduled use. Do not run this against new data, repurpose 2025–2026 for selection, or publish a claimed GO from rerunning one old holdout.

Re-fetch existing frozen monthly and November data using `download_btc_vision.py`, audit with `audit_btc_vision.py`, and package with `package_dataset.py` if an external machine has network access; the downloader intentionally fixes dates to 2026-11-15, rather than continually adding test data. For reproducibility, use the original ZIP snapshot because Binance may revise historic bars.

## GitHub Actions

A manual-only workflow `btc-futures-quant-research.yml` can rerun validation on request; **no schedule** is set because repeated blind testing violates the protocol and would consume public runner minutes. This workflow does not read repository secrets and does not upload to R2 or change any existing cron/job. All paths under `quant_btc/` are additive.

Scientific caveats: The funding rate and timestamp are actual settled events, but settlement notional uses 1h candle OPEN as a mark-price proxy; executions use bar-level adverse slippage and conservative stops, but not full limit-order book or liquidation replay. Controls have count-mismatch caveats. See detailed final report.
