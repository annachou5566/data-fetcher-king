# PREREGISTRATION V1 — BTCUSDT Binance USDⓈ-M perpetual, 1h

**Status:** FROZEN BEFORE ANY STRATEGY BACKTEST. Historical data QC alone completed, no PnL results inspected. The checksum of this document is written to `preregistration.sha256` before running code. No redesign after viewing blind.

## Data/splits

Source: official Binance Vision USDT-margined BTCUSDT perpetual 1h OHLCV, 2020-01-01T00:00Z through 2026-11-15T23:00Z. Funding: official Binance Vision settled `last_funding_rate` from 2020-01 through 2026-10 and Binance public funding history for 2026-11. Exchange rate `funding_interval_hours` / exact event timestamp; no future funding lookahead. Source 4h/1d is QC-only (they show several inconsistencies with 1h); **all strategy decisions use 1h**. Signal computed using closed hourly bar i only; fill at hour i+1 **OPEN**. No borrowed API future candles.

1. Train: 2020-01-01 to 2023-12-31 23:59 UTC. Validation: 2024 calendar year. Blind OOS: 2025-01-01 through 2026-11-15. Blind stays **unqueried** through all training and validation decisions, and only one full champion blind evaluation is allowed.
2. Data anomalous windows were identified only by independent data QC, not strategy profitability: 2022-07-16 00:00-08:00 UTC, 2023-08-16 00:00-24:00 UTC, 2023-11-10 12:00-16:00 UTC, 2024-10-28 20:00-24:00 UTC. Because the 2024-10-28 20:00 1h candle has zero volume and a divergent historical API correction, **all new entries and any discretionary changes are disallowed in these windows**; existing trades are conservatively closed at the first available open before a flagged window if the stop has not already triggered. No fake or interpolated bars. These windows are all outside blind period. A predeclared sensitivity analysis will also exclude 24 hours following each flagged window; such analysis is reported as robustness, never used to optimize blind.
3. Although QA found a few volume-only 4h/1d aggregation mismatches, 1h price is primary and no volume indicator is used. Anomaly masks above remain fixed even if results suffer.

## Hypotheses, economic rationale, falsifiers

H1 — **Donchian price-channel breakout** (N=24, 48 or 96 completed 1h bars; initial/trailing ATR 2 or 3).
- Rationale: leverage/liquidation cascades and slow risk reduction can propagate a price breakout across multiple hours. Literature on managed futures and time-series trend is a proxy, not proof of BTC intraday profitability.
- Potential loser: slow hedgers and leveraged traders forced to reverse on adverse moves, plus contrarian liquidity providers during sustained trends.
- Why unexploited: stop-outs, cost drag in range-bound markets, convex tail capacity, rapid regime changes.
- Falsification: adverse post-breakout drift, expense-adjusted PF <= 1, negative OOS expectancy, or underperformance to random/shifted controls.
- Entry: if close[i] > max(high[i-N:i]) AND previous bar close did not already satisfy its own N-bar breakout, generate long; symmetric short against rolling prior lows. Only one position at once; opposite breakout reverses position at next OPEN. On each bar after entry, raise/lower trailing stop using completed prior bar's close and `ATR(20)*multiplier`; stops cannot be loosened. If stopped, must wait for a NEW breakout event. No fixed TP (convex payoff).
- Configurations: H1_N24_A2, H1_N24_A3, H1_N48_A2, H1_N48_A3, H1_N96_A2, H1_N96_A3.

H2 — **Time-series momentum with daily decision clock** (lookback=48, 96, 168 hours; initial/trailing ATR multiplier=2 or 3).
- Rationale: slow information diffusion, cross-market hedging and adjustment delays can sustain BTC perpetual returns. Academic precedent: Moskowitz, Ooi & Pedersen, *Time Series Momentum*, JFE 2012, based on other futures and generally longer horizons; this is a SHORTER-HORIZON TEST, not a replication.
- Potential loser: traders forced to rebalance against recent direction and delayed hedgers.
- Why unexploited: high turnover, reversals, trading cost and funding friction.
- Falsification: no positive persistence after next-open execution and full frictions, or poor out-of-sample robustness.
- Entry: only when the most recently completed bar is UTC 23:00, choose long if close[i] > close[i-L], short if lower; otherwise flat. At next OPEN enter if flat, reverse if different. Hold until next daily directional change or stop, with stop trailing as H1; if stopped, eligible next day only. No TP.
- Configurations: H2_L48_A2, H2_L48_A3, H2_L96_A2, H2_L96_A3, H2_L168_A2, H2_L168_A3.

H3 — **Contrarian to extreme *settled* funding** (absolute funding thresholds 0.0001, 0.0002, 0.0003, horizon 8 or 16 hours; fixed 2.5 ATR stop).
- Rationale: crowded levered long (positive funding) / short (negative funding) exposure may mean-revert, and contrarian side might collect future funding, though it can lose badly to persistent trends. Funding is not basis and this is not riskless carry.
- Potential loser: highly levered crowded directional participants who unwind at disadvantageous prices.
- Why unexploited: carry basis risk and dominant price trends may overwhelm funding receipts; shorting BTC is costly.
- Falsification: no subsequent reversal after a settled extreme or expenses outweigh drift and net future funding.
- Entry: only after official event becomes known, enforce **at least one fully CLOSED hourly candle after the funding event** (signal at end of that candle, trade next OPEN). If last settled rate >= threshold, short; <= -threshold, long. Ignore duplicate signals while a position is open. Fixed holding exit at the OPEN exactly horizon hours after entry, or earlier intrabar fixed stop 2.5 ATR. No TP; do not apply settled funding retroactively; assess *future* funding events only.
- Configurations: H3_F1_H8, H3_F1_H16, H3_F2_H8, H3_F2_H16, H3_F3_H8, H3_F3_H16 (F1=1 bp, F2=2 bp, F3=3 bp).

## Execution and risk simulation (strict)

- USDⓈ-M linear futures; starting equity 10,000 USDT per train/validation/blind segment, no compounding from outside segment. Taker fee 0.05% per fill, slippage adverse 0.03% per fill. Stress: adverse slippage 0.06% per fill. No rebates, maker assumptions or optimization of execution. Include fixed-notional funding cashflow at each exact historical event `-side*qty*mark_price_proxy*funding_rate` (mark price proxy from 1h OPEN at settlement, disclose approximation; use the actual historic *rate* and timestamp). No funding collected before entry.
- Position size <= min(0.5% pre-trade equity / estimated worst-case stop loss **including entry and exit fees and 0.06% per-side stressed slippage**, 3x pre-trade equity / entry price). No averaging down, one position only. All positions closed at segment end at market, paying costs. If stop trigger intrabar, sell at adverse stop or worse OPEN if gapped; stop checks use highs/lows that are only known to simulator **during** the bar, never used to create the opening decision. Trailing stop is only revised after bar CLOSE for the next bar. On SL + TP ambiguity choose SL (no tested hypothesis has a TP).
- Initial/trailing ATR uses conventional 20-bar true range smoothed as rolling arithmetic mean computed through last completed signal bar. Gaps honored. Trading halts while data QC flags windows. Zero-risk/undefined statistics display N/A, not made-up values.

## Selection rules and multiplicity

- **Fixed 18 primary configurations total**, precisely as above. Do not change these in response to results. Count every actual parameter combination executed in `ledger.json`. The 18 pre-registered configurations evaluated on 2020–2024 are 18 independent configurations, even if both train and validation are run. No trials on blind for selection.
- For each hypothesis select candidate by: train has at least 120 CLOSED trades, train positive expectancy and PF>=1.05 and max DD<=25%; among passing that choose highest train Sharpe. If none pass family is rejected. In 2024 require >=20 closed trades, PF>=1.10, expectancy>0, and max DD<=20%. Among surviving family winners, choose highest 2024 Sharpe (tie PF then lexicographic ID). If zero survive, **NO blind run**, scientific HOLD/NO-GO, no rescue searches.
- For sole champion, test **exactly two additional configurations**: lookback parameter -20% and +20% (rounded to nearest hour) for H1/H2; funding threshold -20% and +20% for H3. No respecification of stop/horizon. These two remain training/validation ONLY and count as two extra trials (max total 20/36). If either perturbation is strongly unstable on validation (PF<1.0 or expectancy<=0) then champion is disqualified and NO blind run. This does NOT authorize picking another champion.
- Once selected and passed ±20% robustness, lock source/prereg hashes and champion ID. Blind 2025–2026-11-15: run **exactly once**. Acceptance requires >=200 closed OOS trades, PF>=1.3, positive expectancy in R, max DD<=15%, Sharpe positive, each calendar year 2025 and 2026 separately positive, bootstrapped (1000 trade bootstrap, seed=20261116) 95% CI lower expectancy >0, stress slippage x2 PF>=1.1 and expectancy>0, plus better risk-adjusted return than 1x BTC buy/hold and annualized-volatility-matched buy/hold, and compared to 1000 random entry/direction and 1000 randomly shifted signal controls: exceed their 95th percentile of Sharpe. Failing any criterion => NO-GO; no parameter rescue.
- Comparison assumptions: Buy/hold 1x acquired at OOS opening and liquidated at last close, both at taker & slip, including funding for long perpetual; equity has no 3x amplification. Also report spot-style BTC close/close reference, but do not compare it without costs. Random and shifted controls replicate the champion's observed trade durations, count, fees, funding, and conservative slippage; these are statistical controls, NOT executable trading rules. Monte-Carlo seeds 20261116 and 20261117; use 1000 of each. For shifted signals, shift entry times by a random strictly nonzero whole-hour circular offset within OOS and keep each trade duration/side. For random entries, randomize start hours subject to equal exact count, matched duration list, non-overlap and randomize directions. If feasible matched controls cannot be constructed, report limitation and NO-GO.

## Safety and interpretation

- No orders, keys, secrets, live positions, deployment, or interaction with WaveAlpha. A public GitHub repo may host only a self-contained, sandboxed OFFLINE researcher. Do not publish protected account data or trade history. Do not schedule recurring backtests that would repeatedly unblind holdout. Prefer manually dispatched job once with lock hash; any future cron should audit fresh data only, not rerun or optimize blind.
- All reported PF/expectancy/trades/DD/Sharpe must be from executed code reading the actual downloaded files; no forecasts. Zero discovered edge is a successful research outcome.
