# Business Specification — Retail Banking Analytics Platform

## 1. Project Overview
This project builds an analytics platform for a retail banking environment. The platform supports historical reporting, customer behavior analysis, card lifecycle monitoring, and merchant performance analysis.

> **Note:** This is a portfolio project built on synthetic data. The persona and business context below are illustrative, written to reflect how a real retail bank would consume this platform.

## 2. Primary User

**Persona: Marketing / Segment Manager**

Responsible for designing card promotions, cashback partnerships, and segment-targeted campaigns. Uses the platform to answer, without depending on the data team:

- Where do our customers spend, by merchant category and geography?
- Which segments (income, gender, age, location) drive transaction value, and how is that shifting?
- Which merchant categories are the best candidates for cashback partnerships?
- Does spending behavior change when a customer's circumstances change (income bracket, relocation)?

Cadence: reviews dashboards weekly for campaign planning; monthly for partnership and portfolio decisions.

## 3. Business Objectives

### Merchant & Category Analytics (primary)
- Which merchant categories capture the most customer spending — candidates for cashback partnerships?
- Which merchants generate the highest transaction volume and value?
- How do transaction patterns vary by merchant category over time?
- Which merchants deliver a poor customer experience (elevated error rates)?

### Customer Analytics
- Which customer segments generate the highest transaction volume?
- Which income groups contribute the most transaction value?
- How do spending patterns vary by geography?
- How does spending behavior change **after** a customer moves to a different income bracket? *(requires point-in-time attributes)*
- How do spending patterns by merchant category change **after** a customer relocates? *(requires point-in-time attributes)*

### Card Portfolio Analytics
- What percentage of customers use chip-enabled cards?
- How frequently are cards reissued?
- How does card technology impact transaction success rates?
- How has the card portfolio evolved over time?

## 4. Metric Definitions

All dashboards and datasets use these definitions. A metric name always means the same thing everywhere.

| Metric | Definition |
|---|---|
| **Active Customer** | Customer with ≥ 1 transaction in the trailing 90 days. |
| **Active Card** | Card with ≥ 1 transaction in the trailing 90 days (consistent with Active Customer). |
| **Error Rate** | Transactions with any error code ÷ total transactions, per grain (merchant, category, card type, day). |
| **Transaction Success Rate** | 1 − Error Rate. |
| **Chip Adoption Rate** | Chip-enabled cards ÷ total issued cards. |
| **Average Transaction Amount** | Total transaction value ÷ transaction count. |
| **Retirement-age segment** | Customers aged ≥ 65 (US convention) at the time of the transaction. |
| **Abnormal Error Rate (merchant)** | A merchant whose Error Rate over the **trailing 30 days** exceeds **4.0%**, counted only among merchants with **≥ 50 transactions** in that window. Method rationale in Decision #22, window in #23; both numbers were calibrated against real data on 2026-07-24 — see Decision #24. |

> These definitions are **enforced in the dbt layer**, not in BI tools: Active Customer/Card materialize in `fact_customer_activity_daily`; retirement age materializes as `customer_age_at_transaction` on `fact_transactions`; Error Rate is computed from the stored success/failure counts (never stored as a ratio). Abnormal Error Rate is **materialized in `rpt_merchant_error_daily`** (Decision #25), built and run green on dev 2026-07-25; its full technical spec is `docs/metrics/merchant_error_daily_report.md`. No §4 metric is left for BI to express itself. BI reads these — it does not re-implement the formulas. The full metric registry, materialization homes, and aggregation rules live in `docs/metrics/metrics_layer.md`.

## 5. Analytical Requirements

### Historical Analysis
Multi-year history of customer attributes, card attributes, and transaction activity. Merchant categories are a Type 1 lookup — no history is kept, because the source carries no time-varying merchant attribute (Decision #21).

### Point-in-Time Analysis
Reports must reflect customer and card attributes **as they were at the time of each transaction**, not as they are today. Concretely:

- Revenue-by-segment reports for a past year use the income bracket each customer had **in that year**.
- Before/after analyses (income bracket change, relocation) join transactions to the attribute version valid on the transaction date.

### Segmentation Analysis
- Income Bracket
- Gender
- Geography (State, City)
- Retirement age (≥ 65)

### Merchant Category Analysis
- Merchant — via the `merchant_id` degenerate dimension on `fact_transactions`; ID only, the source carries no merchant name (Decision #21)
- Merchant Category Code (MCC)

## 6. Dashboard Requirements

### Dashboard A — Merchant & Category Spending *(flagship)*
**Audience:** Marketing / Segment Manager. **Cadence:** weekly.

Answers: *where do customers spend, and which categories should we partner with?*

- Transaction value and volume by merchant category (MCC), with period-over-period trend
- Top merchants by value within a selected category
- Average transaction amount by category
- Spending mix shift over time (category share of wallet)
- Error rate by merchant/category — secondary signal for customer experience quality at a merchant (a partner candidate with high failure rates is a bad partner)

### Dashboard B — Customer Spending by Segment
**Audience:** Marketing / Segment Manager. **Cadence:** weekly/monthly.

Answers: *which segments drive value, and how is that changing?*

- Total transaction value, volume, active customers, average amount — each **broken down by** income bracket, gender, geography, and retirement-age segment, with period-over-period comparison
- Point-in-time views: segment revenue for past periods uses the attributes valid in those periods

### Dashboard C — Card Portfolio (summary section)
**Audience:** Marketing / Segment Manager (context), kept lightweight.

- Issued vs. active cards, chip adoption rate
- Card reissue count
- Transaction success rate by card type

> All three bullets read from `rpt_card_portfolio` *(built 2026-07-25 — Decision #26)*: one row per day per card segment (`chip_segment` × `card_brand`), counts only — chip adoption and success rate are computed by re-aggregating the counts, never stored as ratios. The model ran green on dev the same day — 35/35 tests across three consecutive runs with identical fingerprints (⇒ idempotent), 17,955 rows over full history — so Dashboard C has data behind it. Run log: `scripts/gold-dbt/README.md` §18; spec: `docs/metrics/card_portfolio_report.md`.

## 7. Data Freshness
Daily batch, **T+1**: dashboards reflect all transactions through the end of the previous day, available at start of business. Monthly snapshot facts finalize with the daily batch after month end.

## 8. Data Modeling Implications

### Customer Dimension (SCD Type 2)
- Income Bracket
- Address

### Card Dimension (SCD Type 2)
- Chip availability
- `has_cvv` flag (whether the card carries a CVV — **the CVV value itself is never stored in the analytics layer**)
- Expiration information
- Number of card reissues

### Merchant Category Dimension (Type 1)
- Merchant Category Code (MCC) and category name. A lookup dimension with no history: MCC is a stable industry code set, and the source `silver_mcc` carries no time-varying attribute.
- **The merchant itself has no dimension.** `silver_transactions` exposes only `merchant_id` plus the transaction's own city/state/zip — there are no merchant attributes to track. `merchant_id` is therefore carried on `fact_transactions` as a **degenerate dimension**, and merchant geography resolves through `dim_geo`. Consequence: Dashboard A's "top merchants" view identifies merchants by ID, not by name. See Decision #21 and `docs/facts/transactions_fact.md` Open Question #7.

### fact_transactions
Grain: one row per transaction. Carries point-in-time FKs to customer/card versions (as-of join) and `customer_age_at_transaction` (§4 retirement-age definition).

### fact_user_monthly_snapshot
Grain: one row per customer per month (aligned with the implemented model — the earlier "per card per month per currency" grain was superseded; see `docs/facts/user_monthly_snapshot_fact.md`).

### fact_customer_activity_daily
Grain: one row per customer per day. Materializes the §4 Active Customer / Active Card definitions (trailing 90 days). See `docs/facts/customer_activity_daily_fact.md`.

### rpt_merchant_error_daily *(built and run green on dev 2026-07-25)*
Grain: one row per merchant per day — **only merchants clearing the ≥ 50-transaction floor** in the trailing 30-day window (~165 rows/day, versus ~10,400 merchants transacting). Materializes the §4 Abnormal Error Rate definition and answers success criterion 6 directly.

Carries the window's transaction/failure counts, the merchant's error rate, the portfolio-wide baseline for the same window, and `excess_failed_transactions_30d` — the ranking column that keeps large merchants with real evidence above small merchants that cleared the threshold on three failures. The applied threshold and floor are stored on every row, so historical data states which parameters produced it. See `docs/metrics/merchant_error_daily_report.md`.

> Unlike the trend fact, this table **does** store a ratio (Decision #15 applies to roll-up tables; this is the final consumption grain). Its `_30d` columns must never be summed across days — consecutive windows overlap by 29 days.

### rpt_card_portfolio *(built and run green on dev 2026-07-25)*
Grain: one row per day per card segment (`chip_segment` × `card_brand`). Materializes all of Dashboard C: point-in-time issued count (SCD2 as-of, not `is_current`), Active Card count per segment (trailing 90 days, same §4 definition — second home alongside `fact_customer_activity_daily`, guarded by an exact cross-check test), transaction/success/failure counts over the same 90-day window, and the reissue distribution (0/1/2+ buckets plus total reissues).

Stores **counts only** — chip adoption and success rate are ratios computed downstream (this grain gets rolled up across brand/chip, the exact case Decision #15 exists for; the opposite call from `rpt_merchant_error_daily`, which is a final consumption grain). Unresolved-card transactions are kept in an (`UNKNOWN`, `UNKNOWN`) bucket per Decision #16's precedent. Known limitation: with the static synthetic source and the 1900-01-01 SCD2 backdate, the issued side is a flat line — real evolution appears only once the source can generate attribute changes. See `docs/metrics/card_portfolio_report.md`.

### fact_daily_transaction_trend
Grain: one row per Date × Merchant Category (MCC) × Merchant.

Measures: transaction_count, total_spend_amount, total_inflow_amount, successful_transaction_count, failed_transaction_count, unique_cards, unique_customers.

> Error Rate is **not stored** — it is computed downstream as failed ÷ count (per §4), because ratios cannot be averaged across roll-ups. unique_cards / unique_customers are valid only at table grain (distinct counts are not additive). See `docs/facts/daily_transaction_trend_fact.md`.

### Compliance note
If this platform ran in production, sensitive cardholder data (PAN, CVV values, PINs) would be excluded from the analytics layer entirely, in the spirit of PCI-DSS; customer PII would be access-controlled by role and subject to a retention policy. This project uses synthetic data, but the modeling follows the same rule: no sensitive card data beyond boolean technology flags.

## 9. Success Criteria

The platform succeeds when each question below is answerable with a single query or dashboard view, using the shared metric definitions in §4:

1. "Which three merchant categories should we shortlist for a cashback partnership this quarter?" — from Dashboard A, using category value/volume trends and error rate as a quality filter.
2. "Which income bracket drove the most transaction value last year, using the brackets customers were in **at that time**?" — point-in-time segment report; current attribute values must not overwrite history.
3. "How did a customer cohort's spending change after moving up an income bracket?" — before/after analysis via SCD2 as-of-date joins.
4. "How did category spending mix change for customers who relocated to another state?" — before/after relocation analysis.
5. "What share of our card base is chip-enabled, and does chip vs. non-chip affect success rate?" — Dashboard C, from `rpt_card_portfolio` (built and run green on dev 2026-07-25; Decision #26). Both halves are computed from stored counts at the level being viewed — adoption from `issued_card_count`, success rate from the `_90d` transaction counts — never read from a stored ratio.
6. "Which merchants show an abnormal error rate over the trailing 30 days?" — from `rpt_merchant_error_daily` (built and run green on dev 2026-07-25; computed off `fact_daily_transaction_trend`), refreshed daily at T+1. The window is 30 days rather than "yesterday" because at merchant × day grain most merchants have too few transactions for a rate to mean anything (Decision #23). Rank the shortlist by `excess_failed_transactions_30d`, not by the raw rate — see Decision #25.
7. Curated marts expose the §4 metrics with consistent definitions, suitable as inputs for future ML work.

> **Status caveat (updated 2026-07-24): criteria 2, 3 and 4 remain blocked, but by one cause instead of two** — the three SCD2 showcase analyses (Decision #10). The dbt-spark defect that stopped snapshots from accumulating history **has been fixed and verified on dev**: snapshots now emit `merge into`, `dbt_valid_from` is preserved across runs, and the surrogate keys derived from it are stable. What still blocks these three criteria is the remaining, independent cause: the synthetic source is static, so no attribute change exists for the snapshot to detect. Every customer and card therefore still holds exactly one attribute version, and the point-in-time machinery — though now sitting on a sound foundation — still cannot be distinguished from the current-attribute join it replaced. Unblocking these requires a way to generate attribute change in the source (Open Question #2 of the known-issues doc). Criteria 1, 6 and 7 are unaffected — they read current state. **Criterion 5 is partly affected (noted 2026-07-25, when `rpt_card_portfolio` was built):** its success-rate half is fine, but its chip-adoption half counts issued cards point-in-time over the same SCD2 versions, so with one version per card the adoption line is flat over time. The number for any single day is correct; only its *evolution* is missing, and it unblocks with the same source change as criteria 2/3/4 (spec `docs/metrics/card_portfolio_report.md` §3.1). Full causal chain, verification results and remaining gaps: `docs/known_issues/dbt_spark_relation_cache.md`.
>
> **Two limits that apply to every criterion, including the unaffected ones (added 2026-07-25).** First, the §7 T+1 cadence is a *requirement, not yet an implementation*: every model so far has been built by hand-running the commented steps of `scripts/gold-dbt/deploy_gold_dbt_dev.sh`, and no orchestrator schedules them (`docs/known_issues/dbt_spark_relation_cache.md` Open Question #6 covers what an orchestrator must additionally grant). Second, the synthetic source ends at **2019-10-31**, so "trailing 30/90 days" and "yesterday" always mean *relative to that date*, never to the wall clock — anything consuming these tables must anchor its date filters to the data's max date rather than `current_date`, or it renders empty.

---

## Appendix: Decision Log

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Flagship dashboard | **A — Merchant & Category** | Best story on top of fact_daily_transaction_trend; richest demo (trends, mix shift, anomalies). |
| 2 | Primary persona | **Marketing / Segment Manager** | Single persona keeps the spec focused; their questions span all three dashboards. |
| 3 | Dashboard A framing | **Marketing-oriented** | Story centers on category spending → cashback partnership targeting; error rate kept as a secondary experience-quality signal rather than a risk-monitoring story. |
| 4 | Active Customer | **≥ 1 transaction / trailing 90 days** | Common retail-banking convention; robust to seasonality. |
| 5 | Active Card | **≥ 1 transaction / trailing 90 days** | Consistency with Active Customer; "issued cards" reported separately in Dashboard C. |
| 6 | Error Rate | **Any transaction with an error code** | Simple, unambiguous, matches the existing trans_error_bridge model. |
| 7 | CVV in Card Dimension | **`has_cvv` boolean flag only** | Card-technology attribute is analytically useful; storing CVV values would violate PCI-DSS in a real deployment. |
| 8 | Retirement age threshold | **65 (US convention)** | Dataset is US retail banking (states, cities, MCC). |
| 9 | Data freshness | **Daily batch, T+1** | Matches the batch pipeline architecture and `batch_logical_date` convention. |
| 10 | SCD2 showcase analyses | **All three** (income-bracket change, relocation, historical-state reporting) | These are the analyses that justify the SCD2 investment and differentiate the portfolio. |
| 11 | Success criteria style | **Answerable business questions** | Each criterion is verifiable by running a query/dashboard; replaces unmeasurable "Support X" items. |
| 12 | Decision Log location | **Appendix in this spec** | Portfolio reviewers see the decision process alongside the spec itself. |
| 13 | fact_daily_transaction_trend grain | **Date × MCC × Merchant** | Original Date × MCC grain could not answer success criterion 6 (per-merchant error rate) or Dashboard A's top-merchants view; category level is a roll-up of the merchant grain. |
| 14 | Trend fact amounts | **Split spend / inflow** | Signed-amount convention shared with fact_user_monthly_snapshot; the spend side is the measure Marketing actually uses. |
| 15 | error_rate column | **Not stored, computed downstream** | Ratios are non-additive — averaging a stored rate across roll-ups gives wrong numbers; failed ÷ count from the sums is correct at every level. |
| 16 | Sentinel handling in trend fact | **Keep mcc '-1' bucket; exclude '-1' keys from unique counts** | Unknown-category volume is still information and keeps reconciliation with fact_transactions exact; sentinel keys are not real cards/customers. |
| 17 | Home for metric definitions | **dbt reporting layer** (`models/marts/reporting/`) | Single source of truth with yml docs + tests; MetricFlow/dbt Semantic Layer does not support the dbt-spark adapter, so a SQL reporting layer is the practical equivalent on this stack. |
| 18 | Active Customer/Card materialization | **fact_customer_activity_daily** (date × customer) | Entity-level daily grain lets Dashboard B break active counts down by segment via a point-in-time join to dim_customers; a totals-only daily aggregate could not. |
| 19 | Retirement age materialization | **customer_age_at_transaction column on fact_transactions** | birth_year is already available from the as-of dimension join at load time; computing age once at the fact beats every consumer re-deriving it. Age is stored (not a ≥65 flag) so the threshold can change without a rebuild. |
| 20 | Scope of this round | **Active 90d + retirement age only** | Chip adoption and error-rate reporting views are deferred — both are already computable directly from dim_cards and the trend fact. **Fully superseded:** the error-rate half by Decision #25, the card-portfolio half by Decision #26. The original rationale ("a `count/count` over `dim_cards` carries no aggregation trap") was true for chip adoption alone but did not survive Dashboard C's full scope — see #26. |
| 21 | Merchant modeling | **MCC lookup dimension (Type 1) + `merchant_id` as a degenerate dimension** — no SCD2 merchant dimension | Section 8 previously specified "Merchant Dimension (SCD Type 2)", which never matched the source or the implemented model. `silver_transactions` carries only `merchant_id` and the transaction's own city/state/zip; there is no merchant attribute that can change over time, so an SCD2 merchant dimension would hold exactly one version per merchant forever — history with nothing to record. Merchant-level analysis (Dashboard A top merchants, success criterion 6) works off the degenerate `merchant_id` at the `fact_daily_transaction_trend` grain. Trade-off accepted: merchants appear as IDs, not names. Revisit only if a merchant master source is added — tracked as `docs/facts/transactions_fact.md` Open Question #7. |
| 22 | "Abnormal" error rate definition | **Static threshold (> 5%) plus a minimum-volume floor (≥ 50 transactions)** — not a trailing-baseline deviation or a statistical test | Decision #3 already framed error rate as a customer-experience quality signal used to *filter partner candidates*, not as incident monitoring — so the question being answered is "is this merchant bad?", not "did this merchant just get worse?". A static threshold answers that directly and a Marketing Manager can state it in one sentence, whereas a deviation-from-own-baseline rule would never flag a merchant that is consistently terrible, which is exactly the merchant we must not partner with. The volume floor is the non-negotiable half: without it a merchant with 3 transactions and 1 error reads as 33% and swamps the list. Known limitation accepted: this cannot detect a merchant degrading from 5% to 9%. Both numbers are provisional — see Decision #24. |
| 23 | Evaluation window for criterion 6 | **Trailing 30 days**, replacing the original "yesterday" | At merchant × day grain most merchants have far too few transactions for a rate to be interpretable, so a single-day rule would either flag noise or (with the volume floor applied) return an almost empty list on most days. 30 days also matches the persona's monthly cadence for partnership decisions (§2). The fact stays daily and T+1; only the reporting window is wider, so the metric still refreshes every morning. |
| 24 | Threshold values 5% / 50 | ~~Provisional~~ → **Calibrated 2026-07-24: threshold 4.0%, floor 50 (unchanged)** | Picking the cut-offs honestly required knowing the portfolio-wide error rate and the merchant volume distribution; both are now measured. The portfolio baseline is **1.609%**, so 4.0% sits at 2.49× baseline and flags 12 merchants — a shortlist a person can actually work through, where the provisional 5% flagged only 6 and 3.0% failed the "at least 2× baseline" bar. The floor stays at 50 because it is the only one of 50/100/200 that satisfies every acceptance criterion: it discards 98.4% of merchants (the long tail the floor exists to remove) while retaining **70.2% of transactions and 68.1% of gross value**; raising it to 100 or 200 buys little and shrinks the list to 5 then 2. A dispersion test (chi-square 2.45–3.45 against the Poisson expectation) confirms merchants genuinely differ, so the cut-off is separating signal rather than slicing noise. Full measurements, method and interpretation: `docs/metrics/abnormal_error_rate_calibration.md`. The statistical caveat still stands and must be carried into the model's `.yml`: at n = 50 three failures alone clear 4%, so a flag near the threshold is weak evidence — the metric is a shortlisting filter, not a verdict. |
| 25 | Materializing Abnormal Error Rate | **Build `rpt_merchant_error_daily` as its own reporting model** — reversing the error-rate half of Decision #20; spec `docs/metrics/merchant_error_daily_report.md` | This is the last §4 metric BI would still have to write itself, and it has the most ways to be wrong of anything in the registry — each one silent. The rate must be summed across *every* MCC row of the merchant including the unresolved `'-1'` bucket (a missed row shortens the denominator and flags an innocent merchant); the volume floor must be applied before the threshold, not after; and the rate must never be averaged across a roll-up. All three produce a plausible-looking merchant list when done wrong. Two further choices follow from Decision #24's caveat rather than from convenience: the table stores `excess_failed_transactions_30d` (failures beyond what the portfolio baseline predicts) as the column BI sorts on, because every BI tool's default is to sort by the rate and that puts the weakest evidence on top; and it stores the applied threshold and floor on every row, because these numbers already changed once (5% → 4.0%) and history must say which parameters produced it. Scope kept deliberately narrow: only merchants clearing the floor are stored, so the table is ~590k rows over full history rather than the ~37M an unfiltered version would need for a long tail the floor exists to discard. |
| 26 | Materializing Dashboard C | **Build `rpt_card_portfolio` as its own reporting model** — reversing the remaining half of Decision #20; spec `docs/metrics/card_portfolio_report.md` | Decision #20's rationale covered chip adoption alone, but Dashboard C also requires success rate by card type — a query that joins `fact_transactions` to `dim_cards` **as-of** (rule #5 of the registry), aggregates over a trailing window, and computes a ratio that must never be averaged (rule #1). Each of those fails silently when done wrong in a BI tool, which is the same argument that reversed the other half of #20 (Decision #25). Once the model must exist for the hard part, carrying issued/active/reissue counts on the same grain is nearly free and gives Dashboard C a single SELECT source. Grain is day × card segment (`chip_segment` × `card_brand`); the table stores counts only — unlike `rpt_merchant_error_daily` it is a roll-up grain, so Decision #15's no-stored-ratio rule applies. Active Card thereby gains a second materialization home; the exception to the registry's one-home rule is guarded by an exact cross-check test against `fact_customer_activity_daily`. |
