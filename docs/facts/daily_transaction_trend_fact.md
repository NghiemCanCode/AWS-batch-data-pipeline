# Technical Specification: Daily Transaction Trend Fact

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.1       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                              |
| ------- | ---------- | ------------- | ---------------------------------------------------------------------------------------------------- |
| v.0.0.1 | 2026-07-23 | NghiemCanCode | Initial spec qua Q&A với Claude — chốt grain (Date × MCC × Merchant), tách amount spend/inflow, không lưu error_rate, quy tắc sentinel (xem mục 11 Decision Log). Model chưa được implement. |
| v.0.0.2 | 2026-07-24 | NghiemCanCode | Model được implement (`fact_daily_transaction_trend.sql` + `.yml` + 3 singular test). Chốt quy ước `batch_logical_date()` = ngày DỮ LIỆU (run T+1 phải truyền `--vars`), và scope 3 singular test = partition batch (không full history), qua Q&A — xem Decision Log. |

---

## 1. Overview & Business Context

> **Purpose:** Aggregate fact theo ngày phục vụ Dashboard A (Merchant & Category Spending — flagship) và câu hỏi "merchant nào có error rate bất thường hôm qua" (business spec success criterion 6). Trend theo category/merchant, mix shift, và error monitoring đọc từ bảng này thay vì aggregate lại fact_transactions mỗi lần.
> **Primary consumers:** Dashboard A (Marketing / Segment Manager, cadence weekly); BI dashboard (công cụ chưa chọn — đồng bộ Open Question #2 của `transactions_fact.md`)

| Attribute    | Value                       | Description                                                          |
| ------------ | --------------------------- | ---------------------------------------------------------------------- |
| SCD Type     | None                        | Aggregate fact — rebuild theo partition, không tracking version        |
| Special type | Aggregate (fact-of-fact)    | Nguồn duy nhất là `gold.fact_transactions`, không đọc silver           |
| Grain        | 1 dòng / (`date_key`, `mcc`, `merchant_id`) |                                                        |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                       | Description                                |
| ---------------- | ------------------------------------------------------------ | ------------------------------------------- |
| Table name       | `gold.fact_daily_transaction_trend`                          |                                            |
| Layer            | Gold                                                         |                                            |
| Source(s)        | `gold.fact_transactions`                                     | Fact-of-fact — cùng nguyên tắc với `fact_user_monthly_snapshot` |
| Load strategy    | Incremental (`insert_overwrite` partition theo `date_key`)   | Full history lần đầu / full-refresh; incremental chỉ overwrite partition của `batch_logical_date()` |
| Watermark column | Không cần — partition overwrite theo logical date            |                                            |
| Frequency        | Daily batch T+1 (theo business spec §7)                      |                                            |
| Orchestrator     |                                                               | Chưa quyết định ở cấp dự án                |
| SLA              | None                                                         |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value       |
| ------------------------- | ----------- |
| Table Format              | `Iceberg`   |
| Partitioning Columns      | `date_key`  |
| Z-Order / Clustering Keys | None        |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là tổng hợp giao dịch của 1 merchant trong 1 category trong 1 ngày.
> **Primary Key:** Composite (`date_key`, `mcc`, `merchant_id`) — không có surrogate key (đồng bộ quyết định của `fact_user_monthly_snapshot`).
> **Uniqueness test:** `dbt_utils.unique_combination_of_columns` trên (`date_key`, `mcc`, `merchant_id`)

> Lưu ý: `mcc` nằm trong grain dù về lý thuyết 1 `merchant_id` thuộc 1 category — nguồn không đảm bảo điều đó (mcc đến từng giao dịch). Nếu 1 merchant xuất hiện ở 2 mcc trong cùng ngày, mỗi tổ hợp là 1 dòng riêng.

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source              | Dependency Type    | Note                                                                 |
| ------------------------- | ------------------- | --------------------------------------------------------------------- |
| `gold.fact_transactions`  | Hard (must finish)  | Nguồn duy nhất của mọi measure; kế thừa toàn bộ quy tắc resolve/filter từ fact gốc (kể cả as-of join point-in-time từ v.0.0.3 của `transactions_fact.md`) |
| `gold.dim_dates`          | Soft (test only)    | Relationship test cho `date_key`                                     |
| `gold.dim_merchant`       | Soft (test only)    | Relationship test cho `mcc`                                          |

### Downstream Consumers

| Type         | Name                      | Note                                                        |
| ------------ | ------------------------- | ------------------------------------------------------------ |
| BI Artifact  | Dashboard A — Merchant & Category Spending | Flagship dashboard (business spec §6)       |
| BI Artifact  | Error-rate anomaly view   | Business spec success criterion 6                            |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name                    | Data Type        | Key Type | Not Null | Transformation Logic                                                                                     | Null Handling         | Allowed Range / Sample | Business Definition                                                                     |
| ------------------------------ | ---------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------- | ---------------------- | ----------------------- | ----------------------------------------------------------------------------------------- |
| `date_key`                     | `IntegerType`    | PK, FK   | Y        | `fact_transactions.date_key`, group by                                                                    | Raise pipeline error  | —                       | FK tới `gold.dim_dates.date_key`; partition key.                                        |
| `mcc`                          | `StringType`     | PK, FK   | Y        | `fact_transactions.mcc`, group by. **Giữ bucket `-1` (Unknown)** — volume/error của giao dịch không rõ category vẫn là thông tin (xem Decision Log) | Raise pipeline error  | —                       | FK tới `gold.dim_merchant.mcc`.                                                          |
| `merchant_id`                  | `StringType`     | PK       | Y        | `fact_transactions.merchant_id`, group by                                                                 | Raise pipeline error  | —                       | Degenerate dimension (không có dim ở grain merchant — Open Question #7 của `transactions_fact.md`). |
| `transaction_count`            | `IntegerType`    |          | Y        | `count(*)`                                                                                                | Raise pipeline error  | ≥ 1                     | Tổng số giao dịch trong ngày của merchant×category.                                     |
| `total_spend_amount`           | `DecimalType(18,2)` |       | Y        | `sum(case when transaction_amount < 0 then abs(transaction_amount) else 0 end)` — cùng quy ước tách dấu với `fact_user_monthly_snapshot` | Raise pipeline error  | ≥ 0                     | Tổng chi tiêu (phía khách hàng chi ra) — measure chính cho persona Marketing.           |
| `total_inflow_amount`          | `DecimalType(18,2)` |       | Y        | `sum(case when transaction_amount > 0 then transaction_amount else 0 end)`                                | Raise pipeline error  | ≥ 0                     | Tổng tiền vào (refund/credit).                                                          |
| `successful_transaction_count` | `IntegerType`    |          | Y        | `sum(case when is_error = false then 1 else 0 end)`                                                       | Raise pipeline error  | ≥ 0                     |                                                                                          |
| `failed_transaction_count`     | `IntegerType`    |          | Y        | `sum(case when is_error = true then 1 else 0 end)`                                                        | Raise pipeline error  | ≥ 0                     | Khớp định nghĩa Error Rate của business spec §4 (mọi giao dịch có error code).          |
| `unique_cards`                 | `IntegerType`    |          | Y        | `count(distinct case when card_key != '-1' then card_key end)` — loại sentinel                            | Raise pipeline error  | ≥ 0                     | **Chỉ đúng tại grain của bảng** — distinct không cộng dồn khi roll-up (xem note dưới).  |
| `unique_customers`             | `IntegerType`    |          | Y        | `count(distinct case when customer_key != '-1' then customer_key end)` — loại sentinel                    | Raise pipeline error  | ≥ 0                     | **Chỉ đúng tại grain của bảng** — như trên.                                             |

> **`error_rate` cố tình KHÔNG được lưu** (khác danh sách measures ở business spec bản cũ): ratio không cộng dồn — BI average cột này khi roll-up nhiều ngày/merchant sẽ ra số sai. Downstream luôn tính `failed_transaction_count / transaction_count` từ 2 cột sum, đúng ở mọi mức aggregate. Business spec §8 đã cập nhật theo (xem Decision Log).
>
> **`unique_cards` / `unique_customers` không cộng dồn:** muốn unique ở mức category (gộp nhiều merchant) hoặc nhiều ngày, phải tính từ `fact_transactions` — không sum từ bảng này. Ghi chú này bắt buộc có trong `fact_daily_transaction_trend.yml`.

### 5.2. Schema Evolution Policy

> Chưa quyết định ở cấp dự án — đồng nhất với các spec khác.

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

> Không có surrogate key — composite PK (`date_key`, `mcc`, `merchant_id`) đủ định danh grain, đồng bộ quyết định của `fact_user_monthly_snapshot`.

### 6.2. Unknown / Default Member

| Member         | Key value | Khi nào dùng                                                                 |
| -------------- | --------- | ----------------------------------------------------------------------------- |
| Unknown        | `-1`      | Bucket `mcc = '-1'` được **giữ** làm dòng dữ liệu bình thường (xem 5.1). Sentinel `card_key`/`customer_key = '-1'` bị **loại khỏi unique counts** nhưng giao dịch của chúng vẫn tính trong count/amount. |
| Not Applicable | `-2`      | Không phát sinh ở bảng này.                                                  |

### 6.3. Special Type Handling
> **Special Type:** Aggregate (fact-of-fact) — mọi quy tắc resolve key, null-handling, skip-row đã xử lý xong ở `fact_transactions`; bảng này không lặp lại.

### 6.4. SCD Type 2 - Change Tracking
> None — aggregate rebuild theo partition.

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table               | Join Condition                                  | Unmatched Key Handling                     |
| -------------- | -------------------------- | ------------------------------------------------ | ------------------------------------------- |
| `date_key`     | `gold.dim_dates.date_key`  | Kế thừa từ `fact_transactions` (đã enforce ở đó) | Không phát sinh — fact gốc đã skip          |
| `mcc`          | `gold.dim_merchant.mcc`    | Kế thừa từ `fact_transactions`                   | Bucket `-1` là dòng hợp lệ (seeded member)  |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `gold.fact_transactions`; incremental run: filter `date_key` thuộc ngày của `batch_logical_date()`; first run / full-refresh: toàn bộ lịch sử.
2. Group by (`date_key`, `mcc`, `merchant_id`), tính các measure ở mục 5.1 (unique counts loại sentinel `-1`).
3. `insert_overwrite` partition `date_key` vào `gold.fact_daily_transaction_trend`.

| Attribute           | Value                                                                                     |
| ------------------- | -------------------------------------------------------------------------------------------- |
| Merge / upsert keys | Không merge — overwrite nguyên partition `date_key`                                          |
| Idempotency         | Overwrite partition theo logical date → rerun cùng ngày cho kết quả y hệt, không duplicate    |
| Failure / retry     | Fail + halt khi vi phạm Critical checks (mục 9); an toàn retry vì overwrite là idempotent    |

### 8.2 Backfill & Historical Load Strategy

> Full history được build ở lần chạy đầu / full-refresh (aggregate thẳng từ toàn bộ `fact_transactions`). Sau restatement point-in-time của `fact_transactions` (Decision Log v.0.0.3 của `transactions_fact.md`), bảng này nằm trong chuỗi full-refresh cùng `fact_user_monthly_snapshot`.
>
> Correcting/backfill partition cũ ngoài `batch_logical_date()` chưa được xử lý — cùng open item với `fact_user_monthly_snapshot` (Open Question #1 của spec đó).

---
## 9. Data Quality & Observability Checks

| Check Name                     | Target Column                          | Rule/Condition                                                                       | Threshold    | Severity | Frequency | Action on Fail       | Alert Channel |
| ------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------- | ------------ | -------- | --------- | ----------------------- | --------------- |
| Grain uniqueness                | `date_key`, `mcc`, `merchant_id`        | `dbt_utils.unique_combination_of_columns`                                             | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Grain keys not null             | `date_key`, `mcc`, `merchant_id`        | not null                                                                              | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Date FK integrity               | `date_key`                              | relationship test → `dim_dates.date_key`                                              | 100% match   | Critical | Per run   | Pipeline fail + halt    |                 |
| MCC FK integrity                | `mcc`                                    | relationship test → `dim_merchant.mcc` (bucket `-1` là seeded member nên pass tự nhiên) | 100% match | Critical | Per run   | Pipeline fail + halt    |                 |
| Count reconciliation            | `transaction_count`                     | `sum(transaction_count)` per `date_key` = `count(*)` của `fact_transactions` cùng ngày | 0 lệch       | Critical | Per run   | Pipeline fail + halt    |                 |
| Amount reconciliation           | `total_spend_amount`, `total_inflow_amount` | `sum(spend) - sum(inflow)` per `date_key` khớp `-sum(transaction_amount)`... đối chiếu theo cùng quy ước dấu với `fact_transactions` cùng ngày | 0 lệch | Critical | Per run | Pipeline fail + halt |                 |
| Success/failed consistency      | `successful_transaction_count`, `failed_transaction_count` | `successful + failed = transaction_count` trên mỗi dòng              | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Non-negative measures           | các measure                              | `dbt_utils.accepted_range` min 0                                                      | 0 unexpected | Warning  | Per run   | alert                   |                 |
| Unknown MCC share               | `mcc`                                    | `% transaction_count` thuộc bucket `mcc = '-1'`                                      | < 5%         | High     | Per run   | Alert                   |                 |

> Reconciliation tests viết dạng singular test trong `dbt/tests/` — tái dùng pattern của `fact_user_monthly_snapshot_*_reconciliation.sql`. Alert Channel để mở, đồng bộ Open Question #9 của `transactions_fact.md`.

---
## 10. Security & Governance

| Column                | PII Level | Masking / Encryption Rule                                                       | Data Retention / Purge Policy |
| ---------------------- | --------- | -------------------------------------------------------------------------------- | -------------------------------- |
| Toàn bộ cột            | NSA       | Aggregate theo merchant/category/ngày — không còn định danh cá nhân; `unique_customers` là con số đếm, không truy ngược được cá nhân. |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions

| #   | Question                                                                                                                          | Blocking? | Owner         | Status |
| --- | ----------------------------------------------------------------------------------------------------------------------------------- | --------- | --------------- | ------ |
| 1   | Correcting/backfill partition cũ ngoài `batch_logical_date()` chưa xử lý — cùng open item với `fact_user_monthly_snapshot` #1.     | No        | NghiemCanCode  | Open   |
| 2   | ~~Ngưỡng "bất thường" cho error-rate anomaly (success criterion 6) chưa định nghĩa~~ — **đã chốt 2026-07-24**: merchant có `sum(failed) / sum(transaction_count) > 4.0%` trên cửa sổ **trailing 30 `date_key`**, chỉ xét merchant có `sum(transaction_count) >= 50` trong cửa sổ đó. Không dùng baseline deviation hay kiểm định thống kê (business spec Decision #22/#23). **Lưu ý khi viết query trên bảng này:** phải gộp qua cả `mcc` — cùng một merchant nằm ở nhiều dòng `mcc` trong một ngày, và bucket `mcc = '-1'` vẫn là giao dịch của merchant đó nên phải giữ, nếu không mẫu số thiếu và error rate bị thổi phồng (`metrics_layer.md` §3 quy tắc #6). Hai tham số đã được calibrate 2026-07-24 (ngưỡng 5% → **4,0%**, sàn giữ **50**); số liệu và lập luận ở `docs/metrics/abnormal_error_rate_calibration.md`. | No | NghiemCanCode | **Resolved** |

### Decision Log

| Date       | Decision                                                                                          | Rationale                                                                                                                                          | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- |
| 2026-07-23 | Grain = `date_key` × `mcc` × `merchant_id` (mở rộng từ Date × MCC ở business spec bản cũ)            | Business spec tự mâu thuẫn: grain Date × MCC không trả lời được success criterion 6 (error rate theo merchant) và "top merchants" của Dashboard A. Một bảng ở grain merchant phục vụ cả hai; category là roll-up. Business spec §8 cập nhật theo. | NghiemCanCode    |
| 2026-07-23 | Amount tách `total_spend_amount` / `total_inflow_amount`, không lưu cột net                          | Cùng quy ước tách dấu với `fact_user_monthly_snapshot`; phía "chi tiêu" là measure chính cho persona Marketing; net suy được bằng phép trừ.            | NghiemCanCode    |
| 2026-07-23 | KHÔNG lưu cột `error_rate` (khác danh sách measures business spec bản cũ)                            | Ratio không cộng dồn — average khi roll-up là sai số kinh điển. Downstream tính `failed / count` từ 2 cột sum, đúng ở mọi mức aggregate.               | NghiemCanCode    |
| 2026-07-23 | Giữ bucket `mcc = '-1'`; loại sentinel `-1` khỏi `unique_cards`/`unique_customers`                   | Volume/error của giao dịch không rõ category vẫn là thông tin và giữ reconciliation khớp với fact gốc (khác monthly snapshot — customer `-1` ở đó vô nghĩa cho snapshot hành vi khách). Sentinel không phải thẻ/khách thật nên loại khỏi distinct counts — nhất quán convention `fact_user_monthly_snapshot`. | NghiemCanCode    |
| 2026-07-23 | Fact-of-fact: nguồn duy nhất `gold.fact_transactions`; composite PK không surrogate key; `insert_overwrite` partition `date_key` theo `batch_logical_date()` | Đồng bộ toàn bộ pattern với `fact_user_monthly_snapshot` — một kiểu kiến trúc aggregate duy nhất trong dự án, dễ audit.                                | NghiemCanCode    |
| 2026-07-24 | `batch_logical_date()` = ngày DỮ LIỆU, không phải ngày chạy batch. Incremental overwrite đúng partition ngày đó theo spec nguyên văn; run T+1 sáng D+1 bắt buộc truyền `--vars '{batch_logical_date: <ngày D>}'`. | Giữ semantic macro nhất quán với `fact_user_monthly_snapshot` (một macro duy nhất). Phương án "trừ 1 ngày trong model" bị loại vì làm semantic macro khác nhau giữa 2 fact, dễ nhầm khi backfill. Default `current_date()` chỉ đúng khi chạy cùng ngày dữ liệu. | NghiemCanCode |
| 2026-07-24 | 3 singular test (count/amount reconciliation + Unknown-MCC-share) scope = partition batch, không full history. Reconciliation dùng `coalesce(...,0)` để không silent-pass khi partition rỗng. | Cùng scope với 2 reconciliation test hiện có của `fact_user_monthly_snapshot`, khớp phạm vi recompute của model, rẻ hơn mỗi run. Lỗ hổng NULL-partition của khuôn cũ được đánh dấu `TODO` để vá sau. | NghiemCanCode |
