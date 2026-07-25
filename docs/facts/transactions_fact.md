# Technical Specification: Transaction Fact

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.4       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                                                         |
| ------- | ---------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                                                                                  |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại đầy đủ qua Q&A với Claude — chốt load strategy, partitioning, null-handling rule, FK resolution; sửa các lỗi/mâu thuẫn so với code hiện tại (xem mục 11 Decision Log) |
| v.0.0.3 | 2026-07-23 | NghiemCanCode | Qua Q&A với Claude: đổi resolve `customer_key`/`card_key` từ `is_current = true` sang **as-of range join** theo khoảng hiệu lực (point-in-time, khớp business spec §5); backdate version 1 dimension về 1900-01-01; full-refresh restatement cả chuỗi fact; bổ sung interval DQ checks (xem Decision Log) |
| v.0.0.4 | 2026-07-23 | NghiemCanCode | Qua Q&A với Claude (đợt "nhà cho metric definitions"): thêm cột `customer_age_at_transaction` — vật chất hóa Retirement-age segment (≥65 tại thời điểm giao dịch, business spec §4) tại fact thay vì để mỗi consumer tự tính (xem Decision Log) |

---

## 1. Overview & Business Context

> **Purpose:** Ghi lại thông tin mỗi giao dịch diễn ra
> **Primary consumers:** BI dashboard (chưa chọn công cụ cụ thể — xem Open Question #2)

| Attribute    | Value                       | Description                                                          |
| ------------ | --------------------------- | ---------------------------------------------------------------------- |
| SCD Type     | None                        | Fact table không tracking version — mỗi giao dịch là 1 sự kiện immutable |
| Special type | None                        |                                                                        |
| Grain        | Mỗi dòng là 1 giao dịch (1 `transaction_id`) |                                                        |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                       | Description                                |
| ---------------- | ------------------------------------------------------------ | ------------------------------------------- |
| Table name       | `gold.fact_transactions`                                    | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                                                         |                                            |
| Source(s)        | `silver.silver_transactions` (qua `stg_transactions`)        |                                            |
| Load strategy    | Incremental (merge on `transaction_id`, filter theo `timestamp`) | Giải pháp tạm thời trong giai đoạn migrate — xem Open Question #1 |
| Watermark column | `timestamp` (`silver_transactions.timestamp`)                | Không dùng `_updated_at` như bản draft ban đầu vì cột đó không tồn tại ở nguồn |
| Frequency        |                                                               | Chưa quyết định ở cấp dự án, đồng nhất với các bảng gold khác (xem Open Question #8) |
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

> **Grain:** Mỗi dòng là 1 giao dịch, xác định duy nhất bởi `transaction_id`.
> **Primary Key:** `transaction_id`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT fact_transactions.transaction_id)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source                                          | Dependency Type    | Note                                                                                     |
| ------------------------------------------------------ | ------------------- | ----------------------------------------------------------------------------------------- |
| `silver.silver_transactions` (qua `stg_transactions`) | Hard (must finish)  | Nguồn duy nhất của mọi cột                                                              |
| `gold.dim_dates`                                       | Hard (must finish)  | `date_key` phải resolve được; nếu không, dòng bị skip (mục 7)                            |
| `gold.dim_times`                                       | Hard (must finish)  | `time_key` dùng để xác định giờ giao dịch — xem gap về enforce thực tế ở Open Question #6 |
| `gold.dim_customers`                                   | Soft (best effort)  | `customer_key` map Unknown (`-1`) nếu không resolve                                      |
| `gold.dim_cards`                                       | Soft (best effort)  | `card_key` map Unknown (`-1`) nếu không resolve                                          |
| `gold.dim_geo`                                         | Soft (best effort)  | `merchant_geo_key` map Unknown (`-1`) nếu không resolve                                  |
| `gold.dim_merchant`                                    | Soft (best effort)  | `mcc` map Unknown (`-1`) nếu không resolve                                                |

### Downstream Consumers

| Type         | Name                      | Note                                                                                                    |
| ------------ | ------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Table        | `gold.trans_error_bridge` | Không join trực tiếp lúc load — chỉ tham chiếu chéo ở DQ check (`is_error=true` phải có bridge record tương ứng) |
| Table        | `gold.fact_user_monthly_snapshot` | Fact-of-fact, đọc trực tiếp bảng này                                                                    |
| Table        | `gold.fact_daily_transaction_trend` | Fact-of-fact, đọc trực tiếp bảng này (spec: `daily_transaction_trend_fact.md` — đã implement, green trên dev 2026-07-24) |
| Table        | `gold.fact_customer_activity_daily` | Reporting fact — Active Customer/Card 90 ngày (spec: `customer_activity_daily_fact.md` — đã implement, green trên dev 2026-07-24) |
| BI Artifact  | Chưa chọn công cụ cụ thể  | Xem Open Question #2                                                                                        |
| Data Product |                           | Chưa xác định                                                                                                |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name         | Data Type      | Key Type            | Not Null | Transformation Logic                                                                                                                                                    | Null Handling               | Allowed Range / Sample                                                      | Business Definition                                                                                             |
| ------------------- | -------------- | -------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `transaction_id`    | `StringType`   | PK                   | Y        | `silver_transactions.transaction_id` (qua `stg_transactions`)                                                                                                          | Raise pipeline error         | —                                                                           | Định danh nghiệp vụ duy nhất của giao dịch.                                                                     |
| `timestamp`         | `TimestampType`|                      | Y        | `silver_transactions.timestamp`                                                                                                                                        | Raise pipeline error         | —                                                                           | Thời điểm giao dịch xảy ra (UTC); nguồn để tính `date_key`/`time_key` và làm watermark incremental load.        |
| `date_key`          | `IntegerType`  | FK                   | Y        | `date_format(timestamp, "yyyyMMdd").cast("int")`, left join `dim_dates`                                                                                                | Skip record + log warning    | —                                                                           | FK tới `gold.dim_dates.date_key`.                                                                                |
| `time_key`          | `IntegerType`  | FK                   | Y        | `date_format(timestamp, "HHmmss").cast("int")`, left join `dim_times`                                                                                                  | Skip record + log warning    | —                                                                           | FK tới `gold.dim_times.time_key`. **Gap hiện tại:** code chưa thực sự filter theo kết quả join này — xem Open Question #6. |
| `customer_key`      | `StringType`   | FK                   | N        | `silver_transactions.client_id`, **as-of range join** `dim_customers` ON `client_id = customer_id AND timestamp >= effective_from_date AND timestamp < effective_to_date` — giao dịch gắn đúng phiên bản customer tại thời điểm giao dịch (đổi từ `is_current = true` ở v.0.0.2, xem Decision Log) | Map to Unknown (`-1`) + log  | —                                                                           | Point-in-time FK — nền tảng cho các phân tích SCD2 trong business spec §5 (before/after income bracket, relocation, historical-state reporting). |
| `card_key`          | `StringType`   | FK                   | N        | `silver_transactions.card_id`, **as-of range join** `dim_cards` ON `card_id = card_id AND timestamp >= effective_from_date AND timestamp < effective_to_date` (đổi từ `is_current = true` ở v.0.0.2, xem Decision Log)                                              | Map to Unknown (`-1`) + log  | —                                                                           | Point-in-time FK, cùng quy tắc với `customer_key`.                                                                |
| `transaction_type`  | `StringType`   | (Degenerate Dimension)| N        | `silver_transactions.transaction_channel`, chuẩn hóa uppercase; giá trị lạ → `UNKNOWN`                                                                                 | Default `UNKNOWN`            | `["SWIPE TRANSACTION", "ONLINE TRANSACTION", "CHIP TRANSACTION","UNKNOWN"]` | Degenerate Dimension, tạm thời chưa có thông tin thêm.                                                          |
| `merchant_id`       | `StringType`   | (Degenerate Dimension)| N        | `silver_transactions.merchant_id`                                                                                                                                      | Giữ nguyên, không impute      | —                                                                           | Business key của merchant, mang theo trực tiếp từ source — chưa có dimension ở đúng grain (xem Open Question #7). |
| `merchant_geo_key`  | `StringType`   | FK                   | N        | Process `silver_transactions.merchant_city`, `merchant_state`, `zip`; left join `dim_geo`                                                                              | Map to Unknown (`-1`) + log  | —                                                                           | FK tới `gold.dim_geo.location_key` (đổi tên từ `merchant_location` ở bản draft v.0.0.1 — đây là surrogate key, không phải string mô tả). |
| `mcc`               | `StringType`   | FK                   | N        | `silver_transactions.mcc`, left join `dim_merchant`                                                                                                                    | Map to Unknown (`-1`) + log  | —                                                                           | FK tới `gold.dim_merchant.mcc`.                                                                                  |
| `transaction_amount`| `DecimalType`  |                      | Y        | `silver_transactions.amount`                                                                                                                                           | Raise pipeline error         | Chưa xác định — cho phép giá trị âm (refund/hoàn tiền)? Xem Open Question #5 | Số tiền giao dịch — measure chính của fact.                                                                      |
| `is_error`          | `BooleanType`  |                      | Y        | `silver_transactions.is_error`                                                                                                                                         | Raise pipeline error         | `{true, false}`                                                             | Nếu `true`, phải có ≥1 record tương ứng trong `gold.trans_error_bridge` (theo `transaction_id`).                |
| `customer_age_at_transaction` | `ShortType` |               | N        | `year(timestamp) - birth_year` — `birth_year` lấy từ phiên bản **as-of** của `dim_customers` đã join ở bước resolve (v.0.0.4)                                          | `NULL` khi `birth_year` null hoặc `customer_key = '-1'` | `[0 - 120]`                        | Tuổi khách tại thời điểm giao dịch. Cờ Retirement-age segment (≥ 65, business spec §4) suy từ cột này — một nguồn sự thật, consumer không tự tính lại. |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
>
> **Quy tắc null-handling cho FK:** Not Null = `Y` (thuộc về grain — `date_key`, `time_key`) → không resolve được thì **skip cả dòng**. Not Null = `N` (thuộc tính phụ — `customer_key`, `card_key`, `merchant_geo_key`, `mcc`) → không resolve được thì **map về Unknown (`-1`) + log**, giữ nguyên dòng giao dịch (xem Decision Log #7 — tránh làm mất giao dịch thật khỏi báo cáo tổng chỉ vì 1 dimension phụ không resolve).
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------- | ------------------ | ------------------ |
| Add new column      |                     |                     |
| Drop column         |                     |                     |
| Rename column       |                     |                     |
| Change data type    |                     |                     |
| Change nullability  |                     |                     |
| Reorder columns     |                     |                     |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `trans_error_bridge.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value |
| ------------------- | ----- |
| Strategy            | None  |
| Input columns       |       |
| Policy              |       |
| Stability guarantee |       |

> Không cần surrogate key: `transaction_id` là business key gốc từ source, đủ ổn định và duy nhất để dùng trực tiếp làm PK.
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member         | Key value | Khi nào dùng                    |
| -------------- | --------- | ------------------------------- |
| Unknown        | `-1`      | NK null / không tìm thấy parent |
| Not Applicable | `-2`      | Hiện chưa dùng ở fact này, giữ để đồng bộ convention toàn dự án |
### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| --------------------------------- | ---- |
| Khi NK xuất hiện ở fact trước dim | N/A  |
| Placeholder attribute values      | N/A  |
| Back-update khi dim thật về       | N/A  |
| Cờ đánh dấu                       | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why                                             |
| ------ | ------------- | ------------------------------------------------ |
| None   | —             | Giao dịch là sự kiện immutable, không có version/lịch sử thay đổi. |


---
## 7. Relationship & FK Resolution

| FK Column Name                             | Parent Table                          | Join Condition                                                                          | Unmatched Key Handling      |
| -------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------ |
| `date_key`                                  | `gold.dim_dates.date_key`               | `fact_transactions.date_key = dim_dates.date_key`                                          | Skip record + log warning     |
| `time_key`                                  | `gold.dim_times.time_key`               | `fact_transactions.time_key = dim_times.time_key`                                          | Skip record + log warning     |
| `client_id` (→ `customer_key`)              | `gold.dim_customers.customer_id`        | `fact.client_id = dim_customers.customer_id AND fact.timestamp >= dim_customers.effective_from_date AND fact.timestamp < dim_customers.effective_to_date` (as-of range join) | Map to Unknown (`-1`) + log |
| `card_id` (→ `card_key`)                    | `gold.dim_cards.card_id`                | `fact.card_id = dim_cards.card_id AND fact.timestamp >= dim_cards.effective_from_date AND fact.timestamp < dim_cards.effective_to_date` (as-of range join)            | Map to Unknown (`-1`) + log    |
| `merchant_city, merchant_state, zip` (→ `merchant_geo_key`) | `gold.dim_geo`         | `fact.merchant_city = dim_geo.city AND fact.merchant_state = dim_geo.state AND fact.zip = dim_geo.zip` | Map to Unknown (`-1`) + log |
| `mcc`                                        | `gold.dim_merchant.mcc`                 | `fact_transactions.mcc = dim_merchant.mcc`                                                 | Map to Unknown (`-1`) + log    |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc incremental batch từ `stg_transactions`, filter `timestamp > max(timestamp)` hiện có trong `fact_transactions` (bỏ qua bước filter này khi backfill toàn bộ lịch sử).
2. Tính `date_key` (`yyyyMMdd`) và `time_key` (`HHmmss`) từ `timestamp`.
3. Left join `dim_dates`, `dim_times`, `dim_geo`, `dim_merchant`; **as-of range join** `dim_customers`/`dim_cards` theo khoảng hiệu lực (`timestamp >= effective_from_date AND timestamp < effective_to_date`) để resolve các FK. Yêu cầu thứ tự chạy: snapshot → dimension → fact trong cùng batch, để phiên bản dimension mới nhất đã tồn tại trước khi fact resolve key.
3b. Tính `customer_age_at_transaction = year(timestamp) - birth_year` từ chính phiên bản `dim_customers` đã as-of join ở bước 3; null khi không resolve được customer hoặc thiếu `birth_year`.
4. Map các FK nullable không resolve được (`customer_key`, `card_key`, `merchant_geo_key`, `mcc`) về Unknown (`-1`); dòng có `date_key` không resolve được thì skip toàn bộ dòng.
5. Merge (upsert) kết quả vào `gold.fact_transactions` theo `transaction_id`.

| Attribute           | Value                                                                                     |
| ------------------- | -------------------------------------------------------------------------------------------- |
| Merge / upsert keys | `transaction_id`                                                                             |
| Idempotency         | Merge on `transaction_id` + filter theo watermark `timestamp` đảm bảo rerun/backfill không tạo duplicate |
| Failure / retry     | Fail + halt khi vi phạm Critical checks (mục 9); an toàn để retry vì merge là idempotent      |

### 8.2 Backfill & Historical Load Strategy

> Full backfill toàn bộ lịch sử `silver_transactions` hiện có, chạy 1 lần khi migrate, sau đó chuyển sang incremental theo watermark `timestamp` — đồng bộ cách các bảng gold khác trong dự án đã backfill (`dim_customers`, `dim_cards`, `trans_error_bridge`).
>
> **Restatement khi đổi sang as-of join (Decision Log 2026-07-23):** dữ liệu đã load bằng quy tắc `is_current` cũ mang key sai về mặt point-in-time → bắt buộc `dbt build --full-refresh` cả chuỗi `fact_transactions` + `fact_user_monthly_snapshot` (và các fact phái sinh sau này) đúng 1 lần sau khi merge thay đổi, để toàn bộ lịch sử resolve lại theo cùng một quy tắc. Không chấp nhận trạng thái fact trộn 2 quy tắc resolve.

---
## 9. Data Quality & Observability Checks

| Check Name                       | Target Column                          | Rule/Condition                                                                | Threshold        | Severity | Frequency | Action on Fail        | Alert Channel |
| --------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------- | ------------------ | -------- | --------- | ------------------------ | --------------- |
| PK uniqueness                     | `transaction_id`                        | `COUNT(*) = COUNT(DISTINCT transaction_id)`                                       | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| PK not null                       | `transaction_id`                        | `transaction_id IS NOT NULL`                                                      | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Fact grain consistency            | `transaction_id`                        | 1 `transaction_id` chỉ xuất hiện đúng 1 record                                    | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Date FK integrity                 | `date_key`                              | 100% `date_key` tồn tại trong `dim_dates`                                         | 100% match         | Critical | Per run   | Pipeline fail + halt      |                 |
| Time FK integrity                 | `time_key`                              | 100% `time_key` tồn tại trong `dim_times`                                         | 100% match         | Critical | Per run   | Pipeline fail + halt      |                 |
| Amount completeness               | `transaction_amount`                    | `transaction_amount IS NOT NULL`                                                  | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Error bridge consistency          | `is_error`, `transaction_id`            | `is_error = true` ⇒ tồn tại ≥1 record tương ứng trong `gold.trans_error_bridge`  | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Source count reconciliation       | *                                        | `COUNT(fact)` = `COUNT(stg_transactions)` trong cùng batch                        | 0 lệch             | Critical | Per run   | Pipeline fail + halt      |                 |
| Source amount reconciliation      | `transaction_amount`                    | `SUM(fact.transaction_amount)` = `SUM(stg_transactions.amount)` trong cùng batch  | 0 lệch             | Critical | Per run   | Pipeline fail + halt      |                 |
| Current customer uniqueness       | `dim_customers.is_current`              | Mỗi `customer_id` chỉ có 1 record `is_current = true`                            | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Current card uniqueness           | `dim_cards.is_current`                  | Mỗi `card_id` chỉ có 1 record `is_current = true`                                | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| SCD2 interval no-overlap (customers) | `dim_customers.effective_from_date/to_date` | Mỗi `customer_id`: không có 2 version nào có khoảng `[from, to)` chồng lấn — chồng lấn làm as-of join fan-out | 0 violations | Critical | Per run | Pipeline fail + halt |                 |
| SCD2 interval no-overlap (cards)  | `dim_cards.effective_from_date/to_date`  | Mỗi `card_id`: không có 2 version nào có khoảng `[from, to)` chồng lấn           | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| SCD2 interval coverage (customers) | `dim_customers.effective_from_date/to_date` | Mỗi `customer_id`: các khoảng hiệu lực liền mạch từ `1900-01-01` (version 1 backdate) tới `9999-12-31`, không có gap — gap làm giao dịch rơi nhầm về Unknown (`-1`) | 0 violations | Critical | Per run | Pipeline fail + halt |                 |
| SCD2 interval coverage (cards)    | `dim_cards.effective_from_date/to_date`  | Mỗi `card_id`: các khoảng hiệu lực liền mạch từ `1900-01-01` tới `9999-12-31`, không có gap | 0 violations | Critical | Per run   | Pipeline fail + halt      |                 |
| Fact join determinism             | `customer_key`, `card_key`               | Mỗi transaction `timestamp` được bao phủ bởi **đúng 1** phiên bản dimension per entity — as-of join không fan-out và không rơi nhầm về `-1` khi entity tồn tại. **Enforce gián tiếp** (không có test riêng — fact không giữ `client_id`/`card_id` sau khi load): fan-out bị chặn bởi test `unique transaction_id`, gap/rơi nhầm `-1` bị chặn bởi 4 test interval no-overlap/coverage phía dimension (Decision Log 2026-07-24) | 0 violations       | Critical | Per run   | Pipeline fail + halt      |                 |
| Customer FK coverage              | `customer_key`                          | `% customer_key = '-1'`                                                          | < 2%               | High     | Per run   | Alert                    |                 |
| Card FK coverage                  | `card_key`                              | `% card_key = '-1'`                                                              | < 2%               | High     | Per run   | Alert                    |                 |
| Transaction Type UNKNOWN rate      | `transaction_type`                      | `% transaction_type = 'UNKNOWN'`                                                 | < 1%               | High     | Per run   | Alert                    |                 |
| Merchant Geo FK coverage           | `merchant_geo_key`                      | `% merchant_geo_key = '-1'`                                                      | < 10%              | High     | Per run   | Alert                    |                 |
| MCC FK coverage                   | `mcc`                                    | `% mcc = '-1'`                                                                   | < 5%               | High     | Per run   | Alert                    |                 |
| Date consistency                  | `date_key`                              | `date_key` khớp với `date_format(timestamp, "yyyyMMdd")`                          | 0 lệch             | High     | Per run   | Alert                    |                 |
| Time consistency                  | `time_key`                              | `time_key` khớp với `date_format(timestamp, "HHmmss")`                            | 0 lệch             | High     | Per run   | Alert                    |                 |
| Age plausibility                  | `customer_age_at_transaction`           | `dbt_utils.accepted_range` [0, 120] (bỏ qua NULL)                                 | 0 unexpected       | Warning  | Per run   | Alert                    |                 |
| Future transaction check          | `timestamp`                             | Không có transaction vượt quá ngày hiện tại + 1 ngày                              | 0 vi phạm          | Medium   | Per run   | Warning                  |                 |
| Historical transaction check      | `timestamp`                             | Không có transaction trước ngưỡng lịch sử cho phép (mốc cụ thể — xem Open Question #5) | 0 vi phạm     | Medium   | Per run   | Warning                  |                 |
| Duplicate business event          | `card_key`, `date_key`, `time_key`, `transaction_amount` | Tỷ lệ duplicate `(card_key, date_key, time_key, transaction_amount)` dưới ngưỡng | < 0.1% (giả định) | Medium   | Per run   | Warning                  |                 |
| Merchant Geo coverage trend       | `merchant_geo_key`                      | Rate `-1` không tăng quá 30% so với 7 ngày trước (30 là giả định nghiệp vụ)       | ±30%               | Low      | Per run   | Monitoring               |                 |
| Customer coverage trend           | `customer_key`                          | Rate `-1` không tăng đột biến (30% giả định, đồng nhất với Merchant Geo)          | ±30%               | Low      | Per run   | Monitoring               |                 |
| Daily transaction volume anomaly  | *                                        | Volume lệch >30% so với trung bình 30 ngày (30 là giả định nghiệp vụ)             | ±30%               | Low      | Per run   | Monitoring               |                 |
| Daily transaction amount anomaly  | `transaction_amount`                    | Tổng amount lệch >30% so với trung bình 30 ngày (30 là giả định nghiệp vụ)        | ±30%               | Low      | Per run   | Monitoring               |                 |

> **Alert Channel** chưa được điền — dự án chưa có hệ thống alerting thật (Slack/email/PagerDuty...), xem Open Question #9.

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column               | PII Level | Masking / Encryption Rule                                                                              | Data Retention / Purge Policy |
| --------------------- | --------- | ---------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `transaction_id`      | Chưa xác định | Xem Open Question #4 — đồng bộ với `trans_error_bridge.md` Open Question #4                          |                                   |
| `timestamp`           | NSA       | Không cần masking                                                                                         |                                   |
| `date_key` / `time_key`| NSA       | Không cần masking                                                                                         |                                   |
| `customer_key`        | —         | Xem PII tại `dim_customers` (`cards_dimension.md`-style docs) — đây chỉ là surrogate key, không tự thân là DI |                                   |
| `card_key`            | —         | Xem PII tại `dim_cards` — surrogate key, không tự thân là DI                                              |                                   |
| `transaction_type`    | NSA       | Không cần masking — chỉ là phân loại kênh giao dịch                                                      |                                   |
| `merchant_id`         | NSA       | Business identifier, không phải thông tin cá nhân                                                        |                                   |
| `merchant_geo_key`    | —         | Xem PII tại `dim_geo` (nếu có) — surrogate key                                                            |                                   |
| `mcc`                 | NSA       | Mã phân loại ngành hàng merchant, không phải thông tin cá nhân                                            |                                   |
| `transaction_amount`  | SA (đề xuất) | Gắn với `customer_key` có thể suy luận hành vi tài chính cá nhân — cần xác nhận, xem Open Question #5. Lake Formation column-level policy required (giống `expires_month`/`expires_year` ở `cards_dimension.md`) |  |
| `is_error`            | NSA       | Cờ vận hành, không phải thông tin cá nhân                                                                 |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question                                                                                                                                                                                                 | Blocking? | Owner         | Status |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | --------------- | ------ |
| 1   | Load strategy hiện dùng incremental merge tạm thời (vì `silver_transactions` chưa có `_updated_at`). Cần chuyển sang pure append + partition-overwrite khi silver có watermark ổn định.                | No        | NghiemCanCode  | Open   |
| 2   | Chưa chọn công cụ BI cụ thể để làm Primary/Downstream consumer chính thức (section 1, 4).                                                                                                              | No        | NghiemCanCode  | Open   |
| 3   | `dim_customers`/`dim_cards` chưa seed sẵn row Unknown (`-1`), nên relationships test trong `fact_transactions.yml` phải loại trừ `-1`/`-2` để tránh fail giả. Cần seed bổ sung ở 2 dimension đó.        | No        | NghiemCanCode  | Resolved |
| 4   | PII level của `transaction_id` chưa xác định — đồng bộ với Open Question #4 của `trans_error_bridge.md`.                                                                                                | No        | NghiemCanCode  | Open   |
| 5   | `transaction_amount` có cho phép giá trị âm (refund/hoàn tiền) không? Ảnh hưởng Allowed Range (mục 5.1), ngưỡng "Historical transaction check" (mục 9), và PII level đề xuất (mục 10).                  | No        | NghiemCanCode  | Resolved |
| 6   | `time_key` hiện được tính trực tiếp từ `timestamp` mà không join `dim_times` để enforce — DQ check "Time FK integrity" (mục 9) chưa được enforce thật trong pipeline. Cần cập nhật code sau khi spec này được duyệt. | No        | NghiemCanCode  | Resolved |
| 7   | `merchant_id` chưa có dimension ở đúng grain (chỉ có `dim_merchant` ở grain MCC). Nếu tương lai cần phân tích theo merchant cụ thể, cần xây `dim_merchant_id` riêng.                                    | No        | NghiemCanCode  | Open   |
| 8   | Frequency/Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án (đồng bộ `trans_error_bridge.md` Open Question #3).                                                                      | No        | NghiemCanCode  | Open   |
| 9   | Alert Channel (Slack/email/PagerDuty...) cho các DQ check chưa được xác định — dự án chưa có hệ thống alerting thật.                                                                                    | No        | NghiemCanCode  | Open   |
| 10  | **As-of range join (v.0.0.3) vẫn chưa được kiểm chứng — nhưng lý do đã thu hẹp (cập nhật 2026-07-24).** Phần bug relation cache đã **sửa xong và verify**: `customer_key`/`card_key` nay ổn định giữa các run, nên vế "dimension và fact buộc phải build cùng một lần chạy" **không còn đúng** — rebuild riêng lẻ đã an toàn. Vế còn lại vẫn nguyên: nguồn tĩnh nên mỗi customer/card vẫn chỉ có 1 version backdate về 1900-01-01, as-of join luôn khớp version 1 và cho ra **con số giống hệt** cách join `is_current` cũ. Vẫn không có cách nào biết logic mới đúng hay sai. Mở khoá bằng Open Question #2 của `docs/known_issues/dbt_spark_relation_cache.md` (tạo biến động thuộc tính cho nguồn tĩnh). | No | NghiemCanCode | Open (đổi phạm vi: nguồn tĩnh, không phải cache) |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision                                                                                          | Rationale                                                                                                                                          | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- |
| 2026-07-23 | Load strategy = Incremental + merge on `transaction_id` (tạm thời)                                    | `silver_transactions` không có `_updated_at`; giao dịch immutable nên merge giữ được uniqueness test qua các lần rerun/backfill trong giai đoạn migrate. | NghiemCanCode    |
| 2026-07-23 | Partitioning Columns = `date_key`                                                                    | Sửa `transactions.time` (không tồn tại) thành cột thật — chuẩn cho fact time-series trên Iceberg.                                                     | NghiemCanCode    |
| 2026-07-23 | Thêm `transaction_amount` và `time_key` vào section 5.1                                              | Cả 2 đã được code/yml/DQ checks (mục 9) dùng nhưng thiếu trong bảng cột gốc.                                                                          | NghiemCanCode    |
| 2026-07-23 | Đổi tên `merchant_location` → `merchant_geo_key`, Key Type = FK tới `dim_geo`                        | Đây là surrogate key join `dim_geo`, không phải string mô tả như draft ban đầu.                                                                       | NghiemCanCode    |
| 2026-07-23 | `card_key` join `dim_cards` (không phải `dim_account`)                                                | `dim_account` không tồn tại trong project — xác nhận là lỗi copy-paste ở bản draft v.0.0.1.                                                           | NghiemCanCode    |
| 2026-07-23 | `merchant_id` đổi Key Type từ FK → Degenerate Dimension                                              | Không có dimension nào ở grain `merchant_id`, chỉ có `dim_merchant` ở grain MCC.                                                                       | NghiemCanCode    |
| 2026-07-23 | Null-handling rule cho FK: Not Null=Y → skip row; Not Null=N → map Unknown(`-1`)+log                 | Best practice Kimball — tránh làm mất giao dịch thật khỏi báo cáo tổng chỉ vì 1 dimension phụ không resolve; chỉ skip khi FK thuộc về grain (`date_key`/`time_key`). | NghiemCanCode    |
| 2026-07-23 | Backfill = full historical load                                                                      | Đồng bộ cách các bảng gold khác đã backfill khi migrate (`dim_customers`, `dim_cards`, `trans_error_bridge`).                                          | NghiemCanCode    |
| 2026-07-23 | Frequency/Orchestrator chưa quyết định ở cấp dự án                                                    | Đồng bộ `trans_error_bridge.md` — chưa có quyết định chung cho gold layer.                                                                             | NghiemCanCode    |
| 2026-07-23 | DQ table: Frequency = "Per run" cho mọi check, Alert Channel để mở                                   | Dự án chưa có hệ thống alerting thật; "Per run" là tần suất hợp lý nhất cho batch pipeline hiện tại.                                                   | NghiemCanCode    |
| 2026-07-23 | `customer_key`/`card_key` tham chiếu ngược PII đã xử lý tại `dim_customers`/`dim_cards`, không lặp lại masking rule trong fact | Tránh 2 nguồn sự thật về PII cho cùng 1 giá trị gốc (`customer_id`/`card_id` đã hash tại dimension).                                       | NghiemCanCode    |
| 2026-07-23 | Primary/Downstream consumer = BI dashboard, chưa chọn công cụ cụ thể                                  | Chưa có quyết định công cụ BI ở cấp dự án tại thời điểm viết spec.                                                                                     | NghiemCanCode    |
| 2026-07-23 | `time_key` nay join thật `gold.dim_times` + skip cả dòng khi không resolve, giống hệt `date_key` (đóng Open Question #6) | Đúng quy tắc null-handling FK Not Null=Y đã chốt (mục 5.1/7/11); trước đó code chỉ tính `time_key` từ `timestamp` mà không enforce, khiến DQ check "Time FK integrity" (mục 9) không có tác dụng thật. | NghiemCanCode    |
| 2026-07-23 | Đã seed row Unknown (`-1`) và Not Applicable (`-2`) cho cả `dim_customers` và `dim_cards`, đồng bộ với `dim_geo`/`dim_merchant` (đóng Open Question #3) | Cho phép bỏ điều kiện loại trừ `-1`/`-2` khỏi relationships test của `customer_key`/`card_key` trong `fact_transactions.yml` — referential integrity đúng chuẩn Kimball mà không cần workaround. | NghiemCanCode    |
| 2026-07-23 | "Error bridge consistency" (mục 9) nay được enforce bằng singular test `tests/fact_transactions_is_error_has_bridge_record.sql` | `trans_error_bridge` đã được xây lại đầy đủ theo `transaction_errors_bridge.md` v.0.0.2 (không còn là skeleton stub), đủ điều kiện để đóng gap "chưa enforce" đã note ở Decision Log trước. | NghiemCanCode    |
| 2026-07-23 | `transaction_amount` được phép âm (refund/hoàn tiền hợp lệ), không thêm constraint giới hạn giá trị (đóng Open Question #5) | Refund là nghiệp vụ hợp lệ trong giao dịch thẻ; không có yêu cầu business nào loại trừ giá trị âm tại thời điểm quyết định.                             | NghiemCanCode    |
| 2026-07-23 | `customer_key`/`card_key` resolve bằng **as-of range join** (`timestamp >= effective_from_date AND < effective_to_date`) thay cho `is_current = true` (v.0.0.3) | Quy tắc `is_current` gán phiên bản *hiện tại* cho giao dịch quá khứ — thực chất là hành vi SCD Type 1, mâu thuẫn trực tiếp với yêu cầu Point-in-Time của business spec §5 và vô hiệu hóa 3 phân tích SCD2 showcase (success criteria 2/3/4). Backfill lịch sử nhiều năm bằng `is_current` làm toàn bộ quá khứ mang thuộc tính của hôm nay. | NghiemCanCode    |
| 2026-07-23 | Version 1 của mỗi entity trong `dim_customers`/`dim_cards` được backdate `effective_from_date` về `1900-01-01` | dbt snapshot chỉ bắt đầu tracking từ lần chạy đầu — nếu as-of join nghiêm ngặt, mọi giao dịch trước mốc đó rơi về Unknown (`-1`), hỏng toàn bộ báo cáo lịch sử theo segment. Quy ước phổ biến: trạng thái đầu tiên quan sát được đại diện cho quá khứ trước đó. **Limitation ghi nhận:** thay đổi thuộc tính xảy ra *trước* lần snapshot đầu không thể phục hồi — chấp nhận. | NghiemCanCode    |
| 2026-07-23 | Restatement: `dbt build --full-refresh` cả chuỗi `fact_transactions` + `fact_user_monthly_snapshot` đúng 1 lần sau khi đổi join rule | Không chấp nhận fact trộn 2 quy tắc resolve key (dòng cũ theo `is_current`, dòng mới theo as-of); quy mô dữ liệu portfolio đủ nhỏ để chạy lại toàn bộ. Backdate version 1 cũng làm đổi `effective_from_date`, nên dimension phải rebuild trước fact. | NghiemCanCode    |
| 2026-07-23 | Bổ sung DQ checks: SCD2 interval no-overlap + interval coverage cho `dim_customers`/`dim_cards`; check "Fact join determinism" định nghĩa lại theo interval | Check `is_current` uniqueness cũ không bảo vệ as-of join: chồng lấn khoảng hiệu lực gây fan-out (duplicate giao dịch trong báo cáo), gap gây rơi nhầm về `-1`. Hai check interval chặn cả hai lỗi từ gốc, tại dimension — trước khi fact join. | NghiemCanCode    |
| 2026-07-23 | Thêm cột `customer_age_at_transaction` (v.0.0.4) — lưu **tuổi**, không lưu cờ retirement | Vật chất hóa Retirement-age segment (≥65 tại thời điểm giao dịch, business spec §4) tại fact: `birth_year` đã sẵn trong phiên bản as-of join lúc load, tính 1 lần cho mọi consumer thay vì mỗi dashboard một công thức. Lưu tuổi thay vì cờ vì tuổi tổng quát hơn (cờ ≥65 suy được; đổi ngưỡng không phải rebuild). Thuộc đợt Q&A "nhà cho metric definitions" — cùng đợt với `fact_customer_activity_daily` (spec riêng). | NghiemCanCode    |
| 2026-07-24 | Check "Fact join determinism" (mục 9) enforce **gián tiếp**, không viết test riêng | Fact không giữ `client_id`/`card_id` sau khi load (business key bị drop, chỉ còn surrogate key) nên không test trực tiếp được trên fact. Fan-out đã bị chặn bởi test `unique transaction_id`; gap/rơi nhầm `-1` bị chặn bởi 4 singular test interval no-overlap/coverage phía dimension (`dbt/tests/dim_*_scd2_interval_*.sql`) — tổ hợp này tương đương về mặt logic. Cân nhắc và loại: (a) test re-join từ staging — đắt, quét lại toàn bộ staging mỗi lần test; (b) thêm `client_id`/`card_id` vào fact — trái spec §5.1. Quyết định qua Q&A. | NghiemCanCode    |
| 2026-07-24 | Đợt code point-in-time v.0.0.3/v.0.0.4 đã implement: backdate version 1 (`dim_customers.sql`/`dim_cards.sql`), as-of range join + `customer_age_at_transaction` (`fact_transactions.sql`), 6 singular tests (4 interval + 2 current-uniqueness) | Đồng bộ code với spec đã lock 2026-07-23. Restatement full-refresh chuỗi `snapshot → dim → fact` do owner tự chạy trên dev (không chạy tự động trong đợt code). | NghiemCanCode    |
