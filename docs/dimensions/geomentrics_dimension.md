# Technical Specification: Geo Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | In Review     | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.2       |                                                |
| Owner (table) | NghiemCanCode |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                 |
| ------- | ---------- | ------------- | ----------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                            |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Rewritten to match `dim_geo.sql`/`stg_geo.sql`. Fixed copy-paste from `merchant_categories_dimension.md`/`cards_dimension.md` (Overview purpose, uniqueness test, downstream consumers). Corrected Load strategy/Watermark/Special type from "Late-Arriving" to "Full load / None / None". Filled sections 8.1, 9, 10, 5.1 business definitions. See Decision Log. |

---

## 1. Overview & Business Context

> **Purpose:** Chuẩn hoá địa điểm (city/state/zip) thành một conformed dimension dùng chung cho hai use case: merchant location của giao dịch (`fact_transactions.merchant_geo_key`) và địa chỉ khách hàng (`dim_customers.address_key`).
> **Primary consumers:** `fact_transactions`, `dim_customers`

| Attribute    | Value                                  | Description |
| ------------ | --------------------------------------- | ----------- |
| SCD Type     | Type 1                                 |             |
| Special type | None                                    | Xem Decision Log — trước đây ghi nhầm "Late-Arriving", đã sửa vì code không có logic backfill/placeholder nào. |
| Grain        | Mỗi dòng 1 địa điểm (state, city, zip) |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                                        | Description                                |
| ---------------- | ----------------------------------------------------------------------------- | ------------------------------------------ |
| Table name       | `gold.dim_geo`                                                                | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                                                                          |                                            |
| Source(s)        | `finance_silver.silver_transactions`<br>`finance_silver.silver_users` (qua `stg_geo`) | Sửa từ `silver.transactions`/`silver.users` ở bản draft v.0.0.1 — tên source thật theo `sources.yml` |
| Load strategy    | Full load (full table rebuild mỗi lần chạy)                                  | Sửa từ "Late" — `dim_geo.sql` là `materialized='table'`, không incremental/merge |
| Watermark column | None                                                                          | Sửa từ `_updated_at` — cột này không tồn tại, không có filter incremental nào trong `stg_geo.sql`/`dim_geo.sql` |
| Frequency        | On dbt run (không có lịch riêng)                                             | Chạy mỗi khi dbt DAG được trigger (vd `scripts/gold-dbt/deploy_gold_dbt_dev.sh`), không có cron/schedule độc lập |
| Orchestrator     | dbt (qua script deploy thủ công/CI, chưa có orchestrator riêng)              |                                            |
| SLA              | None                                                                          |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 thông tin về 1 địa điểm. Địa điểm bao gồm state, city (và zip)
> **Natural Key (NK):** None
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT location_key)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source                        | Dependency Type    | Note |
| ------------------------------------ | ------------------ | ---- |
| `finance_silver.silver_transactions` | Hard (must finish) |      |
| `finance_silver.silver_users`        | Hard (must finish) |      |

### Downstream Consumers

| Type  | Name                                            | Note                                                        |
| ----- | ------------------------------------------------ | ------------------------------------------------------------ |
| Table | `gold.fact_transactions.merchant_geo_key`       | Join theo `city`/`state`/`zip` (merchant location của giao dịch) |
| Table | `gold.dim_customers.address_key`                | Join theo `city`/`state` với `dg.zip = 'UNKNOWN'` (địa chỉ khách hàng, vì `silver_users` chưa enrich zip) |
| BI Artifact  |                                             |      |
| Data Product |                                             |      |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name    | Data Type    | Key Type | Not Null | Transformation Logic                                                         | Null Handling             | Allowed Range / Sample          | Business Definition                                                    |
| -------------- | ------------ | -------- | -------- | ---------------------------------------------------------------------------- | ------------------------- | ---------------------------------- | ---------------------------------------------------------------------- |
| `location_key` | `StringType` | PK, SK   | Y        | Surrogate Key Generation                                                     | Raise pipeline error      | —                                  | Khóa thay thế định danh duy nhất của location.                         |
| `city`         | `StringType` |          | Y        | Case 1: `silver_transactions.merchant_city`<br>Case 2: `silver_users.city`   | Skip record + log warning | Có xuất hiện giá trị `'ONLINE'`   | Thành phố của địa điểm — merchant location (từ giao dịch) hoặc customer address (từ hồ sơ khách hàng). Giá trị `'ONLINE'` hợp lệ cho giao dịch online. |
| `state`        | `StringType` |          | Y        | Case 1: `silver_transactions.merchant_state`<br>Case 2: `silver_users.state` | Skip record + log warning | —                                  | Bang/tiểu bang của địa điểm, cùng nguồn merchant/customer như `city`.   |
| `zip`          | `StringType` |          | Y        | Case 1: `silver_transactions.zip`<br>Case 2: `silver_users.zip`              | Default `'UNKNOWN'`       |                                     | Hiện tại `silver_users` chưa enrich field zip -> dùng giá trị Default. |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------ | ----------------- | --------------- |
| Add new column     |                   |                 |
| Drop column        |                   |                 |
| Rename column      |                   |                 |
| Change data type   |                   |                 |
| Change nullability |                   |                 |
| Reorder columns    |                   |                 |

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value              |
| ------------------- | ------------------ |
| Strategy            | Hash               |
| Input columns       | `city, state, zip` |
| Policy              | Dùng `SHA-256`     |
| Stability guarantee | Deterministic — cùng bộ (city, state, zip) luôn ra cùng `location_key`, kể cả sau full rebuild |
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member         | Key value | Khi nào dùng                    |
| -------------- | --------- | ------------------------------- |
| Unknown        | `-1`      | NK null / không tìm thấy parent |
| Not Applicable | `-2`      |                                 |
### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| --------------------------------- | ---- |
| Khi NK xuất hiện ở fact trước dim | N/A — không áp dụng, xem mục 1 (Special type = None) |
| Placeholder attribute values      | N/A  |
| Back-update khi dim thật về       | N/A  |
| Cờ đánh dấu                       | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | ------------- | --- |
| None   |               |     |


---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table | Join Condition | Unmatched Key Handling |
| -------------- | ------------ | -------------- | ---------------------- |
| None           |              |                |                        |



---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `stg_geo`: union merchant location từ `finance_silver.silver_transactions` (`merchant_city`/`merchant_state`/`zip`) với customer location từ `finance_silver.silver_users` (`city`/`state`, `zip` luôn mặc định `'UNKNOWN'`); lọc bỏ các dòng thiếu `city` hoặc `state`.
2. `SELECT DISTINCT city, state, zip` để dedupe.
3. Hash `sha2(concat_ws('||', city, state, zip), 256)` làm `location_key`.
4. Union thêm 2 dòng thành viên đặc biệt: Unknown (`-1`) và Not Applicable (`-2`).
5. Materialize toàn bộ kết quả thành `table` (drop & replace, không merge/upsert).

| Attribute           | Value                          |
| ------------------- | ------------------------------- |
| Merge / upsert keys | None (full table rebuild)       |
| Idempotency         | Có — full replace mỗi lần chạy, cùng input luôn ra cùng output |
| Failure / retry     | Theo cơ chế retry mặc định của dbt run/job điều phối; chưa có custom retry logic |

### 8.2 Backfill & Historical Load Strategy

> Không cần backfill riêng — mỗi lần chạy là full rebuild từ toàn bộ dữ liệu nguồn hiện có trong `stg_geo`.
---
## 9. Data Quality & Observability Checks

| Check Name      | Target Column  | Rule/Condition    | Threshold      | Severity | Frequency | Action on Fail | Alert Channel                          |
| --------------- | -------------- | ----------------- | -------------- | -------- | --------- | -------------- | ---------------------------------------- |
| PK uniqueness   | `location_key` | unique & not null | 0 - violations | Error    | per run   | fail + halt    | Chưa có — TODO(observability)           |
| Not null city   | `city`         | not null          | 0 - violations | Error    | per run   | fail + halt    | Chưa có — TODO(observability)           |
| Not null state  | `state`        | not null          | 0 - violations | Error    | per run   | fail + halt    | Chưa có — TODO(observability)           |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column  | PII Level | Masking / Encryption Rule | Data Retention / Purge Policy |
| ------- | --------- | ------------------------- | ------------------------------- |
| `city`  | QI        | TODO(security) — chưa có Lake Formation | TODO(security) |
| `state` | QI        | TODO(security) — chưa có Lake Formation | TODO(security) |
| `zip`   | QI        | TODO(security) — chưa có Lake Formation | TODO(security) |


---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
| 1   | Section 5.2 (Schema Evolution Policy) và section 8.1 Failure/retry chưa có quy tắc cụ thể — có cần định nghĩa riêng cho `dim_geo` hay dùng chung policy mặc định của dbt project? | No | NghiemCanCode | Open |
| 2   | Alert Channel cho các DQ check ở mục 9 hiện chưa tồn tại (không có logging/alerting hook trong dbt project) — cần chọn kênh (Slack/email/PagerDuty) khi observability được triển khai. | No | NghiemCanCode | Open |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision | Rationale | Decided by |
| ---------- | -------- | --------- | ---------- |
| 2026-07-23 | Sửa Overview > Purpose từ "Mỗi lần giao dịch với bên bán đều có 1 phân loại" thành mô tả conformed location dim dùng chung cho `fact_transactions` và `dim_customers` | Bản draft v.0.0.1 bị copy-paste nguyên câu từ `merchant_categories_dimension.md`, sai hoàn toàn với business context của `dim_geo` | NghiemCanCode |
| 2026-07-23 | Đổi Load strategy = Full load, Watermark column = None, Special type = None (thay vì "Late", `_updated_at`, "Late-Arriving") | `dim_geo.sql` là `materialized='table'`, full rebuild mỗi run, không incremental filter, không có logic placeholder/backfill nào — late-arriving pattern chỉ hợp lý cho dim có key cấp bởi hệ thống ngoài, không phải key tự hash từ chính thuộc tính (best practice Kimball cho conformed lookup dim) | NghiemCanCode |
| 2026-07-23 | Sửa Uniqueness test từ `COUNT(*) = COUNT(DISTINCT cards.card_key)` thành `COUNT(*) = COUNT(DISTINCT location_key)` | Bản draft v.0.0.1 copy-paste nhầm từ spec `dim_cards` | NghiemCanCode |
| 2026-07-23 | Sửa Downstream Consumers từ "gold.transactions, gold.users" (tên bảng không tồn tại) thành `fact_transactions.merchant_geo_key` và `dim_customers.address_key` | Khớp với các `left join {{ ref('dim_geo') }}` thật trong `dim_customers.sql` và `fact_transactions.sql` | NghiemCanCode |
| 2026-07-23 | Giữ nguyên tên file `geomentrics_dimension.md` (không đổi thành `geo_dimension.md`) dù không đúng convention | Tránh phá vỡ các link/reference hiện có đang trỏ tới file này | NghiemCanCode |
| 2026-07-23 | Sửa Source(s) từ `silver.transactions`/`silver.users` thành `finance_silver.silver_transactions`/`finance_silver.silver_users` | Tên source thật theo `sources.yml`, cùng cách sửa đã áp dụng cho `customers_dimension.md` | NghiemCanCode |
| 2026-07-23 | Gắn PII Level = Quasi-Identifier (QI) cho `city`/`state`/`zip`, để masking/retention là TODO(security) | Kết hợp 3 cột này có thể giúp re-identify cá nhân khi join với thuộc tính khác, dù từng cột riêng lẻ không nhạy cảm; dự án chưa migrate Lake Formation nên chưa enforce được — theo đúng pattern deferred đã dùng ở các dimension khác | NghiemCanCode |
