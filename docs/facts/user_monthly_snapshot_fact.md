# Technical Specification: User Monthly Snapshot Fact

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ----------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.2       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                        |
| ------- | ---------- | ------------- | ----------------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft (đã bị copy-paste nhầm nội dung từ Transaction Fact spec)                       |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại toàn bộ theo đúng ngữ cảnh periodic snapshot fact theo khách hàng (Q&A, xem mục 11 Decision Log) |

---

## 1. Overview & Business Context

> **Purpose:** Tổng hợp theo tháng hành vi giao dịch (thu/chi) của từng khách hàng, gộp tất cả các card mà khách hàng đó sở hữu. Đây là bản migrate từ legacy `account_monthly_snapshot_fact` (PySpark), nhưng đổi grain từ theo-card sang theo-khách hàng (xem Decision Log #1).
> **Primary consumers:** BI dashboard / Risk & Analytics team — theo dõi hành vi chi tiêu khách hàng theo tháng, phát hiện bất thường.

| Attribute    | Value                                | Description                                                                 |
| ------------ | ------------------------------------- | -------------------------------------------------------------------------------- |
| SCD Type     | None                                   | Deterministic aggregate, overwrite lại toàn bộ mỗi lần tính — không version hoá. |
| Special type | Periodic Snapshot Fact (Kimball)       | Mỗi tháng 1 dòng snapshot cho mỗi khách hàng, khác với accumulating/transaction fact như `fact_transactions`. |
| Grain        | Mỗi dòng là 1 khách hàng x 1 tháng     |                                                                                    |

---
## 2. Metadata & Operational Info

| Attribute        | Value                            | Description                                                                                          |
| ---------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Table name       | `gold.fact_user_monthly_snapshot` | Đổi tên từ `user_monthly_snapshot_fact` để khớp convention `fact_<danh từ>` đã dùng ở `fact_transactions` (xem Decision Log #6). |
| Layer            | Gold                                |                                                                                                             |
| Source(s)        | `gold.fact_transactions`           | Đây là fact-of-fact — aggregate từ 1 fact khác, không đọc trực tiếp từ silver.                          |
| Load strategy    | Partition Overwrite                 | `insert_overwrite` theo `month_end_date_key` (xem Decision Log #5).                                     |
| Watermark column | `fact_transactions.timestamp`      | Dùng để xác định phạm vi tháng cần recompute mỗi lần chạy — hiện tại chỉ recompute tháng chứa `batch_logical_date` (xem Open Question #1). |
| Frequency        | Daily                               | Cập nhật dần cho tháng đang mở (partial month), không chờ tháng đóng mới chạy (xem Decision Log #4).    |
| Orchestrator     |                                     | Chưa quyết định ở cấp dự án (giống mọi bảng gold khác hiện tại).                                        |
| SLA              | None                                |                                                                                                             |
### 2.1. Physical Storage Layout

| Attribute                 | Value                |
| -------------------------- | ---------------------- |
| Table Format                | `Iceberg`             |
| Partitioning Columns        | `month_end_date_key` |
| Z-Order / Clustering Keys   | None                  |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là tổng hợp giao dịch của 1 `customer_key` trong 1 tháng (`month_end_date_key`).
> **Primary Key:** Composite natural key `(month_end_date_key, customer_key)` — không dùng surrogate (xem Decision Log #7).
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT (month_end_date_key, customer_key))`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source              | Dependency Type    | Note                                                        |
| --------------------------- | ------------------- | -------------------------------------------------------------- |
| `gold.fact_transactions`   | Hard (must finish) | Nguồn duy nhất — cung cấp `customer_key`, `card_key`, `date_key`, `transaction_amount`, `is_error`. |

### Downstream Consumers

| Type         | Name | Note                                    |
| ------------ | ---- | ------------------------------------------ |
| Table        |      |                                            |
| BI Artifact  |      | Chưa xác định cụ thể — xem Open Question #2 |
| Data Product |      |                                            |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name                          | Data Type           | Key Type | Not Null | Transformation Logic                                                                                                       | Null Handling         | Allowed Range / Sample | Business Definition                                                                 |
| ------------------------------------- | -------------------- | -------- | -------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------ | ------------------------- | ----------------------------------------------------------------------------------------- |
| `month_end_date_key`                 | `IntegerType`        | PK, FK   | Y        | `last_day(date)` của tháng chứa giao dịch, format `yyyyMMdd`, cast `int`. Với `date` suy ra từ `fact_transactions.date_key`. | Raise pipeline error     | —                         | Ngày cuối cùng của tháng snapshot; FK tới `dim_dates.date_key`.                          |
| `customer_key`                       | `StringType`         | PK, FK   | Y        | `fact_transactions.customer_key`, loại bỏ dòng `customer_key = '-1'` trước khi group by (xem Decision Log #8)                  | Raise pipeline error     | —                         | Khách hàng được snapshot; FK tới `dim_customers.customer_key`.                            |
| `transaction_count`                  | `IntegerType`        |          | Y        | `count(*)` theo `(month_end_date_key, customer_key)`                                                                          | —                         | ≥ 1                       | Tổng số giao dịch (mọi card) của khách hàng trong tháng.                                  |
| `distinct_card_count`                | `IntegerType`        |          | Y        | `count(distinct card_key)` theo `(month_end_date_key, customer_key)` (xem Decision Log #3)                                     | —                         | ≥ 1                       | Số card khác nhau khách hàng dùng để giao dịch trong tháng — tránh mất thông tin khi gộp grain theo customer. |
| `total_income_amount`                | `DecimalType(18,2)`  |          | Y        | `sum(transaction_amount)` với `transaction_amount > 0`                                                                        | `0.00`                    | ≥ 0                       | Tổng tiền vào (income) trong tháng.                                                        |
| `total_outcome_amount`               | `DecimalType(18,2)`  |          | Y        | `sum(abs(transaction_amount))` với `transaction_amount < 0`                                                                   | `0.00`                    | ≥ 0                       | Tổng tiền ra (outcome) trong tháng, báo cáo dưới dạng số dương.                            |
| `average_daily_total_income_amount`  | `DecimalType(18,2)`  |          | Y        | `total_income_amount / days_in_month` (`days_in_month` = số ngày của tháng, không phải số ngày đã trôi qua)                   | `0.00`                    | ≥ 0                       | Trung bình thu nhập/ngày, tính trên tổng số ngày cả tháng (xem Open Question #4 khi tháng chưa đóng). |
| `average_daily_total_outcome_amount` | `DecimalType(18,2)`  |          | Y        | `total_outcome_amount / days_in_month`                                                                                        | `0.00`                    | ≥ 0                       | Trung bình chi tiêu/ngày, cùng lưu ý như trên.                                             |
| `successful_transaction_count`       | `IntegerType`        |          | Y        | `count(*)` với `is_error = false`                                                                                              | —                         | ≥ 0, ≤ `transaction_count` | Số giao dịch thành công (thay cho `trans_error_type_key == 0` của legacy — xem Decision Log #2). |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------- | ------------------ | ----------------- |
| Add new column       |                     |                     |
| Drop column           |                     |                     |
| Rename column         |                     |                     |
| Change data type     |                     |                     |
| Change nullability   |                     |                     |
| Reorder columns       |                     |                     |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `transaction_errors_bridge.md`, v.v.).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value |
| ------------------- | ----- |
| Strategy            | None  |
| Input columns       |       |
| Policy              |       |
| Stability guarantee |       |

> Không cần surrogate: đây là aggregate fact table, chưa có bảng nào khác cần FK 1-cột trỏ vào đây. Composite natural key `(month_end_date_key, customer_key)` đã đủ unique và được dbt `unique_key` nhận trực tiếp dạng list (xem Decision Log #7).
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member | Key value | Khi nào dùng                                                                    |
| ------ | --------- | ---------------------------------------------------------------------------------- |
| None   | —         | Không áp dụng — bảng này không phải parent table của FK nào. `customer_key = '-1'` (Unknown) bị loại bỏ khỏi aggregate thay vì giữ lại làm 1 bucket riêng (xem Decision Log #8). |
### 6.3. Special Type Handling
> **Special Type:** Periodic Snapshot Fact

| Aspect                                    | Rule                                                                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Rebuild policy                              | Mỗi lần chạy recompute toàn bộ (các) tháng bị ảnh hưởng từ `fact_transactions`, ghi đè nguyên partition — không SCD, không cộng dồn delta. |
| Partial month (tháng đang mở)               | Metrics được tính trên dữ liệu tới thời điểm chạy; `average_daily_*` vẫn chia cho `days_in_month` đầy đủ nên sẽ thấp hơn giá trị thật cho tới khi tháng đóng (xem Open Question #4). |
| Late-arriving correction cho tháng đã đóng | Chưa xử lý — xem Open Question #1.                                                                                       |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why                                                                    |
| ------ | -------------- | --------------------------------------------------------------------- |
| None   | —              | Đây là deterministic aggregate, ghi đè lại mỗi lần chạy — không có khái niệm version/lịch sử thay đổi trên chính bảng này. |

---
## 7. Relationship & FK Resolution

| FK Column Name       | Parent Table              | Join Condition                                                        | Unmatched Key Handling                                                                 |
| --------------------- | --------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `customer_key`        | `gold.dim_customers.customer_key` | Resolve sẵn tại `fact_transactions` (không join lại `dim_customers` ở đây) | `customer_key = '-1'` bị loại khỏi aggregate trước khi group by (xem Decision Log #8) — không map Unknown. |
| `month_end_date_key`  | `gold.dim_dates.date_key`  | `fact_user_monthly_snapshot.month_end_date_key = dim_dates.date_key`      | Raise pipeline error nếu không resolve — ngày cuối tháng luôn phải tồn tại trong `dim_dates`. |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `gold.fact_transactions`, lọc theo phạm vi tháng cần recompute (mặc định = tháng chứa `batch_logical_date`; xem Open Question #1 về late-arriving correction cho tháng cũ).
2. Loại bỏ dòng `customer_key = '-1'` (Unknown, chưa resolve).
3. Tính `month_end_date_key` (last day of month từ `date_key`), phân loại `transaction_amount` thành income/outcome, và `is_error = false` thành successful.
4. Group by `(month_end_date_key, customer_key)`, aggregate 7 metrics ở mục 5.1.
5. `insert_overwrite` vào `gold.fact_user_monthly_snapshot`, partition theo `month_end_date_key`.

| Attribute           | Value                                                                                          |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| Merge / upsert keys | N/A — overwrite theo partition, không merge theo key.                                             |
| Idempotency         | Rerun cùng phạm vi tháng → cùng kết quả, vì recompute từ đầu và ghi đè toàn bộ partition (deterministic aggregate). |
| Failure / retry     | Fail + halt khi vi phạm PK uniqueness / not-null (mục 9); an toàn để retry vì overwrite thay vì append incremental. |

### 8.2 Backfill & Historical Load Strategy

> Full backfill: recompute toàn bộ các tháng có sẵn trong `gold.fact_transactions` khi migrate lần đầu, sau đó chuyển sang chế độ daily update cho tháng hiện tại — đồng nhất cách các bảng khác trong dự án đã backfill.

---
## 9. Data Quality & Observability Checks

| Check Name                          | Target Column                                              | Rule/Condition                                                                          | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| ------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ---------------- | -------- | --------- | -------------- | ------------- |
| PK uniqueness                        | `(month_end_date_key, customer_key)`                        | unique & not null                                                                             | 0 violations     | Critical | per run   | fail + halt    |               |
| Not null                             | tất cả cột ở mục 5.1                                        | not null                                                                                       | 0 violations     | Critical | per run   | fail + halt    |               |
| Date FK integrity                    | `month_end_date_key`                                        | 100% tồn tại trong `dim_dates`                                                                | 0 violations     | Critical | per run   | fail + halt    |               |
| Successful ≤ total                   | `successful_transaction_count`, `transaction_count`          | `successful_transaction_count <= transaction_count`                                          | 0 violations     | Critical | per run   | fail + halt    |               |
| Non-negative amounts                 | `total_income_amount`, `total_outcome_amount`                | `>= 0`                                                                                         | 0 violations     | Critical | per run   | fail + halt    |               |
| No Unknown customer leakage          | `customer_key`                                               | `customer_key <> '-1'` (đã filter trước group by)                                            | 0 violations     | Critical | per run   | fail + halt    |               |
| Transaction count reconciliation     | `transaction_count`                                          | `sum(transaction_count)` theo tháng = `count(*)` của `fact_transactions` cùng tháng (loại `-1`) | 0 diff           | Critical | per run   | fail + halt    |               |
| Amount reconciliation                | `total_income_amount`, `total_outcome_amount`                | `sum(total_income_amount) + sum(total_outcome_amount)` = `sum(abs(transaction_amount))` của `fact_transactions` cùng tháng | 0 diff           | Critical | per run   | fail + halt    |               |
| Card count sanity                    | `distinct_card_count`                                        | `distinct_card_count >= 1`                                                                    | 0 violations     | High     | per run   | alert          |               |
| Monthly volume anomaly               | `transaction_count`                                          | Volume lệch >30% so với trung bình 3 tháng trước (30 là giả định nghiệp vụ)                   | > 30%            | Low      | per run   | monitoring     |               |
| Monthly amount anomaly               | `total_income_amount`, `total_outcome_amount`                | Tổng amount lệch >30% so với trung bình 3 tháng trước (30 là giả định nghiệp vụ)              | > 30%            | Low      | per run   | monitoring     |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column                                | PII Level | Masking / Encryption Rule                                                                          | Data Retention / Purge Policy |
| --------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------ | -------------------------------- |
| `customer_key`                         |           | Chưa xác định — đồng bộ với open question tương tự ở `transactions_fact.md` mục 10, cũng đang bỏ trống. |                                  |
| `total_income_amount`                  | SA        | Sensitive Attribute — tiết lộ hành vi tài chính cá nhân dù đã aggregate theo tháng.                     | Chưa xác định — xem Open Question #3 |
| `total_outcome_amount`                 | SA        | Cùng lý do như trên.                                                                                     | Chưa xác định — xem Open Question #3 |
| `average_daily_total_income_amount`    | SA        | Suy ra trực tiếp từ `total_income_amount`.                                                              | Chưa xác định — xem Open Question #3 |
| `average_daily_total_outcome_amount`   | SA        | Suy ra trực tiếp từ `total_outcome_amount`.                                                              | Chưa xác định — xem Open Question #3 |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question                                                                                                                                                                                         | Blocking? | Owner         | Status |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------------- | ------ |
| 1   | Tháng đã đóng (closed month) có nhận correction/giao dịch trễ (late-arriving) từ nguồn không? Nếu có, phạm vi recompute mỗi ngày (hiện chỉ recompute tháng chứa `batch_logical_date`) cần mở rộng ra N tháng gần nhất. | No        | NghiemCanCode | Open   |
| 2   | Có BI dashboard / data product cụ thể nào sẽ dùng trực tiếp bảng này không, hay hiện tại mới chỉ có yêu cầu chung "Risk & Analytics team"?                                                        | No        | NghiemCanCode | Open   |
| 3   | Data Retention / Purge Policy cho các cột tài chính (SA) chưa được quyết định ở cấp dự án — cần chính sách compliance chung cho Gold layer trước khi go-live thật sự.                            | No        | NghiemCanCode | Open   |
| 4   | `average_daily_total_*` chia cho `days_in_month` đầy đủ ngay cả khi tháng chưa đóng (partial month) — số liệu giữa tháng sẽ thấp hơn thực tế cho tới ngày cuối tháng. Có cần đổi mẫu số thành "số ngày đã trôi qua" cho tháng đang mở không, hay giữ nguyên hành vi kế thừa từ legacy? | No        | NghiemCanCode | Open   |
| 5   | Frequency / Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án.                                                                                                                 | No        | NghiemCanCode | Open   |
| 6   | PII level của `customer_key` chưa xác nhận (đồng thời cũng đang bỏ trống ở `transactions_fact.md`).                                                                                              | No        | NghiemCanCode | Open   |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision                                                                                          | Rationale                                                                                                                                    | Decided by     |
| ---------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | -------------- |
| 2026-07-23 | Grain đổi từ `(account_key, customer_key)` (legacy) sang chỉ `customer_key` x tháng                | Business muốn nhìn hành vi chi tiêu tổng thể của khách hàng, không tách riêng theo từng card.                                                    | NghiemCanCode  |
| 2026-07-23 | `successful_transaction_count` = `count(*) WHERE is_error = false`, thay vì `trans_error_type_key == 0` của legacy | `fact_transactions` (dbt) không còn dimension `trans_error_type`, chỉ có cờ boolean `is_error`.                                                   | NghiemCanCode  |
| 2026-07-23 | Thêm cột `distinct_card_count`                                                                    | Grain đổi từ card sang customer làm mất thông tin số lượng card; thêm cột này để giữ lại tín hiệu đó mà không phá vỡ grain mới.                  | NghiemCanCode  |
| 2026-07-23 | Bỏ hẳn `currency_key` khỏi bảng                                                                    | `fact_transactions` (dbt) hiện chưa có cột/dimension currency; giả định toàn bộ giao dịch cùng 1 currency (USD) cho tới khi có `dim_currency`.    | NghiemCanCode  |
| 2026-07-23 | Frequency = Daily, cập nhật dần cho tháng đang mở (partial month)                                  | Business cần số liệu gần real-time thay vì chờ tháng đóng mới có snapshot đầu tiên.                                                              | NghiemCanCode  |
| 2026-07-23 | Load strategy = Partition Overwrite (`insert_overwrite` theo `month_end_date_key`), không dùng incremental merge | Bảng aggregate cần cộng dồn vào giá trị đã tồn tại nếu merge — rủi ro tính sai/trôi số (drift) khi rerun. Overwrite theo partition đảm bảo idempotent & deterministic tuyệt đối, quan trọng với số liệu tài chính. | NghiemCanCode  |
| 2026-07-23 | Table name = `gold.fact_user_monthly_snapshot`                                                     | Khớp convention `fact_<danh từ>` đã dùng ở `fact_transactions`, thay vì giữ tên `user_monthly_snapshot_fact` kiểu legacy.                        | NghiemCanCode  |
| 2026-07-23 | Primary Key = composite natural key `(month_end_date_key, customer_key)`, không tạo surrogate key | Đây là aggregate fact table, chưa có bảng nào cần join FK 1-cột vào đây; surrogate chỉ có giá trị khi cần (không áp dụng ở đây), tránh overhead hash thừa. | NghiemCanCode  |
| 2026-07-23 | `customer_key = '-1'` (Unknown/chưa resolve) bị loại bỏ khỏi aggregate, không giữ làm 1 bucket riêng | Một dòng "Unknown customer" theo tháng không có ý nghĩa báo cáo hành vi khách hàng — khác với `fact_transactions`, nơi Unknown vẫn cần giữ để không mất giao dịch gốc. | NghiemCanCode  |
