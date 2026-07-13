# Technical Specification: Transaction Fact

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.1       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  |               |                                                |
### Changelog

| Version | Date | Author        | Change        |
| ------- | ---- | ------------- | ------------- |
| v.0.0.1 |      | NghiemCanCode | Initial draft |

---

## 1. Overview & Business Context

> **Purpose:** Ghi lại thông tin mỗi giao dịch diễn ra
> **Primary consumers:** 

| Attribute    | Value | Description |
| ------------ | ----- | ----------- |
| SCD Type     | None  |             |
| Special type | None  |             |
| Grain        | Mỗi   |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                    | Description                                |
| ---------------- | ------------------------ | ------------------------------------------ |
| Table name       | `gold.fact_transactions` | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                     |                                            |
| Source(s)        | `silver.transaction`     |                                            |
| Load strategy    | Append                   |                                            |
| Watermark column | `_updated_at`            |                                            |
| Frequency        |                          |                                            |
| Orchestrator     |                          |                                            |
| SLA              | None                     |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value               |
| ------------------------- | ------------------- |
| Table Format              | `Iceberg`           |
| Partitioning Columns      | `transactions.time` |
| Z-Order / Clustering Keys | None                |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 giao dịch x timestamp 
> **Primary Key:** `transaction_id`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT fact_transactions.transaction_id)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source   | Dependency Type    | Note |
| -------------- | ------------------ | ---- |


### Downstream Consumers

| Type         | Name | Note |
| ------------ | ---- | ---- |
| Table        |      |      |
| Table        |      |      |
| BI Artifact  |      |      |
| Data Product |      |      |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name         | Data Type      | Key Type | Not Null | Transformation Logic                                                                                                              | Null Handling             | Allowed Range / Sample                                                      | Business Definition                                               |
| ------------------- | -------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `transaction_id`    | `StringType`   | PK       | Y        | `silver.transactions.transaction_id`                                                                                              | Raise pipeline error      | —                                                                           |                                                                   |
| `date_key`          | `IntegerType`  | FK       | Y        | `date_format(timestamp, "yyyyMMdd").cast("int")`, left join với `dim_date`<br><br>Với `timestamp = silver.transactions.timestamp` | Skip record + log warning | —                                                                           |                                                                   |
| `customer_key`      | `StringType`   | FK       | N        | `silver.transactions.client_id`<br>Left join `dim_customer` WHERE `is_current = true` ON `client_id = customer_id`                | Skip record + log warning | —                                                                           |                                                                   |
| `card_key`          | `StringType`   | FK       | N        | `silver.transactions.card_id`<br>Left join `dim_account` WHERE `is_current = true` ON `card_id = account_id`                      |                           |                                                                             |                                                                   |
| `transaction_type`  | `StringType`   |          | N        | `silver.transactions.transaction_channel`                                                                                         |                           | `["SWIPE TRANSACTION", "ONLINE TRANSACTION", "CHIP TRANSACTION","UNKNOWN"]` | Degenerate Dimension, tạm thời chưa có thông tin thêm             |
| `merchant_id`       | `StringType`   | FK       | N        | `silver.transactions.merchant_id`                                                                                                 | Skip record + log warning |                                                                             |                                                                   |
| `merchant_location` | `StringType`   | FK       | N        | Process `silver.transactions.merchant_city`, ``silver.transactions.merchant_state`` và `silver.transactions.zip`                  |                           |                                                                             |                                                                   |
| `mcc`               | `StringType`   | FK       | N        | `silver.transactions.mcc`                                                                                                         |                           |                                                                             |                                                                   |
| `is_error`          | `BooleanTyppe` |          | N        | `silver.transactions.is_errors`                                                                                                   |                           |                                                                             | Nếu True, phải có trong table `gold.trans_error_bridge` tương ứng |

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

| Attribute           | Value |
| ------------------- | ----- |
| Strategy            | None  |
| Input columns       |       |
| Policy              |       |
| Stability guarantee |       |
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
| Khi NK xuất hiện ở fact trước dim |      |
| Placeholder attribute values      |      |
| Back-update khi dim thật về       |      |
| Cờ đánh dấu                       |      |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | ------------- | --- |
| None   |               |     |


---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                    | Join Condition | Unmatched Key Handling      |
| -------------- | ------------------------------- | -------------- | --------------------------- |
| `client_id`    | `gold.dim_customer.customer_id` |                | Map to Unknown (`-1`) + log |


---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc batch
2. Transform bằng job
3. 

| Attribute           | Value |
| ------------------- | ----- |
| Merge / upsert keys |       |
| Idempotency         |       |
| Failure / retry     |       |

### 8.2 Backfill & Historical Load Strategy

> <>
---
## 9. Data Quality & Observability Checks

| Check Name                       | Target Column                                                               | Rule/Condition | Threshold       | Severity | Frequency | Action on Fail | Alert Channel |
| -------------------------------- | --------------------------------------------------------------------------- | -------------- | --------------- | -------- | --------- | -------------- | ------------- |
| PK uniqueness                    | `transaction_id` unique                                                     |                |                 | Critical |           | Pipeline fail  |               |
| PK not null                      | `transaction_id IS NOT NULL`                                                | Critical       | Pipeline fail   |          |           |                |               |
| Fact grain consistency           | 1 `transaction_id` chỉ xuất hiện 1 record                                   | Critical       | Pipeline fail   |          |           |                |               |
| Date FK integrity                | 100% `date_key` tồn tại trong `dim_date`                                    | Critical       | Pipeline fail   |          |           |                |               |
| Time FK integrity                | 100% `time_key` tồn tại trong `dim_time`                                    | Critical       | Pipeline fail   |          |           |                |               |
| Amount completeness              | `transaction_amount IS NOT NULL`                                            | Critical       | Pipeline fail   |          |           |                |               |
| Error bridge consistency         | `is_error = true => bridge record exists`                                   | Critical       | Pipeline fail   |          |           |                |               |
| Source count reconciliation      | Fact count = Source count                                                   | Critical       | Pipeline fail   |          |           |                |               |
| Source amount reconciliation     | Fact sum(amount) = Source sum(amount)                                       | Critical       | Pipeline fail   |          |           |                |               |
| Current customer uniqueness      | Mỗi `customer_id` chỉ có 1 record `is_current = true`                       | Critical       | Pipeline fail   |          |           |                |               |
| Current account uniqueness       | Mỗi `account_id` chỉ có 1 record `is_current = true`                        | Critical       | Pipeline fail   |          |           |                |               |
| Fact join determinism            | 1 transaction join ra tối đa 1 customer/account record                      | Critical       | Pipeline fail   |          |           |                |               |
| Customer FK coverage             | `% customer_key IS NULL < 2%`                                               | High           | Alert + Warning |          |           |                |               |
| Card FK coverage                 | `% card_key IS NULL < 2%`                                                   | High           | Alert + Warning |          |           |                |               |
| Transaction Type FK coverage     | `% transaction_type_key IS NULL < 1%`                                       | High           | Alert + Warning |          |           |                |               |
| Merchant FK coverage             | `% merchant_key IS NULL < 5%`                                               | High           | Alert + Warning |          |           |                |               |
| Merchant Geo FK coverage         | `% merchant_geo_key IS NULL < 10%`                                          | High           | Alert + Warning |          |           |                |               |
| Date consistency                 | `date_key` khớp với timestamp source                                        | High           | Alert           |          |           |                |               |
| Time consistency                 | `time_key` khớp với timestamp source                                        | High           | Alert           |          |           |                |               |
| Future transaction check         | Không có transaction vượt quá ngày hiện tại + 1 ngày                        | Medium         | Warning         |          |           |                |               |
| Historical transaction check     | Không có transaction trước ngưỡng lịch sử cho phép                          | Medium         | Warning         |          |           |                |               |
| Duplicate business event         | Tỷ lệ duplicate `(card_key,date_key,time_key,amount)` dưới ngưỡng           | Medium         | Warning         |          |           |                |               |
| Merchant Geo coverage trend      | Null rate không tăng quá 30% so với 7 ngày trước (30 là giả định nghiệp vụ) | Low            | Monitoring      |          |           |                |               |
| Customer coverage trend          | Null rate không tăng đột biến                                               | Low            | Monitoring      |          |           |                |               |
| Daily transaction volume anomaly | Volume lệch >30% so với trung bình 30 ngày (30 là giả định nghiệp vụ)       | Low            | Monitoring      |          |           |                |               |
| Daily transaction amount anomaly | Tổng amount lệch >30% so với trung bình 30 ngày (30 là giả định nghiệp vụ)  | Low            | Monitoring      |          |           |                |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column    | PII Level | Masking / Encryption Rule                      | Data Retention / Purge Policy |
| --------- | --------- | ---------------------------------------------- | ----------------------------- |
|           |           |                                                |                               |


---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
|     |          |           |       |        |
### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date | Decision | Rationale | Decided by |
| ---- | -------- | --------- | ---------- |
|      |          |           |            |
