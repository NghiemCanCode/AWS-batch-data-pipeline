# Technical Specification: Customers Dimension

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

> **Purpose:** Thông tin của khách hàng được lưu. Người dùng thẻ sẽ được đồng nhất thành customer.
> **Primary consumers:** fact_transaction

| Attribute    | Value                           | Description |
| ------------ | ------------------------------- | ----------- |
| SCD Type     | Type 2                          |             |
| Special type | None                            |             |
| Grain        | Mỗi dòng 1 thông tin người dùng |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                          | Description                                |
| ---------------- | ------------------------------ | ------------------------------------------ |
| Table name       | `gold.dim_customers`           | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                           |                                            |
| Source(s)        | `silver.users`                 |                                            |
| Load strategy    | Incremental (SCD Type 2 merge) |                                            |
| Watermark column | `_updated_at`                  |                                            |
| Frequency        |                                |                                            |
| Orchestrator     |                                |                                            |
| SLA              | None                           |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 thông tin về user trong 1 khoảng thời gian
> **Natural Key (NK):** ``
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT customers.customer_id)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source      | Dependency Type    | Note |
| ----------------- | ------------------ | ---- |
| `silver.customer` | Hard (must finish) |      |

### Downstream Consumers

| Type         | Name                     | Note |
| ------------ | ------------------------ | ---- |
| Table        | `gold.fact_transactions` |      |
| Table        |                          |      |
| BI Artifact  |                          |      |
| Data Product |                          |      |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name           | Data Type           | Key Type | Not Null | Transformation Logic                                 | Null Handling                                                                          | Allowed Range / Sample                                                     | Business Definition                                                                  |
| --------------------- | ------------------- | -------- | -------- | ---------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `customer_key`        | `StringType`        | PK, SK   | Y        | Surrogate Key Generation                             | Raise pipeline error                                                                   | —                                                                          | Khóa thay thế định danh duy nhất của customer.                                       |
| `customer_id`         | `StringType`        | NK       | Y        | `silver.users.user_id`                               | Skip record + log warning                                                              | —                                                                          | Mã định danh nghiệp vụ của customer.                                                 |
| `retirement_age`      | `ShortType`         |          | N        | `silver.users.retirement_age`                        | Skip record + log warning                                                              | `[40 - 80]`                                                                | Chưa có nghiệp vụ thực sự nên sẽ dùng khoảng này                                     |
| `gender`              | `StringType`        |          | N        | `silver.users.gender`                                | `UNKNOWN`                                                                              | [`"MALE", "FEMALE", "OTHER", "UNKNOWN"`]                                   |                                                                                      |
| `birth_year`          | `IntegerType`       |          | N        | `silver.users.birth_year`                            | `NULL`                                                                                 | `[1900 - 2100]`                                                            |                                                                                      |
| `address_key`         | `StringType`        | FK       | N        | FK → `gold.dim_location.location_key`                | Unknown nếu không join được — không drop record. Xem **6.2. Unknown / Default Member** |                                                                            |                                                                                      |
| `yearly_income`       | `DecimalType(18,2)` |          | N        | `silver.users.yearly_income`                         | `NULL`                                                                                 |                                                                            | Thu nhập mỗi năm của khách hàng này                                                  |
| `income_bracket`      | `StringType`        |          | N        | Derived từ `silver.users.yearly_income`              | Nếu `yearly_income` null → `"UNKNOWN"`                                                 | `"Low"` (< $60k), `"Middle"` ($60k–$180k), `"High"` (> $180k), `"Unknown"` | Pew Research                                                                         |
| `total_debt`          | `DecimalType(18,2)` |          | N        | `silver.users.total_debt`                            | `NULL`                                                                                 |                                                                            | Tổng dư nợ hiện tại của khách hàng, phản ánh tổng nghĩa vụ tài chính chưa thanh toán |
| `credit_score`        | `IntegerType`       |          | N        | `silver.users.credit_score`                          | `NULL`                                                                                 | `[300 - 850]`                                                              |                                                                                      |
| `num_credit_cards`    | `IntegerType`       |          | N        | `silver.users.num_credit_cards`                      | `NULL`                                                                                 |                                                                            | Dùng để truy vấn nhanh số lượng card                                                 |
| `effective_from_date` | `TimestampNTZType`  |          | Y        | Timestamp khi record được insert (UTC) - SCD key     | Raise pipeline error                                                                   |                                                                            | UTC là yêu cầu nghiệp vụ                                                             |
| `effective_to_date`   | `TimestampNTZType`  |          | Y        | `9999-12-31` khi insert. Cập nhật khi có version mới | Raise pipeline error                                                                   |                                                                            | UTC là yêu cầu nghiệp vụ                                                             |
| `is_current`          | `BooleanType`<br>   |          | Y        | `true` khi insert, `false` khi expire                | Raise pipeline error                                                                   | `true`, `false`                                                            |                                                                                      |
| `version_number`      | `ShortType`         |          | Y        | Bắt đầu từ `1`, tăng mỗi SCD merge                   | Raise pipeline error                                                                   | ≥ 1                                                                        |                                                                                      |

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

| Attribute           | Value                              |
| ------------------- | ---------------------------------- |
| Strategy            | Hash                               |
| Input columns       | `customer_id, effective_from_date` |
| Policy              | Dùng `SHA-256`                     |
| Stability guarantee |                                    |
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
|        |               |     |


---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                | Join Condition | Unmatched Key Handling      |
| -------------- | --------------------------- | -------------- | --------------------------- |
| `address_key`  | `gold.dim_geo.location_key` |                | Map to Unknown (`-1`) + log |


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

| Check Name    | Target Column | Rule/Condition    | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| ------------- | ------------- | ----------------- | -------------- | -------- | --------- | -------------- | ------------- |
| PK uniqueness |               | unique & not null | 0 - violations | Error    | per run   | fail + halt    |               |
|               |               |                   |                |          |           |                |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column           | PII Level | Masking / Encryption Rule                      | Data Retention / Purge Policy |
| ---------------- | --------- | ---------------------------------------------- | ----------------------------- |
| `customer_id`    | DI        | SHA-256 one-way hash trước khi write vào Gold. |                               |
| `gender`         | QI        | Lake Formation column-level policy required.   |                               |
| `income_bracket` | NSA       | Đã được bucket `yearly_income` để giảm PII     |                               |
| `birth_year`     | QI        | Lake Formation column-level policy required.   |                               |
| `address_key`    | NSA       | Đã chuẩn hóa về city + state                   |                               |
| `yearly_income`  | QI        | Lake Formation column-level policy required.   |                               |
| `total_debt`     | QI        | Lake Formation column-level policy required.   |                               |
| `credit_score`   | QI        | Lake Formation column-level policy required.   |                               |

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
