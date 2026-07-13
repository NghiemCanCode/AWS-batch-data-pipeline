# Technical Specification: Account Owner Factless

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

> **Purpose:** Bảng này không đo lường sự kiện — nó **capture một mối quan hệ**: khách hàng nào sở hữu tài khoản nào, và trong khoảng thời gian nào. Câu hỏi nghiệp vụ mà bảng này trả lời:
> - *"Account này đang thuộc về customer nào?"*
> - *"Customer này đang sở hữu những account nào?"*
> - *"Tại thời điểm T, customer X có sở hữu account Y không?"*
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

> **Grain:** Mỗi row là **một khoảng thời gian chồng lấp (temporal overlap)** giữa một version customer và một version card . Grain được xác định bởi `(customer_key, account_key, valid_from_date, valid_to_date)`.
> **Primary Key:** 
> **Uniqueness test:** 

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

| Column Name       | Data Type          | Key Type | Not Null | Transformation Logic                                                         | Null Handling | Allowed Range / Sample | Business Definition |
| ----------------- | ------------------ | -------- | -------- | ---------------------------------------------------------------------------- | ------------- | ---------------------- | ------------------- |
| `customer_key`    | `StringType`       | FK       | Y        | `gold.dim_customers.customer_key`                                            |               |                        |                     |
| `card_key`        | `StringType`       | FK       | Y        | `gold.dim_cards.card_key`                                                    |               |                        |                     |
| `valid_from_date` | `TimestampNTZType` |          | Y        | `GREATEST(dim_customers.effective_from_date, dim_cards.effective_from_date)` |               |                        |                     |
| `valid_to_date`   | `TimestampNTZType` |          | Y        | `LEAST(dim_customers.effective_from_date, dim_cards.effective_from_date)`    |               |                        |                     |
| `is_active`       | `BooleanType`      |          | Y        | `dim_customers.is_current AND dim_cards.is_current`                          |               |                        |                     |

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

| Member | Key value | Khi nào dùng |
| ------ | --------- | ------------ |
| None   |           |              |

### 6.3. Special Type Handling
> **Special Type:** None

| Aspect | Rule |
| ------ | ---- |
|        |      |

### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | ------------- | --- |
| None   |               |     |


---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                    | Join Condition | Unmatched Key Handling      |
| -------------- | ------------------------------- | -------------- | --------------------------- |



---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. 
2. 
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
