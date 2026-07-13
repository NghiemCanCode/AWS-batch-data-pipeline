# Technical Specification: Merchant Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    |               | <Draft \| In Review \| Approved \| Deprecated> |
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

> **Purpose:** Mỗi lần giao dịch với bên bán đều có 1 phân loại
> **Primary consumers:** fact_transaction

| Attribute    | Value                       | Description |
| ------------ | --------------------------- | ----------- |
| SCD Type     | Type 1                      |             |
| Special type | Lookup table                |             |
| Grain        | Mỗi dòng là 1 loại merchant |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value             | Description |
| ---------------- | ----------------- | ----------- |
| Table name       | gold.dim_merchant |             |
| Layer            | Gold              |             |
| Source(s)        | silver.mcc        |             |
| Load strategy    | Full load         |             |
| Watermark column | None              |             |
| Frequency        | Quarterly         |             |
| Orchestrator     |                   |             |
| SLA              | None              |             |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 loại nhóm merchant (merchant category)
> **Natural Key (NK):** `mcc`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT mcc.mcc)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source | Dependency Type    | Note |
| ------------ | ------------------ | ---- |
| `silver.mcc` | Hard (must finish) |      |

### Downstream Consumers

| Type         | Name                | Note |
| ------------ | ------------------- | ---- |
| Table        | `gold.transactions` |      |
| Table        |                     |      |
| BI Artifact  |                     |      |
| Data Product |                     |      |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name              | Data Type             | Key Type | Not Null | Transformation Logic       | Null Handling             | Allowed Range / Sample | Business Definition |
| ------------------------ | --------------------- | -------- | -------- | -------------------------- | ------------------------- | ---------------------- | ------------------- |
| `mcc`                    | `StringType` (PK, NK) |          | Y        | `silver.mcc.mcc`           | Raise pipeline error      | —                      |                     |
| `merchant_category_name` | `StringType`          |          | Y        | `silver.mcc.merchant_name` | Skip record + log warning | Default: `"UNKNOWN`    |                     |

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

> **Note:** Dimension này không có surrigate key

| Attribute           | Value |
| ------------------- | ----- |
| Strategy            |       |
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

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Dimension Table | Join Condition | Unmatched Key Handling |
| -------------- | ---------------------- | -------------- | ---------------------- |
| None           |                        |                |                        |


---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. ...

2. ...

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
| PK uniqueness | mcc           | unique & not null | 0 - violations | Error    | per run   | fail + halt    |               |


---
## 10. Security & Governance

| Column | PII Level | Masking / Encryption Rule | Data Retention / Purge Policy |
| ------ | --------- | ------------------------- | ----------------------------- |
| None   |           |                           |                               |

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
