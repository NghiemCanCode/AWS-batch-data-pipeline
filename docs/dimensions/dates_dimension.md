# Technical Specification: Dates Dimension

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

> **Purpose:** 
> **Primary consumers:** fact_transaction

| Attribute    | Value              | Description |
| ------------ | ------------------ | ----------- |
| SCD Type     | Type 1             |             |
| Special type |                    |             |
| Grain        | Mỗi dòng là 1 ngày |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                    | Description                                |
| ---------------- | ---------------------------------------- | ------------------------------------------ |
| Table name       | `gold.dim_dates`                         | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                                     |                                            |
| Source(s)        | Self-generated (không có upstream table) |                                            |
| Load strategy    | Full Reload                              |                                            |
| Watermark column |                                          |                                            |
| Frequency        | Almost None                              |                                            |
| Orchestrator     |                                          |                                            |
| SLA              | None                                     |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 ngày.
> **Natural Key (NK):** None
> **Uniqueness test:** 
#### Lưu ý 
> **Holiday calendar**: U.S. Federal Holidays (`holidays.US` Python library)
> **Fiscal**: Không áp dụng (calendar year)
---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source   | Dependency Type    | Note |
| -------------- | ------------------ | ---- |
| None           |                    |      |

### Downstream Consumers

| Type         | Name | Note |
| ------------ | ---- | ---- |
| Table        |      |      |
| BI Artifact  |      |      |
| Data Product |      |      |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name | Data Type     | Key Type | Not Null | Transformation Logic                                                         | Null Handling             | Allowed Range / Sample          | Business Definition                                                    |
| ----------- | ------------- | -------- | -------- | ---------------------------------------------------------------------------- | ------------------------- | ------------------------------- | ---------------------------------------------------------------------- |
| `date_key`  | `IntegerType` | PK, SK   | Y        | `date_format(full_date, "yyyyMMdd").cast("int")`                             | Raise pipeline error      | Ví dụ: `20240101`               | Smart key — readable trực tiếp, không cần join để biết ngày            |
| `city`      | `StringType`  |          | Y        | Case 1: `silver.transactions.merchant_city`<br>Case 2: `silver.users.city`   | Skip record + log warning | Có xuất hiện giá trị `'ONLINE'` |                                                                        |
| `state`     | `StringType`  |          | Y        | Case 1: `silver.transactions.merchant_state`<br>Case 2: `silver.users.state` | Skip record + log warning | —                               |                                                                        |
| `zip`       | `StringType`  |          | Y        | Case 1: `silver.transactions.zip`<br>Case 2: `silver.users.zip`              | `'UNKNOWN'`               |                                 | Hiện tại `silver.users` chưa enrich field zip -> dùng giá trị Default. |

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
| Stability guarantee |                    |
### 6.2. Unknown / Default Member
> Áp dụng cho tất cả các dimension nhằm đảm bảo fact table luôn duy trì referential integrity khi không thể resolve foreign key. Quy định này dành cho các bảng có foreign key tham chiếu đến dimension được nêu tên, không áp dụng cho chính dimension đó.

| Member  | Key value  | Khi nào dùng                   |
| ------- | ---------- | ------------------------------ |
| Unknown | Nội suy SK | Bảng `dim_date` thiếu ngày này |

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

| FK Column Name | Parent Table | Join Condition | Unmatched Key Handling |
| -------------- | ------------ | -------------- | ---------------------- |
| None           |              |                |                        |



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
