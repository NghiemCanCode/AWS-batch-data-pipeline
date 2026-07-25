# Technical Specification: Merchant Category Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    |               | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.2       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-24    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                                                                                                                          |
| ------- | ---------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                                                                                                                                                     |
| v.0.0.2 | 2026-07-24 | NghiemCanCode | Đổi tiêu đề "Merchant Dimension" → "Merchant Category Dimension" cho khớp grain thật (MCC) và khớp business spec §8 sau Decision #21. Nội dung §1/§3 (Type 1, lookup, grain = 1 loại merchant) không đổi. |

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
| Table name       | gold.dim_merchant | Tên bảng vật lý giữ nguyên `dim_merchant` dù grain là merchant **category** — đổi tên sẽ tạo bảng orphan trên Glue và phải sửa mọi `ref()` downstream. Xem Decision Log bên dưới. |
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

| Date       | Decision                                                                                                          | Rationale                                                                                                                                                                                                                                                                                              | Decided by    |
| ---------- | ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| 2026-07-24 | Dimension này là **merchant category (MCC)**, không phải merchant. Không xây dimension ở grain `merchant_id`.        | Business spec §8 trước đây ghi "Merchant Dimension (SCD Type 2)" — không khớp source lẫn model đã implement. `silver_transactions` chỉ có `merchant_id` + city/state/zip của chính giao dịch, không có thuộc tính merchant nào biến đổi theo thời gian ⇒ SCD2 sẽ chỉ có đúng 1 version mỗi merchant mãi mãi. Đã sửa business spec (Decision #21). | NghiemCanCode |
| 2026-07-24 | Giữ tên bảng vật lý `gold.dim_merchant`, chỉ sửa tài liệu.                                                          | Đổi sang `dim_merchant_category` phải sửa `ref()` ở `fact_transactions`, relationships test ở `fact_transactions.yml` và `fact_daily_transaction_trend.yml`, deploy script, đồng thời để lại bảng orphan trên Glue phải drop tay — chi phí không tương xứng với lợi ích đặt tên.                              | NghiemCanCode |
