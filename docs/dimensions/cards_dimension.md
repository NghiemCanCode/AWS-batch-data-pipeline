# Technical Specification: Cards Dimension

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

> **Purpose:** Mỗi lần giao dịch với bên bán đều có 1 phân loại
> **Primary consumers:** fact_transaction

| Attribute    | Value                    | Description |
| ------------ | ------------------------ | ----------- |
| SCD Type     | Type 2                   |             |
| Special type | None                     |             |
| Grain        | Mỗi dòng 1 thông tin thẻ |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value               | Description                                |
| ---------------- | ------------------- | ------------------------------------------ |
| Table name       | `gold.dim_customer` | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer            | Gold                |                                            |
| Source(s)        | `silver.cards`      |                                            |
| Load strategy    | Append              |                                            |
| Watermark column | `_updated_at`       |                                            |
| Frequency        |                     |                                            |
| Orchestrator     |                     |                                            |
| SLA              | None                |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 thông tin về card trong 1 khoảng thời gian
> **Natural Key (NK):** `card_id`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT cards.card_key)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source   | Dependency Type    | Note |
| -------------- | ------------------ | ---- |
| `silver.cards` | Hard (must finish) |      |
| `silver.users` | Soft (best effort) |      |

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

| Column Name           | Data Type          | Key Type | Not Null | Transformation Logic                                 | Null Handling             | Allowed Range / Sample                                  | Business Definition                                                                         |
| --------------------- | ------------------ | -------- | -------- | ---------------------------------------------------- | ------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `card_key`            | `StringType`       | PK, SK   | Y        | Surrogate Key Generation                             | Raise pipeline error      | —                                                       | Khóa thay thế định danh duy nhất của card.                                                  |
| `card_id`             | `StringType`       | NK       | Y        | `silver.cards.card_id`                               | Skip record + log warning | —                                                       | Mã định danh nghiệp vụ của card.                                                            |
| `customer_id`         | `StringType`       | NK       | N        | `silver.cards.client_id`                             | Skip record + log warning | —                                                       | Mỗi card chỉ được 1 người sở hữu trong khoảng thời gian -> Vẫn có thể lưu tại dimension này |
| `card_brand`          | `StringType`       |          | N        | `silver.cards.card_brand`                            | `UNKNOWN`                 | [`"VISA", "MASTERCARD", "AMEX", "DISCOVER", "UNKNOWN"`] | Loại card. Chưa cần tạo 1 dimension riêng                                                   |
| `mask_card_number`    | `StringType`       |          | N        | `silver.cards.mask_card_number`                      | `NULL` — không impute     | `******1234`                                            | Masks tất cả, ngoại trừ 4 số cuối                                                           |
| `expires_month`       | `ShortType`        |          | N        | Process `silver.cards.expires`                       | `NULL`                    | `1 - 12`                                                |                                                                                             |
| `expires_year`        | `ShortType`        |          | N        | Process `silver.cards.expires`                       | `NULL`                    |                                                         |                                                                                             |
| `has_a_cvv`           | `BooleanType`      |          | N        | `silver.cards.has_a_cvv`                             | `NULL`                    | `{yes, no}`                                             |                                                                                             |
| `has_chip`            | `BooleanType`      |          | N        | `silver.cards.has_chip`                              | `NULL`                    | `{yes, no}`                                             |                                                                                             |
| `num_card_issue`      | `ShortType`        |          | N        | `silver.cards.num_cards_issued`                      | `NULL`                    | `>= 0`                                                  |                                                                                             |
| `effective_from_date` | `TimestampNTZType` |          | Y        | Timestamp khi record được insert (UTC) - SCD key     | Raise pipeline error      |                                                         | UTC là yêu cầu nghiệp vụ                                                                    |
| `effective_to_date`   | `TimestampNTZType` |          | Y        | `9999-12-31` khi insert. Cập nhật khi có version mới | Raise pipeline error      |                                                         | UTC là yêu cầu nghiệp vụ                                                                    |
| `is_current`          | `BooleanType`<br>  |          | Y        | `true` khi insert, `false` khi expire                | Raise pipeline error      | `true`, `false`                                         |                                                                                             |
| `version_number`      | `ShortType`        |          | Y        | Bắt đầu từ `1`, tăng mỗi SCD merge                   | Raise pipeline error      | ≥ 1                                                     |                                                                                             |

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

| Attribute           | Value                          |
| ------------------- | ------------------------------ |
| Strategy            | Hash                           |
| Input columns       | `card_id, effective_from_date` |
| Policy              | Dùng `SHA-256`                 |
| Stability guarantee |                                |
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

| Column             | Trigger SCD2? | Why                                                  |
| ------------------ | ------------- | ---------------------------------------------------- |
| `has_chip`         | Yes           | Chip Availability là thuộc tính lịch sử cần theo dõi |
| `has_a_cvv`        | Yes           | CVV Availability là thuộc tính lịch sử cần theo dõi  |
| `expires_month`    | Yes           | Thuộc Expiration Information                         |
| `expires_year`     | Yes           | Thuộc Expiration Information                         |
| `num_card_issue`   | Yes           | Number of Card Reissues                              |
| `mask_card_number` | No            | Thay đổi thường là data correction                   |

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

| Check Name    | Target Column | Rule/Condition    | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| ------------- | ------------- | ----------------- | -------------- | -------- | --------- | -------------- | ------------- |
| PK uniqueness | mcc           | unique & not null | 0 - violations | Error    | per run   | fail + halt    |               |
|               |               |                   |                |          |           |                |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column             | PII Level | Masking / Encryption Rule                                | Data Retention / Purge Policy |
| ------------------ | --------- | -------------------------------------------------------- | ----------------------------- |
| `card_id`          | DI        | SHA-256 one-way hash trước khi write vào Gold.           |                               |
| `customer_id`      | DI        | SHA-256 one-way hash trước khi write vào Gold.           |                               |
| `mask_card_number` | NSA       | Đã được Obfuscated ở tầng `silver`, có dạng `******1234` |                               |
| `expires_month`    | QI        | Lake Formation column-level policy required.             |                               |
| `expires_year`     | QI        | Lake Formation column-level policy required.             |                               |

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
