# Technical Specification: Time Dimension

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

| Attribute    | Value                         | Description |
| ------------ | ----------------------------- | ----------- |
| SCD Type     | Type 1                        |             |
| Special type |                               |             |
| Grain        | Mỗi dòng là 1 giây trong ngày |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                    | Description                                |
| ---------------- | ---------------------------------------- | ------------------------------------------ |
| Table name       | `gold.dim_times`                         | Thống nhất tên `dim_<tên object số nhiều>` |
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

| Column Name              | Data Type     | Key Type | Not Null | Transformation Logic                                                                                                                                                                                                                                                               | Null Handling             | Allowed Range / Sample                                                                                                                                                                                                                                                                                                                            | Business Definition                                                     |
| ------------------------ | ------------- | -------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `time_key`               | `IntegerType` | PK, SK   | Y        | `(F.floor(F.col(<time_point_col>) / F.lit(3600)) * F.lit(10000) + F.floor(((F.col(<time_point_col>) % F.lit(3600)) / F.lit(60))) * F.lit(100) + (F.col(<time_point_col>) % F.lit(60))).cast("int")`<br><br>Với `time_point_col` là 1 cột lần lượt có giá trị từ 0 tới (24x60x60)-1 | Raise pipeline error      | Ví dụ: `113300`,<br>Range: `0 - 23:59:59`, có gap với các giá trị không thuộc thời gian như `139967`                                                                                                                                                                                                                                              | Smart key — readable trực tiếp, không cần join để biết ngày             |
| `time_24h`               | `StringType`  |          | Y        | `F.concat( F.lpad(F.floor(F.col(<time_key>) / F.lit(10000)).cast("int"), 2, "0"), F.lit(":"), F.lpad( F.floor(((F.col(time_key) % F.lit(10000)) / F.lit(100))).cast("int"), 2, "0", ), F.lit(":"), F.lpad((F.col(time_key) % F.lit(100)).cast("int"), 2, "0"), )`                  | `'00:00:00' - '23:59:59'` | Có xuất hiện giá trị `'ONLINE'`                                                                                                                                                                                                                                                                                                                   |                                                                         |
| `hour_24`                | `ShortType`   |          | N        | `F.floor(F.col(<time_point_col>) / F.lit(10000)).cast("short")`                                                                                                                                                                                                                    |                           | `0 - 23`                                                                                                                                                                                                                                                                                                                                          | Do giá trị lớn nhất của thời gian chỉ tới `23:59:59`nên không phải `24` |
| `hour_12`                | `ShortType`   |          | N        | `(F.when( F.floor(F.col(<time_point_col>) / F.lit(10000)) % F.lit(12) == F.lit(0), F.lit(12),) .otherwise(F.floor(F.col(<time_point_col>) / F.lit(10000)) % F.lit(12)).cast("short"))`                                                                                             |                           | `0 - 11`                                                                                                                                                                                                                                                                                                                                          | Tương tự `hour_24`                                                      |
| `am_pm`                  | `StringType`  |          | N        | `F.when( F.floor(F.col(column_name) / F.lit(10000)) < F.lit(12),"AM").otherwise("PM")`                                                                                                                                                                                             |                           | `{AM,PM}`                                                                                                                                                                                                                                                                                                                                         |                                                                         |
| `minute`                 | `ShortType`   |          | N        | `F.floor((F.col(column_name) % F.lit(10000)) / F.lit(100)).cast("short")`                                                                                                                                                                                                          |                           | `0 - 59`                                                                                                                                                                                                                                                                                                                                          |                                                                         |
| `second`                 | `ShortType`   |          | N        | `(F.col(column_name) % F.lit(100)).cast("short")`                                                                                                                                                                                                                                  |                           | `0 - 59`                                                                                                                                                                                                                                                                                                                                          |                                                                         |
| `time_bucket_15min`      | `ShortType`   |          | N        | Process thông qua hàm `_calculate_time_bucket_col(<time_key>, 15)`                                                                                                                                                                                                                 |                           | Ví dụ:<br>- `1200`<br>- `2315`                                                                                                                                                                                                                                                                                                                    | Hiển thị dạng smart key, lượt bỏ phần giây                              |
| `time_bucket_30min`      | `ShortType`   |          | N        | Process thông qua hàm `_calculate_time_bucket_col(<time_key>, 30)`                                                                                                                                                                                                                 |                           | Ví dụ:<br>- `1230`<br>- `2300`                                                                                                                                                                                                                                                                                                                    | Hiển thị dạng smart key, lượt bỏ phần giây                              |
| `time_bucket_hourly`     | `ShortType`   |          | N        | Process thông qua hàm `_calculate_time_bucket_col(<time_key>, 60)`                                                                                                                                                                                                                 |                           | Ví dụ:<br>- `1200`<br>- `2300`                                                                                                                                                                                                                                                                                                                    | Hiển thị dạng smart key, lượt bỏ phần giây                              |
| `time_bucket_15min_str`  | `StringType`  |          | N        | Process thông qua hàm `_display_bucket_time_col(<time_bucket_*>)`                                                                                                                                                                                                                  |                           | Ví dụ:<br>- `12:00:00`<br>- `23:15:00`                                                                                                                                                                                                                                                                                                            | Hiển thị đầy đủ, kiểu dữ liệu `String`                                  |
| `time_bucket_hourly_str` | `StringType`  |          | N        | Process thông qua hàm `_display_bucket_time_col(<time_bucket_*>)`                                                                                                                                                                                                                  |                           | Ví dụ:<br>- `12:00:00`<br>- `23:00:00`                                                                                                                                                                                                                                                                                                            | Hiển thị đầy đủ, kiểu dữ liệu `String`                                  |
| `day_part`               | `StringType`  |          | N        | Process thông qua hàm `_calculate_day_part_col(<time_key>)`                                                                                                                                                                                                                        |                           | `{'Early Night', 'Morning', 'Afternoon','Evening','Night'}`<br>- `'Early Night': between(0, 55959)`<br>- `'Morning': between(60000, 115959)`<br>- `'Afternoon': between(120000, 175959)`<br>- `'Evening': between(180000, 215959)`<br>- `'Night': between(220000, 235959)`<br>Các giá trị không thuộc range này => `time_key` bị lỗi => `'Error'` |                                                                         |

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

| Attribute           | Value                   |
| ------------------- | ----------------------- |
| Strategy            | Smart key               |
| Input columns       | `city, state, zip`      |
| Policy              | Dùng smart key function |
| Stability guarantee |                         |

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
