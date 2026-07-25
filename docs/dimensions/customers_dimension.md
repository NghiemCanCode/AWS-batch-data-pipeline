# Technical Specification: Customers Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ----------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.3       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                              |
| ------- | ---------- | ------------- | ------------------------------------------------------------------------------------------------------ |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                                                        |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại đầy đủ qua Q&A với Claude — sửa lỗi/mâu thuẫn so với code hiện tại (xem mục 11 Decision Log), điền các mục còn trống |
| v.0.0.3 | 2026-07-23 | NghiemCanCode | Qua Q&A với Claude (đợt sửa point-in-time của `fact_transactions`): backdate `effective_from_date` của version 1 về `1900-01-01`; SK input đổi sang `dbt_valid_from` gốc của snapshot; thêm DQ checks interval no-overlap/coverage (xem Decision Log) |

---

## 1. Overview & Business Context

> **Purpose:** Thông tin của khách hàng được lưu. Người dùng thẻ sẽ được đồng nhất thành customer.
> **Primary consumers:** `gold.fact_transactions`

| Attribute    | Value                                          | Description |
| ------------ | ----------------------------------------------- | ----------- |
| SCD Type     | Type 2                                          |             |
| Special type | None                                            |             |
| Grain        | Mỗi dòng là 1 customer theo 1 phiên bản SCD2    |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                          | Description                                |
| ----------------- | ----------------------------------------------- | ------------------------------------------- |
| Table name        | `gold.dim_customers`                           | Thống nhất tên `dim_<tên object số nhiều>` |
| Layer              | Gold                                            |                                            |
| Source(s)          | `finance_silver.silver_users` (qua `stg_customers`) | Sửa từ `silver.users` ở bản draft v.0.0.1 — tên source thật theo `sources.yml` (xem Decision Log) |
| Load strategy      | Incremental (SCD Type 2 qua dbt snapshot + merge)   | `snapshot_customers` (strategy=`check`) tạo version, `dim_customers.sql` merge kết quả theo `customer_key` |
| Watermark column   | Không có cột `_updated_at` thật                | 2 tầng: (1) snapshot dùng strategy=`check` — so sánh giá trị `check_cols`, không dùng timestamp; (2) incremental merge lọc theo `dbt_valid_from`/`dbt_valid_to` so với `max(effective_from_date)` của chính `gold.dim_customers` (xem Decision Log) |
| Frequency          |                                                  | Chưa quyết định ở cấp dự án, đồng nhất với các bảng gold khác (xem Open Question #2) |
| Orchestrator       |                                                  | Chưa quyết định ở cấp dự án                |
| SLA                | None                                            |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| -------------------------- | --------- |
| Table Format                | `Iceberg` |
| Partitioning Columns        | None      |
| Z-Order / Clustering Keys   | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 phiên bản (version) của 1 customer — version mới được tạo khi `income_bracket`, `city` hoặc `state` thay đổi (xem mục 6.4). 1 `customer_id` có thể xuất hiện nhiều dòng nếu có nhiều version.
> **Natural Key (NK):** `customer_id`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT customers.customer_key)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source                          | Dependency Type    | Note |
| --------------------------------------- | ------------------- | ---- |
| `finance_silver.silver_users` (qua `stg_customers`) | Hard (must finish) | Nguồn duy nhất của mọi thuộc tính customer. Sửa từ `silver.customer` ở bản draft v.0.0.1 (xem Decision Log). |
| `gold.dim_geo`                          | Hard (must finish)  | `dim_customers.sql` join `dim_geo` để resolve `address_key` — `dim_geo` phải build xong trước (xem mục 7). |

### Downstream Consumers

| Type         | Name                     | Note                                                                                   |
| ------------ | ------------------------ | ----------------------------------------------------------------------------------------- |
| Table        | `gold.fact_transactions` | `customer_key` dùng làm FK, join `WHERE dim_customers.is_current = true ON client_id = customer_id`. |
| Table        |                          |                                                                                             |
| BI Artifact  |                          | Chưa xác định — xem Open Question #3                                                     |
| Data Product |                          | Chưa xác định                                                                             |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name           | Data Type           | Key Type | Not Null | Transformation Logic                                       | Null Handling                                                                          | Allowed Range / Sample                                                     | Business Definition                                                                  |
| ---------------------- | -------------------- | -------- | -------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `customer_key`         | `StringType`         | PK, SK   | Y        | Surrogate Key Generation — xem mục 6.1                        | Raise pipeline error                                                                      | —                                                                            | Khóa thay thế định danh duy nhất của customer (1 giá trị / version).                    |
| `customer_id`          | `StringType`         | NK       | Y        | `finance_silver.silver_users.user_id` (qua `stg_customers`)   | Skip record + log warning                                                                 | —                                                                            | Mã định danh nghiệp vụ của customer.                                                    |
| `retirement_age`       | `ShortType`          |          | N        | `silver_users.retirement_age`                                 | `NULL` — không impute (sửa từ "Skip record + log warning" ở v.0.0.1, xem Decision Log)   | `[40 - 80]`                                                                  | Chưa có nghiệp vụ thực sự nên sẽ dùng khoảng này.                                        |
| `gender`               | `StringType`         |          | N        | `silver_users.gender`                                          | `UNKNOWN`                                                                                  | [`"MALE", "FEMALE", "OTHER", "UNKNOWN"`]                                    |                                                                                            |
| `birth_year`           | `IntegerType`        |          | N        | `silver_users.birth_year`                                      | `NULL`                                                                                     | `[1900 - 2100]`                                                              |                                                                                            |
| `address_key`          | `StringType`         | FK       | N        | FK → `gold.dim_geo.location_key` (sửa từ `dim_location` ở v.0.0.1, xem Decision Log)      | Unknown (`-1`) nếu không join được — không drop record. Xem **6.2** và mục 7 (join condition thật). |                                                                               |                                                                                            |
| `yearly_income`        | `DecimalType(18,2)`  |          | N        | `silver_users.yearly_income`                                   | `NULL`                                                                                     |                                                                               | Thu nhập mỗi năm của khách hàng này.                                                     |
| `income_bracket`       | `StringType`         |          | N        | Derived từ `silver_users.yearly_income`. **SCD2-tracked** (mục 6.4). Category giữ **UPPER** theo convention toàn dự án. | Nếu `yearly_income` null → `"UNKNOWN"`                                                     | `"LOW"` (< $60k), `"MIDDLE"` ($60k–$180k), `"HIGH"` (> $180k), `"UNKNOWN"` | Pew Research bucket. Tồn tại để có 1 thuộc tính ổn định thay cho `yearly_income` khi track SCD2. |
| `total_debt`           | `DecimalType(18,2)`  |          | N        | `silver_users.total_debt`                                      | `NULL`                                                                                     |                                                                               | Tổng dư nợ hiện tại của khách hàng, phản ánh tổng nghĩa vụ tài chính chưa thanh toán.    |
| `credit_score`         | `IntegerType`        |          | N        | `silver_users.credit_score`                                    | `NULL`                                                                                     | `[300 - 850]`                                                                |                                                                                            |
| `num_credit_cards`     | `IntegerType`        |          | N        | `silver_users.num_credit_cards`                                | `NULL`                                                                                     |                                                                               | Dùng để truy vấn nhanh số lượng card.                                                    |
| `effective_from_date`  | `TimestampNTZType`   |          | Y        | Version 1 của mỗi `customer_id`: **backdate về `1900-01-01`** (trạng thái đầu tiên quan sát được đại diện cho quá khứ trước đó — phục vụ as-of join của `fact_transactions`, xem Decision Log v.0.0.3). Version ≥ 2: `dbt_valid_from` của snapshot (UTC) - SCD key | Raise pipeline error                                                                       |                                                                               | UTC là yêu cầu nghiệp vụ.                                                                |
| `effective_to_date`    | `TimestampNTZType`   |          | Y        | `9999-12-31` khi insert. Cập nhật khi có version mới           | Raise pipeline error                                                                       |                                                                               | UTC là yêu cầu nghiệp vụ.                                                                |
| `is_current`           | `BooleanType`        |          | Y        | `true` khi insert, `false` khi expire                          | Raise pipeline error                                                                       | `true`, `false`                                                              |                                                                                            |
| `version_number`       | `ShortType`          |          | Y        | Bắt đầu từ `1`, tăng mỗi SCD merge                              | Raise pipeline error                                                                       | ≥ 1                                                                          |                                                                                            |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
>
> **`birth_month`:** có mặt ở `stg_customers.sql` (lấy từ `silver_users.birth_month`) nhưng bị drop trước khi vào `dim_customers.sql` — không xuất hiện trong bảng cột trên. Chưa rõ đây là quyết định nghiệp vụ có chủ đích hay thiếu sót, xem Open Question #1.
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| -------------------- | -------------------- | ------------------- |
| Add new column        |                       |                       |
| Drop column            |                       |                       |
| Rename column          |                       |                       |
| Change data type      |                       |                       |
| Change nullability    |                       |                       |
| Reorder columns        |                       |                       |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `trans_error_bridge.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute            | Value                                                                    |
| ---------------------- | ---------------------------------------------------------------------------- |
| Strategy               | Hash                                                                         |
| Input columns          | `customer_id, dbt_valid_from` (giá trị gốc của snapshot, **không phải** `effective_from_date` sau backdate — xem Decision Log v.0.0.3) |
| Policy                 | Dùng `SHA-256`. **Code hiện tại (`dim_customers.sql`) dùng `md5`, chưa khớp policy này** — xem Decision Log / Open Question #4. |
| Stability guarantee    | Tái tạo được (deterministic) nếu `customer_id` và `dbt_valid_from` không đổi; key độc lập với quy tắc backdate `effective_from_date` của version 1. Nếu `customer_id` (business key) đổi ở nguồn, `customer_key` cũng đổi theo — hiện chưa có xử lý cho trường hợp business key thay đổi. |
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member         | Key value | Khi nào dùng                    |
| -------------- | --------- | ---------------------------------- |
| Unknown        | `-1`      | NK null / không tìm thấy parent   |
| Not Applicable | `-2`      | Hiện chưa dùng ở dimension này, giữ để đồng bộ convention toàn dự án |
### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| ------------------------------------ | ---- |
| Khi NK xuất hiện ở fact trước dim     | N/A  |
| Placeholder attribute values          | N/A  |
| Back-update khi dim thật về            | N/A  |
| Cờ đánh dấu                            | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column              | Trigger SCD2? | Why                                                                                     |
| --------------------- | --------------- | -------------------------------------------------------------------------------------------- |
| `income_bracket`      | Yes             | Bucket hoá `yearly_income` — đại diện phân khúc thu nhập, thay đổi có ý nghĩa nghiệp vụ.     |
| `city`                 | Yes             | Đổi địa chỉ → `address_key` đổi theo (join `dim_geo`, xem mục 7).                            |
| `state`                | Yes             | Tương tự `city`.                                                                              |
| `yearly_income`        | No              | Giá trị số thô, biến động nhỏ liên tục — track sẽ gây version explosion; đã có `income_bracket` đại diện (xem Decision Log). |
| `total_debt`           | No              | Tương tự `yearly_income`.                                                                     |
| `credit_score`         | No              | Tương tự `yearly_income`.                                                                     |
| `retirement_age`       | No              | Thuộc tính gần như tĩnh, không kỳ vọng đổi theo thời gian.                                    |
| `gender`               | No              | Thuộc tính gần như tĩnh.                                                                       |
| `birth_year`           | No              | Thuộc tính tĩnh.                                                                               |
| `num_credit_cards`     | No              | Giá trị đếm biến động thường xuyên, không cần lịch sử SCD2.                                   |

---
## 7. Relationship & FK Resolution

| FK Column Name  | Parent Table                 | Join Condition                                                                                          | Unmatched Key Handling      |
| ----------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------ |
| `address_key`     | `gold.dim_geo.location_key`    | `nullif(trim(customers.city), '') = dim_geo.city AND nullif(trim(customers.state), '') = dim_geo.state AND dim_geo.zip = 'UNKNOWN'` (silver.users không có zip thật, nên luôn khớp hàng `zip='UNKNOWN'` của `dim_geo`) | Map to Unknown (`-1`) + log    |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. dbt snapshot `snapshot_customers` (strategy=`check` trên `income_bracket`, `city`, `state`) đọc `stg_customers`; đóng version cũ (set `dbt_valid_to`) và mở version mới khi 1 trong 3 cột check thay đổi.
2. `dim_customers.sql` đọc `snapshot_customers`, tính `version_number` bằng `row_number()` partition theo `customer_id` order theo `dbt_valid_from`.
3. Backdate: dòng có `version_number = 1` set `effective_from_date = '1900-01-01'`; các version sau giữ `dbt_valid_from`. Surrogate key luôn hash từ `dbt_valid_from` gốc (mục 6.1).
4. Left join `gold.dim_geo` theo (`city`, `state`, `zip = 'UNKNOWN'`) để resolve `address_key`; không resolve được → Unknown (`-1`).
5. Incremental filter: chỉ lấy các dòng có `dbt_valid_from` hoặc `dbt_valid_to` mới hơn `max(effective_from_date)` hiện có trong `gold.dim_customers` (so sánh dùng `dbt_valid_from` gốc, không dùng giá trị backdate — tránh version 1 của entity mới bị filter sót).
6. Merge (upsert) kết quả vào `gold.dim_customers` theo `customer_key`.

| Attribute           | Value                                                                                          |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| Merge / upsert keys    | `customer_key`                                                                                       |
| Idempotency            | Incremental filter theo `effective_from_date`/`effective_to_date` của chính bảng đích + merge theo `customer_key` đảm bảo rerun/backfill không tạo duplicate. |
| Failure / retry        | Fail + halt khi vi phạm các Critical check (mục 9); an toàn để retry vì merge là idempotent.        |

### 8.2 Backfill & Historical Load Strategy

> Full backfill toàn bộ lịch sử `silver_users` hiện có, chạy 1 lần khi migrate, sau đó chuyển sang incremental — đồng bộ cách các bảng gold khác trong dự án đã backfill (`dim_geo`, `dim_cards`, `fact_transactions`).

---
## 9. Data Quality & Observability Checks

| Check Name                     | Target Column       | Rule/Condition                                                        | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| --------------------------------- | ---------------------- | -------------------------------------------------------------------------- | ---------------- | -------- | --------- | ------------------ | --------------- |
| PK uniqueness                     | `customer_key`         | unique & not null                                                          | 0 violations     | Critical | Per run   | fail + halt         |                 |
| Not null                          | `customer_id`          | not null                                                                    | 0 violations     | Critical | Per run   | fail + halt         |                 |
| Current customer uniqueness       | `is_current`            | Mỗi `customer_id` chỉ có đúng 1 record `is_current = true` — implement tại `dbt/tests/dim_customers_current_uniqueness.sql` (2026-07-24) | 0 violations     | Critical | Per run   | fail + halt         |                 |
| SCD2 interval no-overlap          | `effective_from_date`, `effective_to_date` | Mỗi `customer_id`: không có 2 version nào có khoảng `[from, to)` chồng lấn — chồng lấn gây fan-out ở as-of join của `fact_transactions` | 0 violations | Critical | Per run | fail + halt |                 |
| SCD2 interval coverage            | `effective_from_date`, `effective_to_date` | Mỗi `customer_id`: các khoảng hiệu lực liền mạch từ `1900-01-01` (version 1 backdate) tới `9999-12-31`, không có gap — gap làm fact rơi nhầm về Unknown (`-1`) | 0 violations | Critical | Per run | fail + halt |                 |
| Address FK integrity              | `address_key`           | 100% `address_key` tồn tại trong `dim_geo.location_key`                   | 100% match       | Error    | Per run   | fail + halt         |                 |
| Retirement age range              | `retirement_age`        | `dbt_utils.accepted_range` [40, 80]                                        | 0 unexpected     | Warning  | Per run   | alert               |                 |
| Gender enum validity              | `gender`                | `accepted_values` [MALE, FEMALE, OTHER, UNKNOWN]                          | 0 unexpected     | Warning  | Per run   | alert               |                 |
| Birth year range                  | `birth_year`            | `dbt_utils.accepted_range` [1900, 2100]                                    | 0 unexpected     | Warning  | Per run   | alert               |                 |
| Credit score range                | `credit_score`          | `dbt_utils.accepted_range` [300, 850]                                      | 0 unexpected     | Warning  | Per run   | alert               |                 |
| Income bracket enum validity      | `income_bracket`        | `accepted_values` [LOW, MIDDLE, HIGH, UNKNOWN]                            | 0 unexpected     | Warning  | Per run   | alert               |                 |
| Address FK coverage               | `address_key`           | `% address_key = '-1'`                                                     | < 10%            | Low      | Per run   | monitoring          |                 |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column           | PII Level | Masking / Encryption Rule                      | Data Retention / Purge Policy |
| ------------------ | --------- | --------------------------------------------------- | -------------------------------- |
| `customer_id`      | DI        | SHA-256 one-way hash trước khi write vào Gold. Chưa implement — TODO(security) trong `stg_customers.sql`. |                                   |
| `gender`            | QI        | Lake Formation column-level policy required.        |                                   |
| `income_bracket`    | NSA       | Đã được bucket `yearly_income` để giảm PII.         |                                   |
| `birth_year`        | QI        | Lake Formation column-level policy required.        |                                   |
| `address_key`       | NSA       | Đã chuẩn hóa về city + state — surrogate key, không tự thân là DI. |                                   |
| `yearly_income`     | QI        | Lake Formation column-level policy required.        |                                   |
| `total_debt`        | QI        | Lake Formation column-level policy required.        |                                   |
| `credit_score`      | QI        | Lake Formation column-level policy required.        |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question                                                                                                                                                          | Blocking? | Owner         | Status |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | --------------- | ------ |
| 1   | `birth_month` có mặt ở `stg_customers.sql` nhưng bị drop trước khi vào `dim_customers.sql`, và chưa được nhắc tới ở spec. Chưa rõ có chủ đích (business quyết định không cần) hay là thiếu sót cần bổ sung vào gold. | No        | NghiemCanCode  | Open   |
| 2   | Frequency / Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án (đồng bộ các spec khác).                                                        | No        | NghiemCanCode  | Open   |
| 3   | Chưa xác định BI Artifact / Data Product cụ thể dùng trực tiếp `dim_customers`.                                                                                    | No        | NghiemCanCode  | Open   |
| 4   | Surrogate key policy (mục 6.1) yêu cầu SHA-256 nhưng code hiện tại (`dim_customers.sql`, và cả `dim_cards.sql`) dùng `md5`. Quyết định giữ policy SHA-256 và đổi code sau — chưa thực hiện trong lần cập nhật spec này vì sẽ đổi giá trị `customer_key`/`card_key` đã materialize, ảnh hưởng FK ở `fact_transactions`, cần một đợt migrate/backfill riêng có kế hoạch. | No        | NghiemCanCode  | Open   |
| 5   | `Data Retention / Purge Policy` (mục 10) chưa được điền cho bất kỳ cột nào — chưa có quyết định compliance ở cấp dự án.                                            | No        | NghiemCanCode  | Open   |
| 6   | **Lịch sử SCD2 của dimension này vẫn chưa tích luỹ — nhưng lý do đã đổi (cập nhật 2026-07-24).** Bug relation cache đã **sửa xong và verify**: `snapshot_customers` nay phát `merge into`, `dbt_valid_from` giữ nguyên qua các run, nên `customer_key` **đã ổn định** và ràng buộc "dimension phải build cùng lần chạy với fact" không còn. Thứ còn chặn là nguyên nhân độc lập: nguồn `silver_users` tĩnh, không có gì thay đổi cho `strategy='check'` trên `['income_bracket','city','state']` phát hiện — nên mỗi `customer_id` vẫn chỉ có 1 version và **toàn bộ test SCD2 của dimension này vẫn pass rỗng nghĩa**. Xem `docs/known_issues/dbt_spark_relation_cache.md` §7 và Open Question #2 ở đó. | No | NghiemCanCode | Open (đổi phạm vi: nguồn tĩnh, không phải cache) |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision                                                                                          | Rationale                                                                                                                                          | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- |
| 2026-07-23 | `address_key` FK target (mục 5.1) sửa từ `gold.dim_location` → `gold.dim_geo`                        | `dim_location` chưa từng tồn tại trong project; lỗi copy-paste ở bản draft v.0.0.1. Section 7 và code đã luôn dùng `dim_geo`.                          | NghiemCanCode    |
| 2026-07-23 | Upstream Dependencies (mục 4): sửa `silver.customer` → `finance_silver.silver_users`; thêm `gold.dim_geo` là Hard dependency | Tên source thật theo `sources.yml`/`stg_customers.sql`. `dim_customers.sql` join cứng vào `dim_geo` để resolve `address_key` nên `dim_geo` phải build xong trước. | NghiemCanCode    |
| 2026-07-23 | Watermark column: bỏ `_updated_at` (không tồn tại), ghi lại đúng cơ chế 2 tầng (snapshot check-strategy + incremental filter theo `effective_from_date` của chính bảng đích) | Mô tả đúng cơ chế thật, tránh hiểu nhầm có cột `_updated_at`.                                                                                          | NghiemCanCode    |
| 2026-07-23 | `retirement_age` Null Handling: sửa "Skip record + log warning" → `NULL` passthrough                 | Code không có logic skip riêng cho cột này; đồng nhất với các cột optional khác (`birth_year`, `yearly_income`,...). Không có lý do nghiệp vụ để loại cả record chỉ vì thiếu tuổi nghỉ hưu. | NghiemCanCode    |
| 2026-07-23 | Grain (mục 3) làm rõ = 1 dòng / 1 customer / 1 SCD2 version; NK = `customer_id`; uniqueness test đổi sang `customer_key` | Khớp cấu trúc bảng thật: `customer_id` lặp lại nhiều dòng khi có nhiều version.                                                                        | NghiemCanCode    |
| 2026-07-23 | `snapshot_customers.check_cols` (mục 6.4) giới hạn còn `income_bracket`, `city`, `state` — bỏ `yearly_income`, `total_debt`, `credit_score` khỏi check_cols (code đã sửa) | Các giá trị số thô biến động nhỏ liên tục, track sẽ gây version explosion vô nghĩa cho dimension; `income_bracket` được tạo ra chính là để có 1 thuộc tính bucket ổn định thay thế. Trước đó comment code và check_cols thực tế mâu thuẫn nhau — đã đồng bộ lại. | NghiemCanCode    |
| 2026-07-23 | Surrogate Key Policy (mục 6.1) giữ nguyên yêu cầu SHA-256; ghi nhận code hiện dùng `md5` là chưa khớp, chưa đổi code trong lần này | Đổi thuật toán hash sẽ làm thay đổi toàn bộ `customer_key` đã materialize, ảnh hưởng ngược tới FK ở `fact_transactions` — cần một đợt migrate/backfill có kế hoạch riêng, không làm lẫn với việc hoàn thiện spec lần này. Xem Open Question #4. | NghiemCanCode    |
| 2026-07-23 | Backfill = full historical load                                                                       | Đồng bộ cách các bảng gold khác trong dự án đã backfill khi migrate.                                                                                    | NghiemCanCode    |
| 2026-07-23 | Version 1 của mỗi `customer_id` backdate `effective_from_date` về `1900-01-01` (v.0.0.3)              | Phục vụ as-of range join của `fact_transactions` (point-in-time, business spec §5): dbt snapshot chỉ tracking từ lần chạy đầu, nếu không backdate thì mọi giao dịch trước mốc đó rơi về Unknown (`-1`). Limitation chấp nhận: thay đổi thuộc tính trước lần snapshot đầu không phục hồi được. | NghiemCanCode    |
| 2026-07-23 | SK input đổi từ `effective_from_date` → `dbt_valid_from` gốc của snapshot (v.0.0.3)                   | Giữ `customer_key` deterministic và độc lập với quy tắc backdate — nếu hash từ giá trị sau backdate, key của version 1 sẽ đổi mỗi khi quy tắc backdate thay đổi. Việc đổi key một lần này an toàn vì đã chốt full-refresh cả chuỗi fact (xem `transactions_fact.md` Decision Log). | NghiemCanCode    |
