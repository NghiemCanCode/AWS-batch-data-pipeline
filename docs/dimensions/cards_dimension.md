# Technical Specification: Cards Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.3       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change        |
| ------- | ---------- | ------------- | ------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại đầy đủ — sửa lỗi copy-paste (Purpose, table name, DQ check `mcc`), giải quyết mâu thuẫn `customer_id` null handling giữa §5.1/§7/code, cập nhật load strategy & watermark theo cơ chế snapshot thật (xem mục 11 Decision Log) |
| v.0.0.3 | 2026-07-23 | NghiemCanCode | Qua Q&A với Claude (đợt sửa point-in-time của `fact_transactions`): backdate `effective_from_date` của version 1 về `1900-01-01`; SK input đổi sang `dbt_valid_from` gốc của snapshot; thêm DQ checks interval no-overlap/coverage — đồng bộ với `customers_dimension.md` v.0.0.3 (xem Decision Log) |

---

## 1. Overview & Business Context

> **Purpose:** Lưu thông tin thẻ thanh toán của khách hàng theo từng phiên bản SCD2 (chip, CVV, hạn thẻ, số lần cấp lại). Mỗi giao dịch trong `fact_transactions` tham chiếu về đúng phiên bản thẻ tại thời điểm giao dịch.

> **Primary consumers:** `gold.fact_transactions`

| Attribute    | Value                                      | Description |
| ------------ | ------------------------------------------ | ----------- |
| SCD Type     | Type 2                                     |             |
| Special type | None                                       |             |
| Grain        | Mỗi dòng là 1 card theo 1 phiên bản SCD2   |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                            | Description                                |
| ---------------- | ------------------------------------------------ | ------------------------------------------ |
| Table name       | `gold.dim_cards`                                 | Thống nhất tên `dim_<tên object số nhiều>`. Sửa từ `gold.dim_customer` ở bản draft v.0.0.1 — lỗi copy-paste (xem Decision Log) |
| Layer            | Gold                                             |                                            |
| Source(s)        | `finance_silver.silver_cards` (qua `stg_cards`)  | Sửa từ `silver.cards` ở bản draft v.0.0.1 — tên source thật theo `sources.yml` (xem Decision Log) |
| Load strategy    | Incremental (SCD Type 2 qua dbt snapshot + merge) | `snapshot_cards` (strategy=`check`) tạo version, `dim_cards.sql` merge kết quả theo `card_key`. Sửa từ "Append" ở v.0.0.1 (xem Decision Log) |
| Watermark column | Không có cột `_updated_at` thật                  | 2 tầng: (1) snapshot dùng strategy=`check` — so sánh giá trị `check_cols`, không dùng timestamp; (2) incremental merge lọc theo `dbt_valid_from`/`dbt_valid_to` so với `max(effective_from_date)` của chính `gold.dim_cards` (xem Decision Log) |
| Frequency        |                                                  | Chưa quyết định ở cấp dự án, đồng nhất với các bảng gold khác (xem Open Question #2) |
| Orchestrator     |                                                  | Chưa quyết định ở cấp dự án                |
| SLA              | None                                             |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 phiên bản (version) của 1 card — version mới được tạo khi 1 trong các cột SCD2-tracked thay đổi (`has_chip`, `has_a_cvv`, `expires_month`, `expires_year`, `num_card_issue` — xem mục 6.4). 1 `card_id` có thể xuất hiện nhiều dòng nếu có nhiều version.
> **Natural Key (NK):** `card_id`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT cards.card_key)`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source                                    | Dependency Type    | Note |
| ----------------------------------------------- | ------------------ | ---- |
| `finance_silver.silver_cards` (qua `stg_cards`) | Hard (must finish) | Nguồn duy nhất của mọi thuộc tính card. |
| `gold.dim_customers`                            | Soft (best effort) | Chỉ cần cho relationship test `customer_id → dim_customers.customer_id` pass, không cần cho build. Sửa từ `silver.users` ở v.0.0.1 — pipeline không hề đọc `silver_users` (xem Decision Log). |

### Downstream Consumers

| Type         | Name                     | Note                                                                            |
| ------------ | ------------------------ | ------------------------------------------------------------------------------- |
| Table        | `gold.fact_transactions` | `card_key` dùng làm FK, join `WHERE dim_cards.is_current = true ON card_id`. Sửa từ `gold.transactions` ở v.0.0.1. |
| Table        |                          |                                                                                 |
| BI Artifact  |                          | Chưa xác định — xem Open Question #3                                            |
| Data Product |                          | Chưa xác định                                                                   |

--- 
## 5. Column Definitions

### 5.1. Columns

| Column Name           | Data Type          | Key Type | Not Null | Transformation Logic                                                       | Null Handling                                                                 | Allowed Range / Sample                                  | Business Definition                                                                          |
| --------------------- | ------------------ | -------- | -------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `card_key`            | `StringType`       | PK, SK   | Y        | Surrogate Key Generation — xem mục 6.1                                     | Raise pipeline error                                                          | —                                                       | Khóa thay thế định danh duy nhất của card (1 giá trị / version).                             |
| `card_id`             | `StringType`       | NK       | Y        | `silver_cards.card_id` (`stg_cards` filter `card_id is not null`)          | Skip record (filter tại staging)                                              | —                                                       | Mã định danh nghiệp vụ của card.                                                             |
| `customer_id`         | `StringType`       | FK       | N        | `silver_cards.client_id`                                                   | `NULL` passthrough — không skip, không remap (sửa từ "Skip record" ở v.0.0.1, xem Decision Log) | —                                                       | Business key của chủ sở hữu, tham chiếu `dim_customers.customer_id` (NK-to-NK, không phải surrogate key — xem mục 7). Mỗi card chỉ có 1 chủ sở hữu tại 1 thời điểm nên lưu được tại dimension này. |
| `card_brand`          | `StringType`       |          | N        | `upper(trim(silver_cards.card_brand))`, ngoài whitelist → `UNKNOWN`        | `UNKNOWN`                                                                     | [`"VISA", "MASTERCARD", "AMEX", "DISCOVER", "UNKNOWN"`] | Loại card (network). Chưa cần tạo 1 dimension riêng. Không SCD2-tracked (Type 1).            |
| `mask_card_number`    | `StringType`       |          | N        | `silver_cards.mask_card_number`                                            | `NULL` — không impute                                                         | `******1234`                                            | Masks tất cả, ngoại trừ 4 số cuối. Đã obfuscate ở tầng silver.                               |
| `expires_month`       | `ShortType`        |          | N        | `month(silver_cards.expires)`                                              | `NULL`                                                                        | `1 - 12`                                                | Tháng hết hạn thẻ.                                                                           |
| `expires_year`        | `ShortType`        |          | N        | `year(silver_cards.expires)`                                               | `NULL`                                                                        |                                                         | Năm hết hạn thẻ.                                                                             |
| `has_a_cvv`           | `BooleanType`      |          | N        | `silver_cards.has_a_cvv`                                                   | `NULL`                                                                        | `true`, `false`                                         | Thẻ có CVV hay không.                                                                        |
| `has_chip`            | `BooleanType`      |          | N        | `silver_cards.has_chip`                                                    | `NULL`                                                                        | `true`, `false`                                         | Thẻ có chip hay không.                                                                       |
| `num_card_issue`      | `ShortType`        |          | N        | `silver_cards.num_cards_issued`                                            | `NULL`                                                                        | `>= 0`                                                  | Số lần thẻ được cấp lại.                                                                     |
| `effective_from_date` | `TimestampNTZType` |          | Y        | Version 1 của mỗi `card_id`: **backdate về `1900-01-01`** (phục vụ as-of join của `fact_transactions`, xem Decision Log v.0.0.3). Version ≥ 2: `dbt_valid_from` của snapshot (UTC) - SCD key | Raise pipeline error                                                          |                                                         | UTC là yêu cầu nghiệp vụ.                                                                    |
| `effective_to_date`   | `TimestampNTZType` |          | Y        | `coalesce(dbt_valid_to, '9999-12-31 23:59:59')`. Cập nhật khi có version mới | Raise pipeline error                                                          |                                                         | UTC là yêu cầu nghiệp vụ.                                                                    |
| `is_current`          | `BooleanType`      |          | Y        | `dbt_valid_to is null` — `true` khi insert, `false` khi expire             | Raise pipeline error                                                          | `true`, `false`                                         |                                                                                              |
| `version_number`      | `ShortType`        |          | Y        | `row_number()` partition theo `card_id` order theo `dbt_valid_from` — bắt đầu từ `1`, tăng mỗi SCD merge | Raise pipeline error                                                          | ≥ 1                                                     |                                                                                              |

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

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`customers_dimension.md`, `trans_error_bridge.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value                          |
| ------------------- | ------------------------------ |
| Strategy            | Hash                           |
| Input columns       | `card_id, dbt_valid_from` (giá trị gốc của snapshot, **không phải** `effective_from_date` sau backdate — xem Decision Log v.0.0.3) |
| Policy              | Dùng `SHA-256`. **Code hiện tại (`dim_cards.sql`) dùng `md5`, chưa khớp policy này** — xem Decision Log / Open Question #4 (cùng open item với `dim_customers`). |
| Stability guarantee | Tái tạo được (deterministic) nếu `card_id` và `dbt_valid_from` không đổi; key độc lập với quy tắc backdate `effective_from_date` của version 1. Nếu `card_id` (business key) đổi ở nguồn, `card_key` cũng đổi theo — hiện chưa có xử lý cho trường hợp business key thay đổi. |
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member         | Key value | Khi nào dùng                    |
| -------------- | --------- | ------------------------------- |
| Unknown        | `-1`      | NK null / không tìm thấy parent — `fact_transactions` map `card_key` không resolve được về `-1`. |
| Not Applicable | `-2`      | Đã seed trong `dim_cards.sql` (initial build / full-refresh) nhưng `fact_transactions` hiện chỉ map về `-1`; giữ để đồng bộ convention toàn dự án. |

> Cả 2 member được seed trong `dim_cards.sql` ở lần build đầu / full-refresh; incremental merge giữ nguyên chúng ở các run sau. `customer_id` của 2 dòng seed = `'UNKNOWN'` / `'NOT APPLICABLE'`, khớp với member tương ứng đã seed ở `dim_customers` để relationship test pass không cần exclusion.
### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| --------------------------------- | ---- |
| Khi NK xuất hiện ở fact trước dim | N/A  |
| Placeholder attribute values      | N/A  |
| Back-update khi dim thật về       | N/A  |
| Cờ đánh dấu                       | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column             | Trigger SCD2? | Why                                                  |
| ------------------ | ------------- | ---------------------------------------------------- |
| `has_chip`         | Yes           | Chip Availability là thuộc tính lịch sử cần theo dõi |
| `has_a_cvv`        | Yes           | CVV Availability là thuộc tính lịch sử cần theo dõi  |
| `expires_month`    | Yes           | Thuộc Expiration Information                         |
| `expires_year`     | Yes           | Thuộc Expiration Information                         |
| `num_card_issue`   | Yes           | Number of Card Reissues                              |
| `mask_card_number` | No            | Thay đổi thường là data correction                   |
| `card_brand`       | No            | Một thẻ vật lý không tự đổi network — thay đổi ở nguồn gần như chỉ là data correction (Type 1). Bổ sung ở v.0.0.2, khớp `check_cols` của `snapshot_cards` (xem Decision Log). |
| `customer_id`      | No            | Không nằm trong `check_cols` — nếu card đổi chủ ở nguồn, thay đổi sẽ không được ghi nhận. Xem Open Question #1. |

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                     | Join Condition                                                                                     | Unmatched Key Handling                          |
| -------------- | -------------------------------- | -------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `customer_id`  | `gold.dim_customers.customer_id` | Business-key reference (NK-to-NK), không join lúc build — chỉ enforce qua dbt relationship test `dim_cards.customer_id → dim_customers.customer_id`. | Giữ `NULL` — không skip, không remap về `-1` (sửa từ "Map to Unknown `-1`" ở v.0.0.1, xem Decision Log). |

> Cột ở v.0.0.1 ghi là `client_id` — đó là tên cột ở source (`silver_cards.client_id`); trong `dim_cards` cột đã được rename thành `customer_id` tại `stg_cards`.

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. dbt snapshot `snapshot_cards` (strategy=`check` trên `has_chip`, `has_a_cvv`, `expires_month`, `expires_year`, `num_card_issue`) đọc `stg_cards`; đóng version cũ (set `dbt_valid_to`) và mở version mới khi 1 trong các cột check thay đổi.
2. `dim_cards.sql` đọc `snapshot_cards`, tính `version_number` bằng `row_number()` partition theo `card_id` order theo `dbt_valid_from`.
3. Backdate: dòng có `version_number = 1` set `effective_from_date = '1900-01-01'`; các version sau giữ `dbt_valid_from`. Surrogate key luôn hash từ `dbt_valid_from` gốc (mục 6.1).
4. Incremental filter: chỉ lấy các dòng có `dbt_valid_from` hoặc `dbt_valid_to` mới hơn `max(effective_from_date)` hiện có trong `gold.dim_cards` (so sánh dùng `dbt_valid_from` gốc, không dùng giá trị backdate — tránh version 1 của entity mới bị filter sót).
5. Merge (upsert) kết quả vào `gold.dim_cards` theo `card_key`. Lần build đầu / full-refresh: union thêm 2 dòng Unknown (`-1`) / Not Applicable (`-2`) — xem mục 6.2.

| Attribute           | Value |
| ------------------- | ----- |
| Merge / upsert keys | `card_key` |
| Idempotency         | Incremental filter theo `effective_from_date`/`effective_to_date` của chính bảng đích + merge theo `card_key` đảm bảo rerun/backfill không tạo duplicate. |
| Failure / retry     | Fail + halt khi vi phạm các Critical check (mục 9); an toàn để retry vì merge là idempotent. |

### 8.2 Backfill & Historical Load Strategy

> Full backfill toàn bộ lịch sử `silver_cards` hiện có, chạy 1 lần khi migrate, sau đó chuyển sang incremental — đồng bộ cách các bảng gold khác trong dự án đã backfill (`dim_geo`, `dim_customers`, `fact_transactions`).

---
## 9. Data Quality & Observability Checks

| Check Name              | Target Column         | Rule/Condition                                                  | Threshold    | Severity | Frequency | Action on Fail | Alert Channel |
| ----------------------- | --------------------- | --------------------------------------------------------------- | ------------ | -------- | --------- | -------------- | ------------- |
| PK uniqueness           | `card_key`            | unique & not null                                               | 0 violations | Critical | Per run   | fail + halt    |               |
| Not null                | `card_id`             | not null                                                        | 0 violations | Critical | Per run   | fail + halt    |               |
| Current card uniqueness | `is_current`          | Mỗi `card_id` chỉ có đúng 1 record `is_current = true`          | 0 violations | Critical | Per run   | fail + halt    | Implement tại `dbt/tests/dim_cards_current_uniqueness.sql` (2026-07-24) |
| Owner FK integrity      | `customer_id`         | relationship test → `dim_customers.customer_id` (NULL được bỏ qua theo chuẩn dbt) | 100% match   | Error    | Per run   | fail + halt    |               |
| Brand enum validity     | `card_brand`          | `accepted_values` [VISA, MASTERCARD, AMEX, DISCOVER, UNKNOWN]   | 0 unexpected | Warning  | Per run   | alert          |               |
| Expiry month range      | `expires_month`       | `dbt_utils.accepted_range` [1, 12]                              | 0 unexpected | Warning  | Per run   | alert          |               |
| CVV flag validity       | `has_a_cvv`           | `accepted_values` [true, false]                                 | 0 unexpected | Warning  | Per run   | alert          |               |
| Chip flag validity      | `has_chip`            | `accepted_values` [true, false]                                 | 0 unexpected | Warning  | Per run   | alert          |               |
| Reissue count range     | `num_card_issue`      | `dbt_utils.accepted_range` min 0                                | 0 unexpected | Warning  | Per run   | alert          |               |
| SCD2 date completeness  | `effective_from_date`, `effective_to_date` | not null                                   | 0 violations | Critical | Per run   | fail + halt    |               |
| SCD2 interval no-overlap | `effective_from_date`, `effective_to_date` | Mỗi `card_id`: không có 2 version nào có khoảng `[from, to)` chồng lấn — chồng lấn gây fan-out ở as-of join của `fact_transactions` | 0 violations | Critical | Per run | fail + halt |               |
| SCD2 interval coverage   | `effective_from_date`, `effective_to_date` | Mỗi `card_id`: các khoảng hiệu lực liền mạch từ `1900-01-01` (version 1 backdate) tới `9999-12-31`, không có gap — gap làm fact rơi nhầm về Unknown (`-1`) | 0 violations | Critical | Per run | fail + halt |               |
| Version number validity | `version_number`      | not null & `dbt_utils.accepted_range` min 1                     | 0 unexpected | Warning  | Per run   | alert          |               |

> Check đầu tiên ở v.0.0.1 target cột `mcc` — lỗi copy-paste từ spec merchant categories, đã thay bằng bảng trên (khớp tests trong `dim_cards.yml`).

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column             | PII Level | Masking / Encryption Rule                                | Data Retention / Purge Policy |
| ------------------ | --------- | -------------------------------------------------------- | ----------------------------- |
| `card_id`          | DI        | SHA-256 one-way hash trước khi write vào Gold. Chưa implement — TODO(security) trong `stg_cards.sql`. |                               |
| `customer_id`      | DI        | SHA-256 one-way hash trước khi write vào Gold. Chưa implement — cùng open item với `stg_customers.sql`. |                               |
| `mask_card_number` | NSA       | Đã được Obfuscated ở tầng `silver`, có dạng `******1234` |                               |
| `expires_month`    | QI        | Lake Formation column-level policy required.             |                               |
| `expires_year`     | QI        | Lake Formation column-level policy required.             |                               |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
| 1   | `snapshot_cards.check_cols` (dbt/snapshots/snapshot_cards.sql) không có `customer_id`, nên nếu 1 card đổi chủ sở hữu ở nguồn, `dim_cards` sẽ không bao giờ ghi nhận thay đổi đó — `customer_id` bị "đông cứng" ở giá trị lần đầu ingest. Cần kiểm tra dữ liệu thực tế xem có xảy ra trường hợp đổi chủ không; nếu có, cần thêm `customer_id` vào `check_cols` để trigger SCD2 version mới. Phát hiện trong lúc spec `card_owner_factless.md` (Open Question #2), nơi tạm giả định ownership cố định 1 card = 1 customer vĩnh viễn. | No | NghiemCanCode | Open |
| 2   | Frequency / Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án (đồng bộ các spec khác — cùng open item với `customers_dimension.md` #2). | No | NghiemCanCode | Open |
| 3   | Chưa xác định BI Artifact / Data Product cụ thể dùng trực tiếp `dim_cards`. | No | NghiemCanCode | Open |
| 4   | Surrogate key policy (mục 6.1) yêu cầu SHA-256 nhưng code hiện tại (`dim_cards.sql`, và cả `dim_customers.sql`) dùng `md5`. Quyết định giữ policy SHA-256 và đổi code sau — đổi thuật toán sẽ thay đổi toàn bộ `card_key` đã materialize, ảnh hưởng FK ở `fact_transactions`, cần một đợt migrate/backfill riêng có kế hoạch (cùng open item với `customers_dimension.md` #4). | No | NghiemCanCode | Open |
| 5   | `Data Retention / Purge Policy` (mục 10) chưa được điền cho bất kỳ cột nào — chưa có quyết định compliance ở cấp dự án (cùng open item với `customers_dimension.md` #5). | No | NghiemCanCode | Open |
| 6   | ~~Check "Current card uniqueness" (mục 9) chưa được implement thành dbt test~~ — đã implement tại `dbt/tests/dim_cards_current_uniqueness.sql` (2026-07-24, đợt code point-in-time v.0.0.3). Phần còn lại: `snapshot_cards.yml` (docs + tests cho snapshot) vẫn chưa có — TODO trong `snapshot_cards.sql`, chờ đợt Audit-1 staging test suite. | No | NghiemCanCode | Partially resolved |
| 7   | **Lịch sử SCD2 của dimension này vẫn chưa tích luỹ — nhưng lý do đã đổi (cập nhật 2026-07-24).** Bug relation cache đã **sửa xong và verify**: `snapshot_cards` nay phát `merge into`, `dbt_valid_from` giữ nguyên qua các run, nên `card_key` **đã ổn định** và ràng buộc "dimension phải build cùng lần chạy với fact" không còn. Thứ còn chặn là nguyên nhân độc lập: nguồn tĩnh, không có gì thay đổi cho `strategy='check'` phát hiện — nên mỗi `card_id` vẫn chỉ có 1 version, **toàn bộ test SCD2 của dimension này vẫn pass rỗng nghĩa**, và Open Question #1 (đổi chủ sở hữu card) vẫn không kiểm chứng được bằng dữ liệu. Xem `docs/known_issues/dbt_spark_relation_cache.md` §7 và Open Question #2 ở đó. | No | NghiemCanCode | Open (đổi phạm vi: nguồn tĩnh, không phải cache) |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision | Rationale | Decided by |
| ---------- | -------- | --------- | ---------- |
| 2026-07-23 | Purpose (mục 1) viết lại: "Lưu thông tin thẻ thanh toán của khách hàng theo từng phiên bản SCD2..." | Câu ở v.0.0.1 ("Mỗi lần giao dịch với bên bán đều có 1 phân loại") là copy-paste từ spec merchant categories, không mô tả dimension này. | NghiemCanCode |
| 2026-07-23 | Table name (mục 2) sửa `gold.dim_customer` → `gold.dim_cards` | Lỗi copy-paste ở bản draft v.0.0.1; code luôn build `dim_cards`. | NghiemCanCode |
| 2026-07-23 | Source (mục 2) sửa `silver.cards` → `finance_silver.silver_cards` (qua `stg_cards`); Load strategy sửa "Append" → Incremental SCD2 (snapshot + merge); Watermark: bỏ `_updated_at` (không tồn tại), ghi lại đúng cơ chế 2 tầng | Tên source thật theo `sources.yml`. Cơ chế load thật là `snapshot_cards` (strategy=`check`) + incremental merge theo `card_key` — mô tả "Append" và watermark `_updated_at` không đúng với code, cùng loại lỗi đã sửa ở `customers_dimension.md`. | NghiemCanCode |
| 2026-07-23 | `customer_id` null / không khớp `dim_customers`: chốt NULL passthrough — không skip record, không remap về `-1`; sửa cả §5.1 lẫn §7 cho khớp | Giải quyết mâu thuẫn 3 nguồn (§5.1 "skip record" vs §7 "map -1" vs code giữ NULL). Card không có chủ resolve được vẫn có giá trị cho fact (card_key vẫn resolve theo `card_id`); skip sẽ mất dữ liệu card, còn remap `-1` buộc sửa code và trộn lẫn "không có chủ" với seeded member. `dim_cards.yml` đã document NULL passthrough. Quyết định qua Q&A. | NghiemCanCode |
| 2026-07-23 | Upstream Dependencies (mục 4): bỏ `silver.users`, thay bằng `gold.dim_customers` (Soft); Downstream sửa `gold.transactions` → `gold.fact_transactions` | Pipeline `dim_cards` không hề đọc `silver_users`; liên hệ thật là relationship test tới `dim_customers` (chỉ cần cho test pass, không cần cho build → Soft). `gold.transactions` không tồn tại, tên thật là `fact_transactions`. Quyết định qua Q&A. | NghiemCanCode |
| 2026-07-23 | `card_brand` xác nhận Type 1 — không track SCD2, bổ sung dòng vào mục 6.4 | Một thẻ vật lý không tự đổi network; thay đổi ở nguồn gần như chỉ là data correction. Khớp code hiện tại (`card_brand` không nằm trong `check_cols` của `snapshot_cards`). Quyết định qua Q&A. | NghiemCanCode |
| 2026-07-23 | Bảng DQ checks (mục 9) viết lại toàn bộ theo tests trong `dim_cards.yml` | Check duy nhất ở v.0.0.1 target cột `mcc` — copy-paste từ spec merchant categories; `dim_cards` không có cột `mcc`. | NghiemCanCode |
| 2026-07-23 | Surrogate Key Policy (mục 6.1) giữ nguyên yêu cầu SHA-256; ghi nhận code hiện dùng `md5` là chưa khớp, chưa đổi code trong lần này | Cùng quyết định và rationale với `customers_dimension.md` — đổi hash làm thay đổi toàn bộ `card_key` đã materialize, ảnh hưởng FK ở `fact_transactions`, cần đợt migrate riêng. Xem Open Question #4. | NghiemCanCode |
| 2026-07-23 | Backfill = full historical load | Đồng bộ cách các bảng gold khác trong dự án đã backfill khi migrate. | NghiemCanCode |
| 2026-07-23 | Version 1 của mỗi `card_id` backdate `effective_from_date` về `1900-01-01` (v.0.0.3) | Phục vụ as-of range join của `fact_transactions` (point-in-time, business spec §5): dbt snapshot chỉ tracking từ lần chạy đầu, nếu không backdate thì mọi giao dịch trước mốc đó rơi về Unknown (`-1`). Limitation chấp nhận: thay đổi thuộc tính trước lần snapshot đầu không phục hồi được. Cùng quyết định với `customers_dimension.md` v.0.0.3. | NghiemCanCode |
| 2026-07-23 | SK input đổi từ `effective_from_date` → `dbt_valid_from` gốc của snapshot (v.0.0.3) | Giữ `card_key` deterministic và độc lập với quy tắc backdate. Đổi key một lần này an toàn vì đã chốt full-refresh cả chuỗi fact (xem `transactions_fact.md` Decision Log). | NghiemCanCode |
