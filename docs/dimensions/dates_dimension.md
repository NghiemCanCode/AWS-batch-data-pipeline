# Technical Specification: Dates Dimension

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ----------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.2       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-23    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                                                    |
| ------- | ---------- | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                                                                             |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại toàn bộ qua Q&A với Claude — nội dung draft cũ thực chất là spec của geo dimension bị đặt nhầm file (xem Decision Log), viết lại đúng theo `dim_dates.sql`/`dim_date.yml` |

---

## 1. Overview & Business Context

> **Purpose:** Cung cấp bảng lịch chuẩn hóa (calendar reference) để các fact table join theo `date_key`, phục vụ phân tích theo ngày/tuần/tháng/quý/năm và đánh dấu ngày lễ liên bang Mỹ (holiday) + cuối tuần cho BI/reporting.
> **Primary consumers:** `gold.fact_transactions`

| Attribute    | Value                                                            | Description |
| ------------ | ----------------------------------------------------------------- | ----------- |
| SCD Type     | Type 1                                                            |             |
| Special type | None                                                              |             |
| Grain        | Mỗi dòng là 1 ngày, phạm vi cố định 2010-01-01 → 2035-12-31       |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                                        | Description                                |
| ---------------- | ------------------------------------------------------------------------------- | ------------------------------------------ |
| Table name       | `gold.dim_dates`                                                              | Thống nhất tên `dim_<tên object số nhiều>`. **Lưu ý:** `dim_date.yml` khai báo `name: dim_date` (số ít), không khớp tên model thật (tên file `dim_dates.sql`, `ref('dim_dates')` trong `fact_transactions.sql`, và `dbt list` đều xác nhận model là `dim_dates`) — xem Decision Log / Open Question #1. |
| Layer            | Gold                                                                          |                                            |
| Source(s)        | Self-generated date spine (`dbt_utils`/`sequence`, var `dim_date_start_date`/`dim_date_end_date`) + seed `us_holidays` | Không có upstream table thật; seed `us_holidays` là Hard dependency (xem mục 4). |
| Load strategy    | Full Reload                                                                   | `materialized='table'` — ghi đè toàn bộ mỗi lần chạy, không incremental. |
| Watermark column | None                                                                           | Không áp dụng — full refresh, không dùng incremental filter. |
| Frequency        |                                                                                | Chưa quyết định ở cấp dự án — xem Open Question #2. |
| Orchestrator     |                                                                                | Chưa quyết định ở cấp dự án — xem Open Question #2. |
| SLA              | None                                                                          |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 ngày (`full_date`), phạm vi cố định từ `dim_date_start_date` đến `dim_date_end_date` (mặc định 2010-01-01 → 2035-12-31, xem `dbt_project.yml` vars).
> **Natural Key (NK):** `full_date`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT full_date) = COUNT(DISTINCT date_key)`
#### Lưu ý
> **Holiday calendar**: Seed tĩnh `us_holidays` (311 ngày lễ, 2010-2035), được tạo offline bằng `scripts/gold-dbt/generate_holidays_seed.py` (thư viện `holidays` US) rồi checked in dưới dạng csv. Pipeline lúc runtime chỉ `left join` vào seed này, không gọi lại thư viện `holidays`. Range của seed **phải luôn khớp** `dim_date_start_date`/`dim_date_end_date` — nếu lệch, các năm thiếu sẽ có `is_holiday = false` sai.
> **Fiscal**: Không áp dụng (calendar year)
---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source                              | Dependency Type    | Note |
| ------------------------------------------- | ------------------- | ---- |
| Self-generated date spine (`dim_date_start_date`/`dim_date_end_date` vars) | Hard (must finish) | Sinh `full_date`/`date_key` và các thuộc tính lịch (day_of_week, month, quarter, year...). |
| Seed `us_holidays`                          | Hard (must finish) | Join theo `full_date` để lấy `holiday_name`/`is_holiday`. |

### Downstream Consumers

| Type         | Name                      | Note |
| ------------ | ------------------------- | ---- |
| Table        | `gold.fact_transactions`  | FK `date_key`, left join; record không match `date_key` bị **drop khỏi fact** (xem mục 7 của spec `fact_transactions`), không map về Unknown. |
| BI Artifact  |                           | Chưa xác định — xem Open Question #3 |
| Data Product |                           | Chưa xác định — xem Open Question #3 |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name    | Data Type     | Key Type | Not Null | Transformation Logic                                             | Null Handling                                    | Allowed Range / Sample                              | Business Definition                                                          |
| --------------- | ------------- | -------- | -------- | -------------------------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------------- |
| `full_date`     | `DateType`    | NK       | Y        | Date spine `sequence(start_date, end_date, interval 1 day)`           | Raise pipeline error                               | `[2010-01-01, 2035-12-31]`                             | Ngày lịch — business key tự nhiên.                                                |
| `date_key`      | `IntegerType` | PK, SK   | Y        | `date_format(full_date, "yyyyMMdd").cast("int")`                      | Raise pipeline error                               | Ví dụ: `20240101`                                     | Smart key — readable trực tiếp, không cần join để biết ngày.                     |
| `day_of_week`   | `ShortType`   |          | Y        | `dayofweek(full_date)`                                                | N/A — deterministic từ `full_date`                 | `[1-7]` (Spark: 1=Sunday ... 7=Saturday)               |                                                                                   |
| `day_of_month`  | `ShortType`   |          | Y        | `dayofmonth(full_date)`                                               | N/A — deterministic từ `full_date`                 | `[1-31]`                                               |                                                                                   |
| `day_of_year`   | `ShortType`   |          | Y        | `dayofyear(full_date)`                                                | N/A — deterministic từ `full_date`                 | `[1-366]`                                              |                                                                                   |
| `week_of_year`  | `ShortType`   |          | Y        | `weekofyear(full_date)`                                               | N/A — deterministic từ `full_date`                 | `[1-53]`                                               |                                                                                   |
| `month`         | `ShortType`   |          | Y        | `month(full_date)`                                                    | N/A — deterministic từ `full_date`                 | `[1-12]`                                               |                                                                                   |
| `quarter`       | `ShortType`   |          | Y        | `quarter(full_date)`                                                  | N/A — deterministic từ `full_date`                 | `[1-4]`                                                |                                                                                   |
| `year`          | `ShortType`   |          | Y        | `year(full_date)`                                                     | N/A — deterministic từ `full_date`                 | `[2010-2035]`                                          |                                                                                   |
| `is_weekend`    | `BooleanType` |          | Y        | `dayofweek(full_date) in (1, 7)`                                      | N/A — deterministic từ `full_date`                 | `true`, `false`                                        | Thứ Bảy/Chủ Nhật.                                                                 |
| `holiday_name`  | `StringType`  |          | N        | Left join `us_holidays.holiday_name` theo `full_date`                 | `NULL` khi không phải ngày lễ — không impute      | Ví dụ: `"Christmas Day"`                              | Tên ngày lễ liên bang Mỹ, theo seed `us_holidays`.                               |
| `is_holiday`    | `BooleanType` |          | Y        | `holiday_name is not null`                                            | Raise pipeline error                               | `true`, `false`                                        |                                                                                   |

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

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `customers_dimension.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value                                                                |
| ------------------- | ------------------------------------------------------------------------ |
| Strategy            | Derived smart key (không phải hash)                                     |
| Input columns       | `full_date`                                                              |
| Policy              | `cast(date_format(full_date, "yyyyMMdd") as int)`                        |
| Stability guarantee | Deterministic, tái tạo được 100% từ `full_date` — không phụ thuộc thứ tự chạy, không có rủi ro hash collision. |
### 6.2. Unknown / Default Member
> Áp dụng cho tất cả các dimension nhằm đảm bảo fact table luôn duy trì referential integrity khi không thể resolve foreign key. Quy định này dành cho các bảng có foreign key tham chiếu đến dimension được nêu tên, không áp dụng cho chính dimension đó.

| Member  | Key value | Khi nào dùng |
| ------- | --------- | ------------ |
| None    | —         | `dim_dates` không có Unknown/Default member (khác `dim_geo`/`dim_customers` có `-1`/`-2`). `dim_dates` phủ toàn bộ range `2010-01-01`–`2035-12-31` nên gần như luôn resolve được; nếu `date_key` không match (ví dụ timestamp nguồn nằm ngoài range), `fact_transactions.sql` **drop record đó khỏi fact** (`where matched_date_key is not null`) thay vì map về Unknown. |

### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| --------------------------------- | ---- |
| Khi NK xuất hiện ở fact trước dim  | N/A — `dim_dates` được generate sẵn toàn bộ range 2010-2035 trước khi có dữ liệu fact nào. |
| Placeholder attribute values       | N/A  |
| Back-update khi dim thật về        | N/A  |
| Cờ đánh dấu                       | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | ------------- | --- |
| None   | —             | Type 1, full reload — không track lịch sử version cho dimension này. |

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table | Join Condition | Unmatched Key Handling |
| --------------- | ------------- | --------------- | ----------------------- |
| None            |               |                 |                         |

> `dim_dates` là self-generated, không có cột FK nào tham chiếu ra dimension khác. Chiều FK ngược lại (`fact_transactions.date_key` → `dim_dates.date_key`) thuộc về spec của `fact_transactions` (xem mục 4 ở trên để biết cách unmatched key được xử lý phía fact).

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Generate date spine từ `dbt_utils`/`sequence` (`var dim_date_start_date` → `dim_date_end_date`, mặc định `2010-01-01` → `2035-12-31`), explode thành 1 dòng / ngày (`full_date`).
2. Tính các thuộc tính lịch (`date_key`, `day_of_week`, `day_of_month`, `day_of_year`, `week_of_year`, `month`, `quarter`, `year`, `is_weekend`) trực tiếp từ `full_date` bằng Spark date functions.
3. Left join với seed `us_holidays` theo `full_date` để lấy `holiday_name`; suy ra `is_holiday = holiday_name is not null`.
4. Materialize full table — ghi đè toàn bộ `gold.dim_dates`, không merge/upsert.

| Attribute           | Value                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------- |
| Merge / upsert keys | N/A — full reload thay thế toàn bộ bảng mỗi lần chạy.                                       |
| Idempotency         | Có — cùng input (date range vars + seed `us_holidays`) luôn sinh kết quả giống hệt; full-refresh nên rerun an toàn. |
| Failure / retry     | Fail + halt nếu vi phạm PK uniqueness/not-null (mục 9); an toàn để retry vì là full reload.  |

### 8.2 Backfill & Historical Load Strategy

> Không áp dụng theo nghĩa thông thường — toàn bộ range 2010-2035 đã được generate sẵn trong 1 lần full-refresh, không có khái niệm "backfill dần theo lô". Muốn mở rộng range (ví dụ thêm năm 2036+) cần sửa `dim_date_start_date`/`dim_date_end_date` trong `dbt_project.yml` rồi chạy lại full-refresh.

---
## 9. Data Quality & Observability Checks

> Các check dưới đây phản ánh test đã khai báo trong `dim_date.yml`. **Lưu ý:** do naming mismatch ở mục 2 (`name: dim_date` không khớp model thật `dim_dates`), các test này **có thể chưa thực sự được gắn/chạy** trên model — cần verify (`dbt build --select dim_dates` + xem kết quả test) trước khi coi đây là check đã enforce (xem Open Question #1).

| Check Name                | Target Column   | Rule/Condition                                                      | Threshold          | Severity | Frequency | Action on Fail | Alert Channel |
| -------------------------- | --------------- | ------------------------------------------------------------------------ | -------------------- | -------- | --------- | -------------- | ------------- |
| PK uniqueness              | `date_key`      | unique & not null                                                        | 0 violations         | Critical | Per run   | fail + halt    |               |
| NK uniqueness              | `full_date`     | unique & not null                                                        | 0 violations         | Critical | Per run   | fail + halt    |               |
| Holiday flag completeness  | `is_holiday`    | not null                                                                 | 0 violations         | Warning  | Per run   | alert          |               |
| Date range coverage        | `full_date`     | `min(full_date)`/`max(full_date)` khớp `dim_date_start_date`/`dim_date_end_date` | 100% covered, no gaps | Low      | Per run   | monitoring     |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:**
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column         | PII Level | Masking / Encryption Rule | Data Retention / Purge Policy |
| --------------- | --------- | -------------------------- | -------------------------------- |
| Tất cả các cột  | NSA       | Không cần — dữ liệu lịch/holiday công khai, không chứa thông tin cá nhân. |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
| 1   | `dim_date.yml`/`dim_time.yml` khai báo `name: dim_date`/`dim_time` (số ít), không khớp tên model thật `dim_dates`/`dim_times` (số nhiều, theo tên file `.sql` và `ref()` trong `fact_transactions.sql`) — `dbt list` xác nhận model thật là `dim_dates`/`dim_times`. Các test/description trong 2 file yml này có thể không thực sự gắn được vào model. Cần verify bằng `dbt build --select dim_dates dim_times` + kiểm tra test có chạy không, rồi sửa `name:` trong yml nếu đúng là bug. | No | NghiemCanCode | Open |
| 2   | Frequency / Orchestrator (mục 2) chưa được quyết định ở cấp dự án. Quan sát thấy hiện tại `dim_dates` được chạy tay qua dòng lệnh bị comment trong `scripts/gold-dbt/deploy_gold_dbt_dev.sh` (`dbt run --select dim_date`), nhưng đây chưa phải quyết định chính thức. | No | NghiemCanCode | Open |
| 3   | Chưa xác định BI Artifact / Data Product cụ thể dùng trực tiếp `dim_dates`. | No | NghiemCanCode | Open |
| 4   | `Data Retention / Purge Policy` (mục 10) chưa được điền — chưa có quyết định compliance ở cấp dự án (đồng bộ tình trạng các spec khác). | No | NghiemCanCode | Open |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision | Rationale | Decided by |
| ---------- | -------- | --------- | ---------- |
| 2026-07-23 | Toàn bộ nội dung spec cũ (city/state/zip, table name `gold.dim_dates` nhưng SK là hash của city+state+zip) thực chất là nội dung của **geo dimension**, không phải date — bị đặt sai file. Viết lại toàn bộ spec này từ đầu dựa theo code thật (`dim_dates.sql`, `dim_date.yml`, seed `us_holidays.csv`, `fact_transactions.sql`/`.yml`). Nội dung geo không được migrate sang file riêng trong lần cập nhật này. | Tránh nhầm lẫn vĩnh viễn nếu tiếp tục điền vào nội dung sai; user xác nhận spec đã bị ghi đè và yêu cầu lấy lại từ code. | NghiemCanCode |
| 2026-07-23 | Table name giữ `gold.dim_dates` (số nhiều, khớp tên file/model thật/`ref()`); ghi nhận `dim_date.yml` đang khai báo `name: dim_date` không khớp là bug ở code, không đổi tên model theo yml sai. | `ref()` trong `fact_transactions.sql` và `dbt list` đều xác nhận model thật là `dim_dates`; tên trong yml/comment mới là cái sai. | NghiemCanCode |
| 2026-07-23 | Mục 6.1 Surrogate Key: sửa Strategy từ "Hash" → "Derived smart key" (`date_key = date_format(full_date, "yyyyMMdd") cast int`). | Khớp đúng code — đây là format trực tiếp, không phải hash. | NghiemCanCode |
| 2026-07-23 | Mục 3 Grain: Natural Key sửa từ "None" → `full_date`. | `full_date` là business key tự nhiên, unique, khớp 2 unique test (`full_date`, `date_key`) trong `dim_date.yml`. | NghiemCanCode |
| 2026-07-23 | Mục 4: thêm seed `us_holidays` làm Hard upstream dependency (trước đó ghi "None"). | `dim_dates.sql` left join trực tiếp vào seed này để lấy `holiday_name`/`is_holiday`. Seed được generate offline bằng script Python dùng thư viện `holidays`, sau đó checked in làm csv tĩnh; pipeline lúc runtime chỉ đọc seed, không gọi lại thư viện. | NghiemCanCode |
| 2026-07-23 | Mục 6.2: Không có Unknown/Default member cho `dim_dates` (khác `dim_geo`/`dim_customers` có `-1`/`-2`). | `dim_dates` phủ toàn bộ range 2010-2035; `date_key` không match sẽ bị drop khỏi `fact_transactions` (`where matched_date_key is not null`) thay vì map Unknown. | NghiemCanCode |
| 2026-07-23 | Mục 10: toàn bộ cột đánh PII Level = NSA, không cần masking/retention rule. | `dim_dates` chỉ chứa dữ liệu lịch/holiday công khai, không chứa thông tin cá nhân. | NghiemCanCode |
| 2026-07-23 | `dim_time` (từ `dim_times.sql`/`dim_time.yml`) không được gộp vào spec này — sẽ có spec riêng (`times_dimension.md`) sau, giữ convention 1 file / 1 dimension như `cards_dimension.md`, `customers_dimension.md`. | Giữ đồng nhất cấu trúc tài liệu dự án; `dim_date` và `dim_time` tuy cùng là static reference table nhưng là 2 model độc lập, không có FK lẫn nhau. | NghiemCanCode |
| 2026-07-24 | `dim_date_start_date` mở rộng `2015-01-01` → **`2010-01-01`**; seed `us_holidays` regen tương ứng (256 → 311 dòng) | **Bug mất dữ liệu**: giao dịch sớm nhất trong silver là `2010-01-01`, nhưng `dim_dates` chỉ phủ từ 2015 — vì `dim_dates` không có Unknown member, `fact_transactions` drop mọi dòng có `date_key` không resolve, nên **toàn bộ giao dịch 2010-2014 bị âm thầm loại khỏi fact**. Phát hiện 2026-07-24 khi test `trans_error_bridge_transaction_exists_in_fact` fail 104352 dòng (bridge không lọc theo date range nên giữ các giao dịch mà fact đã drop). Seed phải regen cùng lúc, nếu không 2010-2014 sẽ có `is_holiday = false` sai. | NghiemCanCode |
