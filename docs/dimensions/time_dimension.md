# Technical Specification: Time Dimension

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

| Version | Date       | Author        | Change                                                                                                                                                                          |
| ------- | ---------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft                                                                                                                                                                    |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại qua Q&A với Claude — draft cũ có nhiều đoạn leftover copy-paste từ dim_dates/dim_geo (xem Decision Log), viết lại đúng theo `dim_times.sql`/`dim_times.yml`; đồng thời phát hiện và fix 1 bug trong `fact_transactions.sql` (dead join tới `dim_times`). |

---

## 1. Overview & Business Context

> **Purpose:** Cung cấp bảng tham chiếu giờ chuẩn hóa (time-of-day reference) để các fact table join theo `time_key`, phục vụ phân tích theo giờ/phút/giây, khung giờ (15/30/60 phút) và phần ngày (`day_part`) cho BI/reporting.
> **Primary consumers:** `gold.fact_transactions`

| Attribute    | Value                                                              | Description |
| ------------ | ------------------------------------------------------------------- | ----------- |
| SCD Type     | Type 1                                                              |             |
| Special type | None                                                                |             |
| Grain        | Mỗi dòng là 1 giây trong ngày, cố định 86,400 dòng (00:00:00–23:59:59) |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                     | Description                                |
| ---------------- | ------------------------------------------ | ------------------------------------------ |
| Table name       | `gold.dim_times`                          | Thống nhất tên `dim_<tên object số nhiều>`. **Khác `dim_dates`:** `dim_times.yml` đã khai báo đúng `name: dim_times`, không có bug naming mismatch (xem Decision Log). |
| Layer            | Gold                                      |                                            |
| Source(s)        | Self-generated second-of-day spine (không có upstream table) | `explode(sequence(0, 86399))`.            |
| Load strategy    | Full Reload                               | `materialized='table'` — ghi đè toàn bộ mỗi lần chạy, không incremental. |
| Watermark column | None                                      | Không áp dụng — full refresh, không dùng incremental filter. |
| Frequency        |                                            | Chưa quyết định ở cấp dự án — xem Open Question #1. |
| Orchestrator     |                                            | Chưa quyết định ở cấp dự án — xem Open Question #1. |
| SLA              | None                                      |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| ------------------------- | --------- |
| Table Format              | `Iceberg` |
| Partitioning Columns      | None      |
| Z-Order / Clustering Keys | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 giây trong ngày, phạm vi cố định từ giây 0 (00:00:00) đến giây 86399 (23:59:59), tổng 86,400 dòng.
> **Natural Key (NK):** None — không có cột NK riêng biệt được expose ra ngoài (khác `dim_dates` có `full_date`). `time_key` vừa là smart surrogate key vừa là giá trị duy nhất định danh dòng, sinh trực tiếp từ `second_of_day` (cột nội bộ, không xuất hiện trong output cuối).
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT time_key) = 86400`
#### Lưu ý
> **Holiday calendar**: Không áp dụng cho `dim_times` — holiday là thuộc tính của `dim_dates`, không phải time-of-day.
> **Fiscal**: Không áp dụng.
---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source   | Dependency Type    | Note |
| -------------- | ------------------- | ---- |
| None           |                     | Self-generated, không có upstream table thật. |

### Downstream Consumers

| Type         | Name                     | Note |
| ------------ | ------------------------ | ---- |
| Table        | `gold.fact_transactions` | FK `time_key`. Không còn runtime join tới `dim_times` trong `fact_transactions.sql` (xem Decision Log) — FK integrity được enforce độc lập qua dbt `relationships` test trong `fact_transactions.yml`. |
| BI Artifact  |                          | Chưa xác định — xem Open Question #2. |
| Data Product |                          | Chưa xác định — xem Open Question #2. |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name              | Data Type     | Key Type | Not Null | Transformation Logic                                                                                                                                                                                        | Null Handling                      | Allowed Range / Sample                                                                                                                                                                                                                     | Business Definition                                                                            |
| ------------------------- | ------------- | -------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `time_key`                | `IntegerType` | PK, SK   | Y        | `floor(second_of_day / 3600) * 10000 + floor(mod(second_of_day, 3600) / 60) * 100 + mod(second_of_day, 60)`, với `second_of_day` chạy từ `0` đến `86399` (`explode(sequence(0, 86399))`)                     | Raise pipeline error                | Range hợp lệ: `0 - 235959` (dạng `HHMMSS`, vd `113300` = 11:33:00). Vì kiểu dữ liệu là Integer nên về kỹ thuật cho phép cả giá trị không tương ứng thời gian thật (vd `139967`, do `MM=99`/`SS=67` > 59) — nhưng trong `dim_times` mọi `time_key` đều sinh từ `second_of_day` hợp lệ nên không bao giờ có gap này; `time_key` từ fact (qua `date_format(timestamp,'HHmmss')`) cũng luôn hợp lệ vì bắt nguồn từ timestamp thật. | Smart key — readable trực tiếp, không cần join để biết giờ.                                       |
| `time_24h`                | `StringType`  |          | Y        | `concat(lpad(hour_24,2,'0'), ':', lpad(minute,2,'0'), ':', lpad(second,2,'0'))`                                                                                                                                | Raise pipeline error (deterministic từ `time_key`) | `'00:00:00' - '23:59:59'`                                                                                                                                                                                                                        | Display string đầy đủ giờ:phút:giây, từ `time_key`.                                              |
| `hour_24`                 | `ShortType`   |          | Y        | `floor(time_key / 10000)`                                                                                                                                                                                      | Raise pipeline error (deterministic) | `0 - 23`                                                                                                                                                                                                                                          | Do giá trị lớn nhất của thời gian chỉ tới `23:59:59` nên không có `24`.                          |
| `hour_12`                 | `ShortType`   |          | Y        | `case when mod(hour_24, 12) = 0 then 12 else mod(hour_24, 12) end`                                                                                                                                             | Raise pipeline error (deterministic) | `1 - 12`                                                                                                                                                                                                                                          | Quy ước 12h chuẩn: 0h và 12h đều map về `12` (không có `0`).                                     |
| `am_pm`                   | `StringType`  |          | Y        | `case when hour_24 < 12 then 'AM' else 'PM' end`                                                                                                                                                               | Raise pipeline error (deterministic) | `{AM, PM}`                                                                                                                                                                                                                                        |                                                                                                    |
| `minute`                  | `ShortType`   |          | N        | `floor(mod(time_key, 10000) / 100)`                                                                                                                                                                            |                                     | `0 - 59`                                                                                                                                                                                                                                          |                                                                                                    |
| `second`                  | `ShortType`   |          | N        | `mod(time_key, 100)`                                                                                                                                                                                           |                                     | `0 - 59`                                                                                                                                                                                                                                          |                                                                                                    |
| `time_bucket_15min`       | `ShortType`   |          | N        | Macro `time_bucket(time_key, 15)`                                                                                                                                                                              |                                     | Vd: `1200`, `2315`                                                                                                                                                                                                                                | Hiển thị dạng smart key `HHMM`, lược bỏ phần giây.                                                |
| `time_bucket_30min`       | `ShortType`   |          | N        | Macro `time_bucket(time_key, 30)`                                                                                                                                                                              |                                     | Vd: `1230`, `2300`                                                                                                                                                                                                                                | Hiển thị dạng smart key `HHMM`, lược bỏ phần giây.                                                |
| `time_bucket_hourly`      | `ShortType`   |          | N        | Macro `time_bucket(time_key, 60)`                                                                                                                                                                              |                                     | Vd: `1200`, `2300`                                                                                                                                                                                                                                | Hiển thị dạng smart key `HHMM`, lược bỏ phần giây.                                                |
| `time_bucket_15min_str`   | `StringType`  |          | N        | Macro `display_bucket_time(time_bucket_15min)`                                                                                                                                                                 |                                     | Vd: `12:00`, `23:15`                                                                                                                                                                                                                              | Hiển thị `HH:MM`, kiểu dữ liệu `String`.                                                          |
| `time_bucket_30min_str`   | `StringType`  |          | N        | Macro `display_bucket_time(time_bucket_30min)`                                                                                                                                                                 |                                     | Vd: `12:30`, `23:00`                                                                                                                                                                                                                              | Hiển thị `HH:MM`, kiểu dữ liệu `String`.                                                          |
| `time_bucket_hourly_str`  | `StringType`  |          | N        | Macro `display_bucket_time(time_bucket_hourly)`                                                                                                                                                                |                                     | Vd: `12:00`, `23:00`                                                                                                                                                                                                                              | Hiển thị `HH:MM`, kiểu dữ liệu `String`.                                                          |
| `day_part`                | `StringType`  |          | N        | `case` theo `time_key` (xem range bên cạnh). Category giữ **UPPER** theo convention toàn dự án.                                                                                                                 |                                     | `{'EARLY NIGHT', 'MORNING', 'AFTERNOON', 'EVENING', 'NIGHT'}`<br>- `'EARLY NIGHT'`: `between(0, 59999)`<br>- `'MORNING'`: `between(60000, 119999)`<br>- `'AFTERNOON'`: `between(120000, 179999)`<br>- `'EVENING'`: `between(180000, 219999)`<br>- `'NIGHT'`: `between(220000, 235959)`<br>Mốc tròn số (`X9999`) chứ không phải mốc chính xác theo giờ:phút:giây (vd `EARLY NIGHT` "thật ra" kết thúc ở `055959`) — nhưng cho kết quả giống hệt vì `minute`/`second` luôn `≤ 59` nên `time_key` không bao giờ chạm vùng `X9999` giả. | Phần ngày, phân loại thô từ `time_key`.                                                           |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------ | ------------------ | ---------------- |
| Add new column     |                    |                  |
| Drop column        |                    |                  |
| Rename column      |                    |                  |
| Change data type   |                    |                  |
| Change nullability |                    |                  |
| Reorder columns    |                    |                  |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `customers_dimension.md`, `dates_dimension.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute           | Value                                                                                       |
| -------------------- | ---------------------------------------------------------------------------------------------- |
| Strategy            | Derived smart key (không phải hash)                                                            |
| Input columns       | `second_of_day` (Integer, `0–86399`) — sinh nội bộ qua `explode(sequence(0, 86399))`, không expose ra ngoài. |
| Policy              | Công thức tính trực tiếp trong SQL của model (`floor`/`mod` trên `second_of_day`), không qua macro/function riêng — chỉ 2 cột bucket smart key (`time_bucket_*`) và display string (`*_str`) dùng macro (`time_bucket`, `display_bucket_time` trong `macros/time_dim_helpers.sql`). |
| Stability guarantee | Deterministic, tái tạo được 100% từ `second_of_day` — không phụ thuộc thứ tự chạy, không có rủi ro hash collision. |

### 6.2. Unknown / Default Member
> Áp dụng cho tất cả các dimension nhằm đảm bảo fact table luôn duy trì referential integrity khi không thể resolve foreign key. Quy định này dành cho các bảng có foreign key tham chiếu đến dimension được nêu tên, không áp dụng cho chính dimension đó.

| Member  | Key value | Khi nào dùng |
| ------- | --------- | ------------ |
| None    | —         | `dim_times` không có Unknown/Default member (giống `dim_dates`, khác `dim_geo`/`dim_customers`/`dim_cards` có `-1`/`-2`). `dim_times` phủ toàn bộ 86,400 giây/ngày, và `time_key` phía fact luôn derive từ một timestamp hợp lệ (`date_format(timestamp,'HHmmss')`) nên luôn nằm trong domain này — luôn resolve được. |

### 6.3. Special Type Handling
> **Special Type:** None

| Aspect                            | Rule |
| ----------------------------------- | ---- |
| Khi NK xuất hiện ở fact trước dim   | N/A — `dim_times` được generate sẵn toàn bộ 86,400 giây trước khi có dữ liệu fact nào. |
| Placeholder attribute values        | N/A  |
| Back-update khi dim thật về         | N/A  |
| Cờ đánh dấu                        | N/A  |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | -------------- | --- |
| None   | —              | Type 1, full reload — không track lịch sử version cho dimension này. |

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table | Join Condition | Unmatched Key Handling |
| ---------------- | ------------- | ----------------- | ------------------------- |
| None             |               |                    |                            |

> `dim_times` là self-generated, không có cột FK nào tham chiếu ra dimension khác. Chiều FK ngược lại (`fact_transactions.time_key` → `dim_times.time_key`) thuộc về spec của `fact_transactions`; sau khi xóa runtime join thừa (xem Decision Log), FK integrity được kiểm tra qua dbt `relationships` test (`fact_transactions.yml`), không còn qua join trong model.

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Generate second-of-day spine: `explode(sequence(0, 86399))` → mỗi dòng 1 giây trong ngày.
2. Tính `time_key` (smart key `HHMMSS`) trực tiếp từ `second_of_day` bằng công thức `floor`/`mod`.
3. Tính các thuộc tính dẫn xuất từ `time_key`: `hour_24`, `hour_12`, `am_pm`, `minute`, `second`, `time_bucket_15min`/`30min`/`hourly` (qua macro `time_bucket`), `day_part`.
4. Tính các cột display string: `time_24h` (concat/lpad), `time_bucket_*_str` (qua macro `display_bucket_time`).
5. Materialize full table — ghi đè toàn bộ `gold.dim_times`, không merge/upsert.

| Attribute           | Value                                                                                      |
| --------------------- | ---------------------------------------------------------------------------------------------- |
| Merge / upsert keys | N/A — full reload thay thế toàn bộ bảng mỗi lần chạy.                                          |
| Idempotency         | Có — range `0–86399` cố định luôn sinh đúng 86,400 dòng giống hệt nhau mỗi lần chạy.            |
| Failure / retry     | Fail + halt nếu vi phạm PK uniqueness/not-null (mục 9); an toàn để retry vì là full reload.     |

### 8.2 Backfill & Historical Load Strategy

> Không áp dụng — toàn bộ 86,400 giây/ngày được generate sẵn trong 1 lần full-refresh, không có khái niệm "backfill dần theo lô" (khác `dim_dates`, range ở đây cố định `0–86399`, không có var cấu hình để mở rộng).

---
## 9. Data Quality & Observability Checks

| Check Name                       | Target Column                              | Rule/Condition                                             | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| ----------------------------------- | --------------------------------------------- | -------------------------------------------------------------- | ---------------- | -------- | --------- | ---------------- | --------------- |
| PK uniqueness                    | `time_key`                                  | unique & not null                                             | 0 violations    | Critical | Per run   | fail + halt      |               |
| Core time attribute completeness | `time_24h`, `hour_24`, `hour_12`, `am_pm`   | not null                                                       | 0 violations    | Warning  | Per run   | alert            |               |
| `am_pm` domain validity          | `am_pm`                                      | accepted values `{AM, PM}`                                     | 0 violations    | Warning  | Per run   | alert            |               |
| `day_part` domain validity       | `day_part`                                   | accepted values `{EARLY NIGHT, MORNING, AFTERNOON, EVENING, NIGHT}` | 0 violations    | Warning  | Per run   | alert            |               |
| Full-day coverage                | `time_key`                                   | `count(*) = 86400`, `min = 0`, `max = 235959`, không gap        | 100% covered    | Low      | Per run   | monitoring       |               |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:**
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column          | PII Level | Masking / Encryption Rule | Data Retention / Purge Policy |
| ----------------- | --------- | --------------------------- | -------------------------------- |
| Tất cả các cột  | NSA       | Không cần — dữ liệu thời gian thuần túy, không chứa thông tin cá nhân. |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
| 1   | Frequency / Orchestrator (mục 2) chưa được quyết định ở cấp dự án. Quan sát thấy hiện tại `dim_times` được chạy tay qua dòng lệnh comment trong `scripts/gold-dbt/deploy_gold_dbt_dev.sh` (`dbt run --select dim_times`), nhưng đây chưa phải quyết định chính thức. | No | NghiemCanCode | Open |
| 2   | Chưa xác định BI Artifact / Data Product cụ thể dùng trực tiếp `dim_times`. | No | NghiemCanCode | Open |
| 3   | `Data Retention / Purge Policy` (mục 10) chưa được điền — chưa có quyết định compliance ở cấp dự án (đồng bộ tình trạng các spec khác). | No | NghiemCanCode | Open |
| 4   | `fact_transactions.yml` mô tả cột `time_key` vẫn ghi "FK to gold.dim_time.time_key" (số ít, không khớp tên model thật `dim_times`) trong phần description — nằm ngoài phạm vi spec này (`time_dimension.md`), ghi nhận để dọn cùng đợt khi sửa `fact_transactions`/`transactions_fact.md`. | No | NghiemCanCode | Open |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision | Rationale | Decided by |
| ---------- | -------- | --------- | ---------- |
| 2026-07-23 | Draft cũ có nhiều đoạn leftover copy-paste từ các spec khác: mục 3 "Lưu ý" (Holiday calendar/Fiscal — thuộc `dim_dates`), mục 6.1 Input columns "city, state, zip" (thuộc `dim_geo`), mục 6.2 "Bảng `dim_date` thiếu ngày này" (nhầm tên dimension), và ô Allowed Range của `time_24h` ghi "Có xuất hiện giá trị 'ONLINE'" (thuộc `transaction_type`/`merchant_city`, không liên quan `time_24h`). Viết lại toàn bộ dựa theo code thật (`dim_times.sql`, `dim_times.yml`, `macros/time_dim_helpers.sql`). | Tránh khóa cứng nội dung sai vào spec chính thức; user xác nhận đây là copy-paste leftover, không phải business rule thật. | NghiemCanCode |
| 2026-07-23 | Table name giữ `gold.dim_times`; xác nhận `dim_times.yml` **đã** khai báo đúng `name: dim_times` — không có bug naming mismatch giống `dim_dates`. | Kiểm tra trực tiếp file thực tế, khác với suy đoán ban đầu dựa trên pattern đã thấy ở `dim_date.yml`. | NghiemCanCode |
| 2026-07-23 | Sửa `scripts/gold-dbt/deploy_gold_dbt_dev.sh`: đổi dòng comment `--select dim_time` → `--select dim_times`. | Dòng comment (chưa từng chạy) vẫn dùng tên số ít sai, cùng loại lỗi đã thấy ở `dim_date`; sửa cho khớp tên model thật. | NghiemCanCode |
| 2026-07-23 | **[Blocking fix]** `fact_transactions.sql`: xóa `left join {{ ref('dim_times') }} as dt on transactions.time_key = dt.time_key` khỏi CTE `resolved`. Join này không select cột nào từ `dt` và không dùng để filter (khác `dd`/`dc`/`dca`/`dg`/`dm`, đều được coalesce hoặc dùng filter) — hoàn toàn không ảnh hưởng output, là dead code (nhiều khả năng do copy nửa chừng pattern của các dim khác). FK integrity (`time_key` → `dim_times`) vẫn được enforce độc lập qua dbt `relationships` test trong `fact_transactions.yml` (chạy ở test-time, không phụ thuộc join trong model). | User xác nhận đây là bug cần fix ngay (blocking), không chỉ ghi Open Question; best practice là để dbt schema test đảm nhiệm FK validation, không duplicate logic ở runtime khi domain đã được xác nhận luôn resolve được (xem Unknown/Default Member bên dưới). | NghiemCanCode |
| 2026-07-23 | Mục 6.1 Surrogate Key: sửa Input columns từ "city, state, zip" (leftover từ `dim_geo`) → `second_of_day` (Integer, 0–86399); Policy = công thức inline trong SQL, không phải "smart key function" riêng. | Khớp đúng code — `dim_times.sql` tính `time_key` trực tiếp bằng `floor`/`mod` trên `second_of_day`, không gọi function/macro riêng cho bước này. | NghiemCanCode |
| 2026-07-23 | Mục 6.2 Unknown/Default Member: `dim_times` = "None", giống `dim_dates`. | `dim_times` phủ toàn bộ 86,400 giây/ngày; `time_key` phía fact luôn derive từ timestamp hợp lệ nên luôn nằm trong domain này, luôn resolve được — không cần Unknown member. | NghiemCanCode |
| 2026-07-23 | Sửa cột "Not Null" của `hour_24`, `hour_12`, `am_pm` từ `N` → `Y`. | Khớp `not_null` test đã khai báo cho cả 3 cột trong `dim_times.yml`; draft cũ ghi sai. | NghiemCanCode |
| 2026-07-23 | Sửa Allowed Range của `hour_12` từ "0-11" → "1-12". | Khớp đúng công thức code (`mod(hour_24,12)`, khi bằng 0 thì trả về 12 thay vì 0) và mô tả trong `dim_times.yml` ("Hour in 12-hour format (1-12)"). | NghiemCanCode |
| 2026-07-23 | Sửa sample của `time_bucket_15min_str`/`time_bucket_hourly_str` từ dạng có giây (`"12:00:00"`) → đúng format thật chỉ giờ:phút (`"12:00"`); thêm dòng còn thiếu `time_bucket_30min_str`. | Khớp macro `display_bucket_time` (chỉ nối `HH:MM`, không có phần giây) và `dim_times.yml`/`dim_times.sql` (có tính `time_bucket_30min_str` nhưng draft cũ thiếu dòng này). | NghiemCanCode |
| 2026-07-23 | Mục 5.1 `day_part`: giữ range theo đúng code thực (mốc tròn số, vd `'Early Night'`: `0-59999`) thay vì mốc chính xác theo giờ:phút:giây, kèm note giải thích 2 cách tương đương. | User chọn ưu tiên khớp code thay vì range "đẹp" về mặt lý thuyết, tránh gây hiểu lầm khi đối chiếu với `dim_times.sql`. | NghiemCanCode |
| 2026-07-23 | Mục 3 "Lưu ý": xóa nội dung Holiday calendar/Fiscal (leftover từ `dim_dates`), thay bằng "Không áp dụng cho `dim_times`". | `dim_times` là time-of-day dimension thuần, không có khái niệm ngày lễ/fiscal year. | NghiemCanCode |
| 2026-07-23 | Mục 10: toàn bộ cột đánh PII Level = NSA, không cần masking/retention rule. | `dim_times` chỉ chứa dữ liệu thời gian thuần túy, không chứa thông tin cá nhân — giống `dim_dates`. | NghiemCanCode |
