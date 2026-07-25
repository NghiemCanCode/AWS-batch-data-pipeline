# Technical Specification: Customer Activity Daily Fact

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.2       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-24    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                              |
| ------- | ---------- | ------------- | ---------------------------------------------------------------------------------------------------- |
| v.0.0.1 | 2026-07-23 | NghiemCanCode | Initial spec qua Q&A với Claude — "nhà" cho định nghĩa Active Customer/Card trailing 90 ngày (business spec §4). Thuộc reporting layer mới `models/marts/reporting/`. Model chưa được implement. |
| v.0.0.2 | 2026-07-24 | NghiemCanCode | Chốt 3 điểm còn mở trước khi implement (Q&A 2026-07-24): (1) giao dịch `customer_key = '-1'` bị loại hoàn toàn; (2) định nghĩa chính xác điểm kết thúc của date spine cho từng chế độ chạy; (3) `active_card_count_90d` đếm distinct **`card_id`** (NK qua `dim_cards`) thay vì `card_key`. Thêm `dim_cards` vào upstream dependencies. |

---

## 1. Overview & Business Context

> **Purpose:** Vật chất hóa định nghĩa **Active Customer / Active Card = ≥1 giao dịch trong trailing 90 ngày** (business spec §4) — một nguồn sự thật duy nhất, BI không tự tính lại công thức. Phục vụ KPI "Active Customers" của Dashboard B (breakdown theo segment) và "Issued vs Active cards" của Dashboard C.
> **Primary consumers:** Dashboard B (Customer Spending by Segment), Dashboard C (Card Portfolio)

| Attribute    | Value                       | Description                                                          |
| ------------ | --------------------------- | ---------------------------------------------------------------------- |
| SCD Type     | None                        | Derived fact — rebuild theo partition                                  |
| Special type | Derived / reporting fact (fact-of-fact) | Nguồn duy nhất là `gold.fact_transactions`               |
| Grain        | 1 dòng / (`date_key`, `customer_id`)     | Mỗi khách hàng × mỗi ngày kể từ ngày có giao dịch đầu tiên của khách đó |

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                       | Description                                |
| ---------------- | ------------------------------------------------------------ | ------------------------------------------- |
| Table name       | `gold.fact_customer_activity_daily`                          | Model đặt tại `models/marts/reporting/` — layer mới cho các model mã hóa metric definitions (Decision Log) |
| Layer            | Gold (reporting)                                             |                                            |
| Source(s)        | `gold.fact_transactions`; `gold.dim_dates` (date spine); `gold.dim_customers` (resolve `customer_id` NK + `customer_key` as-of); `gold.dim_cards` (resolve `card_id` NK cho distinct card count) |  |
| Load strategy    | Incremental (`insert_overwrite` partition theo `date_key`)   | Mỗi partition ngày D cần trailing 90 ngày fact — luôn recompute được độc lập, idempotent |
| Watermark column | Không cần — partition overwrite theo `batch_logical_date()`  |                                            |
| Frequency        | Daily batch T+1 (business spec §7)                           |                                            |
| Orchestrator     |                                                               | Chưa quyết định ở cấp dự án                |
| SLA              | None                                                         |                                            |
### 2.1. Physical Storage Layout

| Attribute                 | Value       |
| ------------------------- | ----------- |
| Table Format              | `Iceberg`   |
| Partitioning Columns      | `date_key`  |
| Z-Order / Clustering Keys | None        |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là trạng thái hoạt động của 1 khách hàng tại cuối 1 ngày.
> **Primary Key:** Composite (`date_key`, `customer_id`) — không surrogate key (đồng bộ các aggregate fact khác).
> **Uniqueness test:** `dbt_utils.unique_combination_of_columns` trên (`date_key`, `customer_id`)
>
> **Date spine:** mỗi `customer_id` có 1 dòng cho **mọi ngày** (theo `dim_dates`) từ ngày giao dịch đầu tiên của khách đó đến điểm kết thúc spine — kể cả ngày không giao dịch (khi đó các cờ/measure phản ánh trailing window). Khách chưa từng giao dịch không xuất hiện — xem Open Question #1.
>
> **Điểm kết thúc spine** (chốt 2026-07-24, xem Decision Log):
> - **Full-refresh:** ngày giao dịch lớn nhất có thật trong `fact_transactions`. Tránh sinh hàng năm dòng rỗng khi build lại trên dữ liệu lịch sử mà không truyền `batch_logical_date`.
> - **Incremental:** đúng ngày `batch_logical_date()` — *không* phụ thuộc dữ liệu ngày đó. Một ngày D không có giao dịch nào trên toàn hệ thống vẫn phải ghi đủ partition: mọi khách có giao dịch đầu ≤ D đều có dòng, với measure phản ánh cửa sổ trailing (có thể `is_active_90d = false`).
>
> **Phạm vi khách:** chỉ tính giao dịch có `customer_key != '-1'`. Giao dịch không resolve được khách bị **loại hoàn toàn** khỏi bảng này (xem §6 và Decision Log) — do đó tổng `transaction_count_90d` không khớp tuyệt đối với `fact_transactions`; đây là chênh lệch đã biết, không phải lỗi.

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source              | Dependency Type    | Note                                                                 |
| ------------------------- | ------------------- | --------------------------------------------------------------------- |
| `gold.fact_transactions`  | Hard (must finish)  | Nguồn hoạt động; kế thừa as-of point-in-time từ v.0.0.3+             |
| `gold.dim_dates`          | Hard (must finish)  | Date spine                                                            |
| `gold.dim_customers`      | Hard (must finish)  | Hai vai trò: (1) `customer_key` → `customer_id` (NK) để đặt grain theo entity; (2) resolve `customer_key` as-of cuối ngày (xem 5.1) để BI join lấy segment đúng point-in-time |
| `gold.dim_cards`          | Hard (must finish)  | Lookup `card_key` → `card_id` (NK) cho `active_card_count_90d` — đếm distinct trên NK để thẻ đổi version SCD2 giữa cửa sổ không bị đếm 2 lần |

### Downstream Consumers

| Type         | Name          | Note                                                              |
| ------------ | ------------- | ------------------------------------------------------------------ |
| BI Artifact  | Dashboard B   | Active Customers theo segment: `count(distinct customer_id) where is_active_90d`, join `dim_customers` qua `customer_key` |
| BI Artifact  | Dashboard C   | Active cards: `sum(active_card_count_90d)`... (xem note 5.1 về distinct) |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name              | Data Type        | Key Type | Not Null | Transformation Logic                                                                                          | Null Handling        | Allowed Range / Sample | Business Definition                                                                     |
| ------------------------ | ---------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------- | --------------------- | ----------------------- | ----------------------------------------------------------------------------------------- |
| `date_key`               | `IntegerType`    | PK, FK   | Y        | Date spine từ `dim_dates`                                                                                       | Raise pipeline error | —                       | Ngày "tính đến cuối ngày này". FK → `dim_dates.date_key`; partition key.                |
| `customer_id`            | `StringType`     | PK, NK   | Y        | Join `fact_transactions.customer_key` → `dim_customers.customer_key` (khớp surrogate chính xác) lấy `customer_id`; chỉ các khách có ≥1 giao dịch với `customer_key != '-1'` | Raise pipeline error | —                       | Business key của khách — grain theo entity, không theo SCD2 version.                     |
| `customer_key`           | `StringType`     | FK       | Y        | `dim_customers.customer_key` của phiên bản **as-of cuối ngày** (`effective_from <= end_of_day < effective_to`)  | Map Unknown (`-1`)   | —                       | Cho BI join thẳng `dim_customers` lấy segment đúng point-in-time của ngày đó.            |
| `is_active_90d`          | `BooleanType`    |          | Y        | `true` nếu khách có ≥1 giao dịch trong `(date - 89 ngày, date]` (trailing 90 ngày, tính cả ngày hiện tại)       | Raise pipeline error | `{true, false}`         | **Định nghĩa Active Customer của business spec §4** — nguồn sự thật duy nhất.            |
| `active_card_count_90d`  | `IntegerType`    |          | Y        | `count(distinct card_id)` của các giao dịch trong cùng trailing window — `card_id` lấy qua join `fact_transactions.card_key` → `dim_cards`; giao dịch có `card_key = '-1'` bị loại khỏi phép đếm (nhưng vẫn tính vào `transaction_count_90d`) | Raise pipeline error | ≥ 0                     | Số thẻ active (định nghĩa Active Card §4) của khách tại ngày đó. Đếm trên **NK `card_id`**, không phải surrogate `card_key`: một thẻ đổi phiên bản SCD2 giữa cửa sổ 90 ngày mang 2 `card_key` nhưng vẫn là 1 thẻ (chốt 2026-07-24). |
| `transaction_count_90d`  | `IntegerType`    |          | Y        | `count(*)` giao dịch trong trailing window (bao gồm cả giao dịch có `card_key = '-1'` — chúng vẫn là giao dịch thật) | Raise pipeline error | ≥ 0                     | Tiện cho phân tích cường độ hoạt động; `is_active_90d = (transaction_count_90d > 0)`.   |
| `last_transaction_date_key` | `IntegerType` | FK       | N        | `max(date_key)` của giao dịch gần nhất ≤ ngày hiện tại (toàn lịch sử, không giới hạn 90 ngày)                  | `NULL` nếu chưa có (không xảy ra với spine từ giao dịch đầu) | — | Phục vụ phân tích recency/churn sau này.                          |

> **Cảnh báo cộng dồn (bắt buộc ghi trong yml):** "Active Customers" của một ngày = `count(*) where is_active_90d` tại đúng ngày đó. **Không** sum/avg `is_active_90d` qua nhiều ngày (một khách active 30 ngày liên tiếp không phải 30 khách). "Active Cards" toàn hàng = phải tính `count(distinct card_id)` từ `fact_transactions` (join `dim_cards`) — `sum(active_card_count_90d)` qua nhiều khách đúng (thẻ thuộc 1 khách) nhưng qua nhiều ngày thì sai.

### 5.2. Schema Evolution Policy

> Chưa quyết định ở cấp dự án — đồng nhất với các spec khác.

---
## 6. Key Strategy & Special Members

> Không surrogate key (composite PK). Không seed Unknown member — bảng chỉ chứa khách thật đã có giao dịch; `customer_key` fallback `-1` chỉ khi as-of resolve thất bại (vi phạm interval coverage — sẽ bị DQ check của dimension bắt trước).
>
> **Xử lý sentinel ở nguồn** (chốt 2026-07-24): giao dịch có `customer_key = '-1'` bị **loại khỏi bảng**, cùng lý do và cùng cách làm với `fact_user_monthly_snapshot` (`where customer_key != '-1'`) — một bảng hoạt-động-theo-khách không có ý nghĩa báo cáo cho khách không xác định, và không thể tạo dòng spine cho một `customer_id` không tồn tại. Khác với `fact_daily_transaction_trend` (giữ sentinel để reconcile khớp tuyệt đối với `fact_transactions`). Sentinel `card_key = '-1'` thì ngược lại: giao dịch được giữ, chỉ bị loại bên trong phép đếm distinct thẻ.

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                     | Join Condition                                                              | Unmatched Key Handling |
| -------------- | -------------------------------- | ---------------------------------------------------------------------------- | ----------------------- |
| `date_key`     | `gold.dim_dates.date_key`        | Spine sinh từ chính `dim_dates`                                             | Không phát sinh         |
| `customer_key` | `gold.dim_customers.customer_key`| As-of **cuối** ngày D. Với interval `[effective_from_date, effective_to_date)`, "cuối ngày D" là khoảnh khắc ngay trước nửa đêm D+1, nên điều kiện là `effective_from_date < timestamp(D+1) AND effective_to_date >= timestamp(D+1)`. Dấu `<` (không phải `<=`) ở vế `from` là chủ ý: phiên bản bắt đầu đúng nửa đêm D+1 chưa tồn tại trong ngày D | Map Unknown (`-1`) + log |
| *(không phải cột)* `card_id` | `gold.dim_cards.card_key` | Lookup khớp surrogate chính xác `fact_transactions.card_key = dim_cards.card_key` (không cần as-of: chỉ lấy NK, thuộc tính thẻ không dùng ở đây). Không xuất hiện trong output, chỉ dùng bên trong `active_card_count_90d` | `card_key = '-1'` loại khỏi phép đếm |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `fact_transactions` **toàn lịch sử** (không lọc theo ngày batch), bỏ `customer_key = '-1'`. Lưu ý: khác với các fact-of-fact khác, bộ lọc ngày **không** đặt được ở bước đọc nguồn — mỗi ngày output cần 90 ngày trước đó, và `last_transaction_date_key` + ngày giao dịch đầu của khách cần toàn bộ lịch sử.
2. Gộp trước về grain (`customer_id`, ngày): số giao dịch trong ngày + tập `card_id` distinct trong ngày (map `customer_key`/`card_key` → NK qua `dim_customers`/`dim_cards`).
3. Dựng spine (khách × mọi ngày từ giao dịch đầu của khách đến điểm kết thúc spine — xem §3), left join kết quả bước 2; ngày không giao dịch null-fill về 0/tập rỗng.
4. Tính measure trailing 90 ngày bằng window function trượt trên spine (`rows between 89 preceding and current row`) thay vì range-join spine × giao dịch — spine liền mạch 1 dòng/khách/ngày nên frame theo dòng chính là 90 ngày dương lịch, và tránh cartesian per-customer mà range-join sinh ra trên Spark.
5. Lọc về partition ngày batch (chỉ ở incremental run) — đặt **sau** bước 4, không phải trước.
6. As-of join `dim_customers` lấy `customer_key` hiệu lực cuối ngày D.
7. `insert_overwrite` partition `date_key`.

| Attribute           | Value                                                                                     |
| ------------------- | -------------------------------------------------------------------------------------------- |
| Merge / upsert keys | Không merge — overwrite nguyên partition `date_key`                                          |
| Idempotency         | Mỗi partition ngày D chỉ phụ thuộc fact 90 ngày trước đó → recompute độc lập, rerun idempotent |
| Failure / retry     | Fail + halt khi vi phạm Critical checks (mục 9); an toàn retry                               |

### 8.2 Backfill & Historical Load Strategy

> Full history build ở lần chạy đầu / full-refresh. Bảng nằm trong chuỗi full-refresh restatement point-in-time (`transactions_fact.md` Decision Log v.0.0.3) vì phụ thuộc `fact_transactions`.

---
## 9. Data Quality & Observability Checks

| Check Name                  | Target Column                  | Rule/Condition                                                              | Threshold    | Severity | Frequency | Action on Fail       | Alert Channel |
| ---------------------------- | -------------------------------- | ---------------------------------------------------------------------------- | ------------ | -------- | --------- | ----------------------- | --------------- |
| Grain uniqueness             | `date_key`, `customer_id`       | `dbt_utils.unique_combination_of_columns`                                    | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Grain keys not null          | `date_key`, `customer_id`       | not null                                                                     | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Flag/count consistency       | `is_active_90d`, `transaction_count_90d` | `is_active_90d = (transaction_count_90d > 0)` trên mỗi dòng          | 0 violations | Critical | Per run   | Pipeline fail + halt    |                 |
| Active ⇒ có thẻ hoặc unknown | `is_active_90d`, `active_card_count_90d` | Không enforce `active_card_count_90d > 0` khi active — giao dịch có thể mang `card_key = '-1'` (sentinel bị loại khỏi distinct) | — | — | — | Ghi nhận hành vi, không phải check | |
| Customer key resolution      | `customer_key`                  | `% customer_key = '-1'`                                                     | 0% (kỳ vọng — interval coverage đã đảm bảo) | High | Per run | Alert |                 |
| Spot reconciliation          | `is_active_90d`                 | Singular test scope theo ngày `batch_logical_date()`: `count(*) where is_active_90d` khớp `count(distinct customer_id)` có ≥1 giao dịch trong `(D-89, D]` tính trực tiếp từ `fact_transactions`. Distinct theo **`customer_id`** (không phải `customer_key` — khách đổi version SCD2 giữa cửa sổ vẫn là 1 khách); loại `customer_key = '-1'` ở cả hai vế cho khớp phạm vi bảng | 0 lệch | Critical | Per run | Pipeline fail + halt |                 |

---
## 10. Security & Governance

| Column            | PII Level | Masking / Encryption Rule                                                    | Data Retention / Purge Policy |
| ------------------ | --------- | ------------------------------------------------------------------------------ | -------------------------------- |
| `customer_id`      | DI        | Cùng quy tắc với `dim_customers.customer_id` (hash tại nguồn — xem spec đó); không lặp lại rule tại đây | |
| Các cột còn lại    | NSA/SA    | Cờ hoạt động + count gắn với `customer_id` có thể suy luận hành vi — cùng mức với `transaction_amount` của fact gốc (SA đề xuất) | |

---
## 11. Open Questions & Decision Log
### Open Questions

| #   | Question                                                                                                                              | Blocking? | Owner         | Status |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------- | --------- | --------------- | ------ |
| 1   | Khách tồn tại trong `dim_customers` nhưng **chưa từng giao dịch** không xuất hiện trong bảng → mẫu số "tỷ lệ active trên tổng khách" không lấy được từ đây (phải đếm từ `dim_customers`). Có cần mở spine về ngày mở tài khoản không? Nguồn hiện không có ngày mở tài khoản. | No | NghiemCanCode | Open |
| 2   | Chip adoption (`rpt_card_portfolio`) và error-rate view (`rpt_merchant_error_daily`) được hoãn khỏi phạm vi đợt này (Q&A 2026-07-23). **Cập nhật 2026-07-25:** cả hai đều không còn hoãn — `rpt_merchant_error_daily` có spec `docs/metrics/merchant_error_daily_report.md` (Decision #25), `rpt_card_portfolio` có spec `docs/metrics/card_portfolio_report.md` (Decision #26). **Cập nhật 2026-07-25 (chiều):** `rpt_card_portfolio` **đã implement** (verify offline, chưa chạy dev); `rpt_merchant_error_daily` vẫn chưa. **Cập nhật 2026-07-25 (tối):** cả hai **đã implement và đã chạy green trên dev** (README §18) — không còn bảng reporting nào ở trạng thái chờ. Riêng với model NÀY, lần chạy đó đã trả lời câu hỏi bỏ ngỏ của cross-check: **Δ = 0, residual = 0**, hai "nhà" của Active Card cùng cho 3.385 tại `date_key` 20191031. Hệ quả đã hiện thực cho model NÀY: nó nay là **một vế của một test không thuộc về nó** — `dbt/tests/rpt_card_portfolio_active_card_cross_check.sql` so `sum(active_card_count_90d)` của hai bảng tại cùng `date_key`, cộng số hạng Δ đo đúng chỗ hai bảng cố ý khác nhau (bảng này drop giao dịch `customer_key = '-1'`, bảng kia giữ). Sửa logic `active_card_count_90d` ở đây sẽ làm fail một test nằm ở thư mục khác. | No | NghiemCanCode | **Resolved** |

### Decision Log

| Date       | Decision                                                                                          | Rationale                                                                                                                                          | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- |
| 2026-07-23 | Metric definitions sống ở **dbt reporting layer** (`models/marts/reporting/`, prefix `rpt_`/`agg_`/fact reporting), không phải BI tool hay MetricFlow | Một nguồn sự thật duy nhất, có yml docs + tests; MetricFlow không hỗ trợ adapter dbt-spark nên semantic layer "chính chủ" không khả thi trên stack này. | NghiemCanCode    |
| 2026-07-23 | Active Customer/Card 90d vật chất hóa bằng `fact_customer_activity_daily`, grain date × customer      | Grain entity-level cho phép Dashboard B breakdown theo segment (join `dim_customers` qua `customer_key` as-of); bảng aggregate-theo-ngày thuần không làm được điều đó. Quy mô dữ liệu (~nghìn khách × nghìn ngày) đủ nhỏ. | NghiemCanCode    |
| 2026-07-23 | Grain theo `customer_id` (NK/entity), carry thêm `customer_key` as-of cuối ngày                       | Hoạt động là thuộc tính của *khách*, không của *phiên bản SCD2*; nhưng carry key as-of để BI lấy segment đúng point-in-time bằng 1 join thường.       | NghiemCanCode    |
| 2026-07-23 | Trailing window = `(date - 89, date]`, tính cả ngày hiện tại                                          | "Trailing 90 ngày" nghĩa là 90 ngày dương lịch kết thúc tại chính ngày đang xét — nhất quán với cách người đọc business hiểu §4.                      | NghiemCanCode    |
| 2026-07-23 | Phạm vi đợt này chỉ gồm Active 90d + retirement-age (cột trên fact); chip adoption và error-rate views hoãn | Q&A 2026-07-23 — hai metric còn lại đã tính trực tiếp được từ `dim_cards`/trend fact, chưa cấp thiết. Xem Open Question #2.                            | NghiemCanCode    |
| 2026-07-24 | Giao dịch `customer_key = '-1'` bị **loại hoàn toàn**, không map về dòng `UNKNOWN`                    | Theo tiền lệ `fact_user_monthly_snapshot`: hoạt động không gán được cho khách nào thì không có ý nghĩa báo cáo, và không thể dựng spine cho một `customer_id` không tồn tại. Nếu giữ, `'UNKNOWN'` sẽ hiện lên như một khách hàng trong Dashboard B. Đánh đổi: tổng giao dịch không khớp tuyệt đối `fact_transactions` — chấp nhận, ghi rõ ở §3. | NghiemCanCode    |
| 2026-07-24 | Điểm kết thúc spine: full-refresh = max ngày giao dịch thật; incremental = `batch_logical_date()`      | Full-refresh chạy trên dữ liệu lịch sử mà không truyền `--vars` sẽ lấy default `current_date()`, sinh hàng năm dòng rỗng vô nghĩa nếu neo theo đó. Ngược lại, incremental phải neo vào ngày batch (không phải max ngày có giao dịch) để một ngày trống toàn hệ thống vẫn ghi đủ partition, và để backfill một ngày cũ dừng đúng chỗ. | NghiemCanCode    |
| 2026-07-24 | `active_card_count_90d` đếm distinct `card_id` (NK qua `dim_cards`), không phải `card_key`             | `card_key` là surrogate SCD2: thẻ đổi thuộc tính giữa cửa sổ 90 ngày mang 2 key và bị đếm thành 2 thẻ. "Số thẻ active" là thuộc tính của *thẻ*, không của *phiên bản thẻ* — cùng lập luận với việc đặt grain theo `customer_id`. Chi phí: thêm 1 join `dim_cards` (§4). | NghiemCanCode    |
