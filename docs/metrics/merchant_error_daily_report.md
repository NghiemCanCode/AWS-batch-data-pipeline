# Technical Specification: Merchant Error Daily Report

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.3       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-25    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                              |
| ------- | ---------- | ------------- | ---------------------------------------------------------------------------------------------------- |
| v.0.0.1 | 2026-07-25 | NghiemCanCode | Initial spec qua Q&A với Claude — "nhà" cho metric **Abnormal Error Rate (merchant)** (business spec §4, Decision #22/#23/#24). Đóng nửa phần merchant của Open Question #1 ở `metrics_layer.md` (nửa `rpt_card_portfolio` vẫn hoãn). Model **chưa được implement**. |
| v.0.0.3 | 2026-07-25 | NghiemCanCode | **Đã chạy trên dev** — 35/35 green cả ba lần (full-refresh 762s + hai lần incremental ~178s), WARN=0, fingerprint giống hệt nhau ⇒ **idempotent**. Sáu ẩn số của v.0.0.2 nay có số thật (§8.3): mặt bằng **`0.016090`** khớp đến chữ số cuối với 1,609% mà calibration đo bằng một đường tính hoàn toàn khác — bằng chứng độc lập rằng phép gộp qua `mcc` không sót dòng; **12** merchant gắn cờ đúng dự đoán và nằm trong band 10–50; **584.908** dòng / **165** dòng/ngày, khớp ước tính §8.2. Không sửa grain, cột, logic hay quy tắc aggregate nào. |
| v.0.0.2 | 2026-07-25 | NghiemCanCode | **Model đã implement** — `dbt/models/marts/reporting/rpt_merchant_error_daily.sql` + `.yml` + 4 singular test + 2 vars ở `dbt_project.yml`. Mới **verify offline**, chưa chạy dev lần nào (xem §8.3 mới). Ghi nhận 3 quyết định phát sinh lúc viết code, đều nằm trong khoảng spec để mở: `primary_mcc` dùng `row_number` thay `max_by`; `excess` tính từ `expected` **đã làm tròn**; check "Flagged-count in operating band" **bỏ qua partition rỗng**. Open Question #3 giữ Resolved, thêm ghi chú trạng thái. |

---

## 1. Overview & Business Context

> **Purpose:** Vật chất hóa định nghĩa **Abnormal Error Rate (merchant)** của business spec §4 — merchant có Error Rate trong cửa sổ trailing 30 ngày vượt **4,0%**, chỉ xét merchant có **≥ 50 giao dịch** trong cửa sổ đó. Đây là metric duy nhất trong §4 mà BI hiện phải tự viết công thức; bảng này chấm dứt điều đó. Trả lời trực tiếp **success criterion 6** ("Which merchants show an abnormal error rate over the trailing 30 days?") và cấp tín hiệu chất lượng trải nghiệm cho Dashboard A — một ứng viên cashback có tỷ lệ lỗi cao là một đối tác tồi (Decision #3).
> **Primary consumers:** Dashboard A (Merchant & Category Spending — flagship), success criterion 6

| Attribute    | Value                                              | Description                                                     |
| ------------ | -------------------------------------------------- | --------------------------------------------------------------- |
| SCD Type     | None                                               | Derived report — rebuild theo partition                          |
| Special type | Derived / reporting model (fact-of-fact)           | Nguồn duy nhất là `gold.fact_daily_transaction_trend`             |
| Grain        | 1 dòng / (`date_key`, `merchant_id`), **chỉ merchant qua sàn volume** | Trạng thái error rate trailing 30 ngày của 1 merchant tính đến cuối 1 ngày |

> **Bảng này là bộ lọc lập shortlist, KHÔNG phải phán quyết.** Ở sát sàn (n = 50), một merchant chỉ cần **3 lỗi** là vượt ngưỡng 4% trong khi kỳ vọng theo mặt bằng là 0,8 — khoảng tin cậy 95% của tỷ lệ đo được quanh mức đó rộng cỡ **±6 điểm phần trăm**. Câu này là ràng buộc bắt buộc từ `abnormal_error_rate_calibration.md` §4 và **phải xuất hiện trong `.yml` của model**, không chỉ ở tài liệu này. Cột `excess_failed_transactions_30d` (§5.1) tồn tại chính vì lý do đó.

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                                 | Description                                |
| ---------------- | --------------------------------------------------------------------- | ------------------------------------------- |
| Table name       | `gold.rpt_merchant_error_daily`                                       | Model đặt tại `models/marts/reporting/` — layer cho các model mã hóa metric definitions (Decision #17 business spec) |
| Layer            | Gold (reporting)                                                      |                                            |
| Source(s)        | `gold.fact_daily_transaction_trend` (nguồn measure duy nhất); `gold.dim_dates` (date spine); `gold.dim_merchant` (chỉ để FK-check `primary_mcc`, không join lấy thuộc tính) | |
| Load strategy    | Incremental (`insert_overwrite` partition theo `date_key`)            | Mỗi partition ngày D chỉ phụ thuộc đúng 30 ngày trend fact → recompute độc lập, idempotent |
| Watermark column | Không cần — partition overwrite theo `batch_logical_date()`           |                                            |
| Frequency        | Daily batch T+1 (business spec §7)                                    | Fact nguồn vẫn daily; chỉ cửa sổ báo cáo rộng 30 ngày (Decision #23) |
| Orchestrator     |                                                                       | Chưa quyết định ở cấp dự án                |
| SLA              | None                                                                  |                                            |

> **`batch_logical_date()` là ngày DỮ LIỆU, không phải ngày chạy batch** — cùng quy ước với ba insert_overwrite fact còn lại. Một lần chạy T+1 vào ngày D+1 **phải** truyền `--vars '{batch_logical_date: <D>}'`, nếu không macro lấy default `current_date()` và ghi đè một partition rỗng.
>
> **Bẫy `2036-01-01`:** giá trị sentinel "process everything" mà các script legacy PySpark truyền vào **không bao giờ** được dùng làm `batch_logical_date` ở đây. `dim_dates` kết thúc 2035-12-31 nên cửa sổ resolve ra rỗng: model ghi 0 dòng trong im lặng, mà check reconciliation vẫn pass vì cả hai vế đều 0.

### 2.1. Physical Storage Layout

| Attribute                 | Value       |
| ------------------------- | ----------- |
| Table Format              | `Iceberg`   |
| Partitioning Columns      | `date_key`  |
| Z-Order / Clustering Keys | None        |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là trạng thái error rate trailing 30 ngày của 1 merchant tính đến cuối 1 ngày.
> **Primary Key:** Composite (`date_key`, `merchant_id`) — không surrogate key (đồng bộ mọi aggregate fact khác trong repo).
> **Uniqueness test:** `dbt_utils.unique_combination_of_columns` trên (`date_key`, `merchant_id`)

**Cửa sổ trailing 30 ngày** = `(date_key − 29 ngày, date_key]` — 30 ngày dương lịch **tính cả ngày hiện tại**, cùng quy ước với cửa sổ 90 ngày của `fact_customer_activity_daily` (Decision Log 2026-07-23 của spec đó).

**Sàn volume nằm TRONG bảng.** Chỉ merchant có `transaction_count_30d >= var('abnormal_error_min_transaction_count')` (hiện 50) mới có dòng. Hệ quả phải biết:

- Bảng **không** là nguồn để đo mặt bằng portfolio hay phân bố volume của toàn bộ merchant — 98,4% merchant bị loại theo đúng thiết kế (calibration §5). Muốn phân bố đầy đủ thì đo từ `fact_daily_transaction_trend`.
- Nhưng cột `portfolio_error_rate_30d` (§5.1) **được tính trước khi áp sàn**, trên toàn bộ merchant của cửa sổ — nên mặt bằng đọc trong bảng vẫn là mặt bằng thật, không phải mặt bằng của nhóm đã lọc.
- "Merchant X không có trong bảng" là thông tin nhập nhằng: có thể vì X sạch lỗi, mà cũng có thể vì X quá nhỏ. Phân biệt hai trường hợp phải quay lại trend fact.

**Merchant không giao dịch vào đúng ngày D vẫn có dòng** nếu cửa sổ `(D−29, D]` của nó đạt sàn — đó chính là nghĩa của cửa sổ trailing. Ngược lại, sau 30 ngày không giao dịch merchant tự rụng khỏi bảng (count về 0 < sàn).

**Phạm vi merchant:** không lọc sentinel nào. Trend fact giữ toàn bộ dòng nguồn (kể cả bucket `mcc = '-1'`), và bảng này gộp qua **mọi** `mcc` của merchant — xem §6.

---
## 4. Data Lineage & Dependencies

### Upstream Dependencies

| Table/Source                        | Dependency Type    | Note                                                                 |
| ----------------------------------- | ------------------- | --------------------------------------------------------------------- |
| `gold.fact_daily_transaction_trend` | Hard (must finish)  | Nguồn measure **duy nhất**. Registry `metrics_layer.md` §2 chỉ định đây là nguồn của metric; các cột count đều additive nên gộp lên merchant/30 ngày là hợp lệ (quy tắc #6 §3 của registry) |
| `gold.dim_dates`                    | Hard (must finish)  | Date spine của cửa sổ trượt                                          |
| `gold.dim_merchant`                 | Soft (test-only)    | Chỉ dùng cho `relationships` test của `primary_mcc`; model không join lấy thuộc tính nào |

> **Vì sao không đọc thẳng `fact_transactions`:** trend fact đã ở đúng grain cần (merchant × ngày, sau khi gộp mcc) và nhỏ hơn một bậc, nên rolling window rẻ hơn nhiều. Đổi lại, mọi lỗi gộp của trend fact đều truyền xuống đây — đó là lý do check reconciliation ở §9 **cố ý tính lại từ `fact_transactions`** thay vì từ trend fact.

### Downstream Consumers

| Type         | Name                    | Note                                                                 |
| ------------ | ----------------------- | --------------------------------------------------------------------- |
| BI Artifact  | Dashboard A             | Error rate theo merchant — tín hiệu chất lượng trải nghiệm lọc ứng viên cashback (business spec §6, Decision #3) |
| Query        | Success criterion 6     | `select * from gold.rpt_merchant_error_daily where date_key = <D> and is_abnormal_error_rate order by excess_failed_transactions_30d desc` |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name                        | Data Type          | Key Type | Not Null | Transformation Logic                                                                                                       | Null Handling        | Allowed Range / Sample | Business Definition                                                                 |
| ---------------------------------- | ------------------ | -------- | -------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------------- | ----------------------- | ------------------------------------------------------------------------------------- |
| `date_key`                         | `IntegerType`      | PK, FK   | Y        | Date spine từ `dim_dates`                                                                                                    | Raise pipeline error  | `20100130`              | Ngày "tính đến cuối ngày này". FK → `dim_dates.date_key`; partition key.             |
| `merchant_id`                      | `StringType`       | PK, DD   | Y        | `fact_daily_transaction_trend.merchant_id`, gộp qua mọi `mcc`                                                                | Raise pipeline error  | `4802`                  | Degenerate dimension — không có merchant dimension (Decision #21). Merchant hiện ra bằng ID, không có tên. |
| `primary_mcc`                      | `StringType`       | FK       | Y        | `mcc` có `transaction_count` cao nhất của merchant **trong cửa sổ**; hòa thì lấy `mcc` nhỏ nhất theo thứ tự chuỗi (định định) | Raise pipeline error  | `5812`, có thể `-1`     | Ngành hàng **xấp xỉ** của merchant, để đọc shortlist không phải join thêm. FK → `dim_merchant.mcc`. **KHÔNG phải một chiều để group by** — xem cảnh báo dưới bảng. |
| `distinct_mcc_count`               | `IntegerType`      |          | Y        | Số `mcc` có ≥1 giao dịch của merchant trong cửa sổ                                                                          | Raise pipeline error  | ≥ 1                     | Cho biết `primary_mcc` xấp xỉ đến mức nào: `= 1` là chính xác, `> 1` là merchant đa ngành. |
| `window_day_count`                 | `IntegerType`      |          | Y        | Số ngày dương lịch thực có trong cửa sổ = `datediff(D, greatest(D − 29, history_start)) + 1`, với `history_start = min(date_key)` của trend fact | Raise pipeline error | 1..30 | Bằng 30 với mọi dòng trừ 29 ngày đầu lịch sử. Dòng có `< 30` là cửa sổ cụt — tỷ lệ vẫn tính đúng trên số ngày có thật, nhưng **không so sánh được** với dòng cửa sổ đủ. |
| `transaction_count_30d`            | `IntegerType`      |          | Y        | `sum(transaction_count)` của mọi dòng trend fact thuộc merchant trong cửa sổ, **qua mọi `mcc` kể cả `'-1'`**                 | Raise pipeline error  | ≥ sàn (hiện 50)         | Mẫu số của Error Rate. Cũng là cột phải đọc cạnh tỷ lệ để đánh giá độ mạnh bằng chứng. |
| `failed_transaction_count_30d`     | `IntegerType`      |          | Y        | `sum(failed_transaction_count)` cùng phạm vi                                                                                 | Raise pipeline error  | ≥ 0                     | Tử số của Error Rate — giao dịch mang bất kỳ error code nào (business spec §4, Decision #6). |
| `error_rate_30d`                   | `DecimalType(9,6)` |          | Y        | `failed_transaction_count_30d / transaction_count_30d`                                                                       | Không xảy ra (mẫu số ≥ sàn > 0) | `0.041237`   | **Error Rate của merchant trong cửa sổ** — định nghĩa business spec §4. Đây là ratio **được lưu**, khác trend fact — xem Decision Log. |
| `portfolio_error_rate_30d`         | `DecimalType(9,6)` |          | Y        | Tổng `failed` ÷ tổng `transaction` của **toàn bộ merchant** trong cùng cửa sổ, tính **trước** khi áp sàn. Hằng số theo `date_key`. | Raise pipeline error | `0.016090`   | Mặt bằng portfolio của chính cửa sổ đó — mốc so sánh, và là thứ ngưỡng 4,0% được calibrate dựa vào (2,49× mặt bằng 1,609%). |
| `expected_failed_transactions_30d` | `DecimalType(12,2)`|          | Y        | `transaction_count_30d * portfolio_error_rate_30d`                                                                           | Raise pipeline error  | ≥ 0                     | Số lỗi **đáng lẽ có** nếu merchant này chỉ tệ ngang mặt bằng.                        |
| `excess_failed_transactions_30d`   | `DecimalType(12,2)`|          | Y        | `failed_transaction_count_30d − expected_failed_transactions_30d`                                                            | Raise pipeline error  | có thể âm              | **Cột để BI `order by`.** Số lỗi vượt trội tuyệt đối. Xếp theo cột này thì merchant 485 giao dịch / 20 lỗi đứng trên merchant 51 giao dịch / 3 lỗi — đúng thứ tự bằng chứng, ngược với xếp theo `error_rate_30d`. |
| `total_spend_amount_30d`           | `DecimalType(18,2)`|          | Y        | `sum(total_spend_amount)` cùng phạm vi                                                                                       | Raise pipeline error  | ≥ 0                     | Chi tiêu khách hàng trong cửa sổ, số dương (cùng quy ước dấu với trend fact). Để phân biệt "tệ nhưng to" với "tệ nhưng nhỏ" — hai quyết định partnership khác nhau. |
| `applied_error_rate_threshold`     | `DecimalType(5,4)` |          | Y        | `var('abnormal_error_rate_threshold')` tại thời điểm build                                                                   | Raise pipeline error  | `0.0400`                | Ngưỡng đã áp cho dòng này. Carry ra cột để dữ liệu cũ tự nói được nó tính bằng tham số nào — tham số đã đổi 5% → 4,0% một lần rồi (Decision #24). |
| `applied_min_transaction_count`    | `IntegerType`      |          | Y        | `var('abnormal_error_min_transaction_count')` tại thời điểm build                                                            | Raise pipeline error  | `50`                    | Sàn volume đã áp. Mọi dòng thỏa `transaction_count_30d >= giá trị này` theo cấu trúc. |
| `is_abnormal_error_rate`           | `BooleanType`      |          | Y        | `error_rate_30d > applied_error_rate_threshold`                                                                              | Raise pipeline error  | `{true, false}`         | **Cờ Abnormal Error Rate của business spec §4.** Nửa "sàn volume" của định nghĩa đã nằm trong phạm vi bảng (§3), nên cờ này chỉ còn mang nửa ngưỡng. |

> **Cảnh báo cộng dồn (bắt buộc ghi trong yml):**
> 1. **`error_rate_30d` và `portfolio_error_rate_30d` là ratio — tuyệt đối không sum, không average.** Chúng chỉ đúng ở đúng grain này. Muốn error rate ở mức category/tuần/toàn hàng thì tính lại từ hai cột count ở mức đó (quy tắc #1 `metrics_layer.md` §3).
> 2. **Không sum bất kỳ cột `_30d` nào qua nhiều `date_key`.** Cửa sổ của hai ngày liền nhau chồng lấn 29 ngày — cộng chúng lại là đếm cùng một giao dịch tới 30 lần. Cột `_30d` chỉ đọc tại **một** `date_key`.
> 3. **`primary_mcc` không phải chiều để group by.** Nó là nhãn xấp xỉ cho merchant đa ngành (`distinct_mcc_count > 1`); group by nó rồi cộng sẽ gán trọn giao dịch của merchant cho một ngành duy nhất. Phân tích theo ngành hàng phải làm ở `fact_daily_transaction_trend`, nơi `mcc` là một phần của grain thật.
> 4. **Không so sánh dòng có `window_day_count < 30` với dòng cửa sổ đủ.**
> 5. **Xếp hạng theo `error_rate_30d` đẩy bằng chứng yếu lên đầu** — dùng `excess_failed_transactions_30d`, và luôn đọc `transaction_count_30d` cạnh tỷ lệ (calibration §5.1).

### 5.2. Schema Evolution Policy

> Chưa quyết định ở cấp dự án — đồng nhất với các spec khác.

---
## 6. Key Strategy & Special Members

> Không surrogate key (composite PK). Không seed Unknown member — bảng chỉ chứa merchant thật đã qua sàn volume.
>
> **Gộp qua `mcc` là bắt buộc, và bucket `'-1'` PHẢI được giữ** (`metrics_layer.md` §3 quy tắc #6, business spec Decision #16). Grain của trend fact là `date × mcc × merchant`, nên một merchant có thể nằm ở nhiều dòng cùng ngày. Bỏ sót dòng `mcc` khác của cùng merchant, hoặc loại bucket `'-1'` (giao dịch chưa resolve được ngành hàng — **vẫn là giao dịch của merchant đó**), đều làm **mẫu số thiếu ⇒ error rate bị thổi phồng ⇒ merchant bị gắn cờ oan**. Đây là lỗi nguy hiểm nhất mà model này có thể mắc, và là lý do tồn tại của check reconciliation ở §9.
>
> Hệ quả của việc giữ `'-1'`: `primary_mcc` có thể bằng `'-1'` nếu phần lớn giao dịch của merchant trong cửa sổ chưa resolve được ngành hàng. Đó là giá trị hợp lệ (`'-1'` là member đã seed của `dim_merchant`), không phải lỗi.

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                | Join Condition                              | Unmatched Key Handling |
| -------------- | --------------------------- | ------------------------------------------- | ----------------------- |
| `date_key`     | `gold.dim_dates.date_key`   | Spine sinh từ chính `dim_dates`             | Không phát sinh         |
| `primary_mcc`  | `gold.dim_merchant.mcc`     | Không join khi build — giá trị kế thừa từ `fact_daily_transaction_trend.mcc`, vốn đã có `relationships` test tới `dim_merchant`. Ở đây chỉ lặp lại test. | Không phát sinh; `'-1'` là member hợp lệ |
| `merchant_id`  | *(không có parent)*         | Degenerate dimension — không có merchant dimension (Decision #21) | — |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. `history_start` = `min(date_key)` của `fact_daily_transaction_trend` (scan riêng, **không** lọc theo ngày batch) — chỉ để tính `window_day_count`. Rẻ: Iceberg trả lời từ metadata partition.
2. Đọc `fact_daily_transaction_trend`. Ở nhánh `is_incremental()`, **lọc ngay ở bước đọc** về `date_key ∈ [D − 29, D]` với `D = batch_logical_date()` — khác `fact_customer_activity_daily`, ở đây filter đẩy xuống được vì không có cột nào cần toàn bộ lịch sử.
3. Dựng spine ở mức **(`merchant_id`, `mcc`, ngày)**: mỗi cặp (merchant, mcc) có 1 dòng cho mọi ngày từ ngày giao dịch đầu tiên của cặp đó đến `min(spine_end, ngày giao dịch cuối của cặp + 29)`, giao với khoảng đang build. Left join dữ liệu bước 2, ngày không giao dịch null-fill về 0.
   - Cắt spine ở `last_txn_day + 29` là **chính xác, không phải xấp xỉ**: sau đó cửa sổ trượt của cặp đó rỗng, `transaction_count_30d = 0 < sàn`, dòng bị loại ở bước 6 dù sao. Không cắt thì spine phình lên cỡ `merchant × mọi ngày lịch sử` (~10.4k × 3.590 ≈ 37 triệu dòng) cho một output ~165 dòng/ngày.
4. Rolling 30 ngày trên spine bằng window frame `rows between 29 preceding and current row` partition theo (`merchant_id`, `mcc`) — **không** dùng range-join spine × giao dịch: spine liền mạch 1 dòng/cặp/ngày nên frame theo dòng chính là 30 ngày dương lịch, và Spark biến non-equi join thành cartesian per-merchant.
5. Gộp lên grain output: `group by (date_key, merchant_id)`, `sum` các rolling sum qua mọi `mcc` (tổng của các rolling sum = rolling sum của tổng, nên phép này đúng), đồng thời lấy `primary_mcc` = `argmax` theo rolling `transaction_count` (tie-break `min(mcc)`) và `distinct_mcc_count` = số `mcc` có rolling count > 0.
6. Mặt bằng portfolio theo ngày: từ bảng tổng-theo-ngày (1 dòng/ngày, rất nhỏ) tính rolling 30 ngày của tổng `transaction_count` và tổng `failed_transaction_count` trên **toàn bộ merchant**, chia ra `portfolio_error_rate_30d`. **Bước này phải chạy trước bước 7** — mặt bằng lấy trên toàn portfolio, không phải trên nhóm đã qua sàn.
7. Áp sàn: giữ dòng có `transaction_count_30d >= var('abnormal_error_min_transaction_count')`.
8. Tính `error_rate_30d`, `expected_/excess_failed_transactions_30d`, `window_day_count`, hai cột `applied_*`, và `is_abnormal_error_rate`.
9. Lọc về partition ngày batch (chỉ ở nhánh incremental) — logic bước 3–8 **giữ nguyên hệt nhau ở cả hai nhánh**, jinja chỉ thu hẹp phạm vi đọc (bước 2) và phạm vi spine (bước 3). Một nhánh incremental viết bằng `group by` phẳng (không spine, không window) sẽ nhanh hơn nhưng tạo ra hai đường tính phải tự khớp nhau — không đánh đổi.
10. `insert_overwrite` partition `date_key`.

| Attribute           | Value                                                                                              |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| Merge / upsert keys | Không merge — overwrite nguyên partition `date_key`                                                 |
| Idempotency         | Mỗi partition ngày D chỉ phụ thuộc đúng 30 ngày trend fact → recompute độc lập, rerun idempotent    |
| Failure / retry     | Fail + halt khi vi phạm Critical checks (§9); an toàn retry                                         |

**Tham số** khai báo ở `dbt_project.yml`:

```yaml
vars:
  abnormal_error_rate_threshold: 0.04          # business spec Decision #24
  abnormal_error_min_transaction_count: 50     # business spec Decision #24
```

Đổi hai giá trị này **không** tự động sửa lịch sử — dòng cũ giữ nguyên `applied_*` của lần build ra chúng. Muốn áp tham số mới cho toàn lịch sử thì phải `--full-refresh`; đó là chủ ý, để một lần re-calibrate không âm thầm viết lại quá khứ.

### 8.2 Backfill & Historical Load Strategy

> Full history build ở lần chạy đầu / full-refresh, từ `min(date_key)` đến điểm kết thúc spine. **Điểm kết thúc spine** theo đúng tiền lệ `fact_customer_activity_daily` (Decision Log 2026-07-24 của spec đó): full-refresh = ngày giao dịch lớn nhất có thật trong trend fact; incremental = đúng `batch_logical_date()`, không phụ thuộc dữ liệu ngày đó.
>
> Bảng nằm trong chuỗi full-refresh restatement point-in-time (`transactions_fact.md` Decision Log v.0.0.3) vì phụ thuộc gián tiếp `fact_transactions`.
>
> Quy mô ước tính trên dev: ~165 dòng/ngày × ~3.590 ngày ≈ **590 nghìn dòng** toàn lịch sử.

### 8.3. Trạng thái implement (2026-07-25)

> **Đã implement VÀ ĐÃ CHẠY TRÊN DEV 2026-07-25 — 35/35 green cả ba lần, WARN=0, idempotent.** Số liệu đầy đủ ở `scripts/gold-dbt/README.md` §18. Verify offline trước đó gồm: `dbt parse` sạch; `dbt ls` xác nhận 30 schema test + 4 singular test attach; cả hai nhánh `is_incremental()` được render rồi parse bằng sqlglot dialect `spark`; đọc lại bản render để xác nhận hai jinja site khớp cửa sổ `[D−29, D]` và không có `where date_key =` nào lọt ra ngoài CTE `output_dates`.
>
> **Kết quả chạy dev — mọi con số trúng dự đoán:**
>
> | Câu hỏi | Dự đoán | Đo được |
> | ------- | ------- | ------- |
> | Mặt bằng `portfolio_error_rate_30d` | 1,609% (calibration §5) | **`0.016090`** — khớp đến chữ số cuối |
> | Số merchant gắn cờ tại `20191031` | ~12 (calibration đo 4) | **12**, nằm trong band 10–50 nên test `flagged_count_band` im lặng |
> | Số dòng toàn lịch sử | ~590 nghìn (§8.2) | **584.908** |
> | Số dòng/ngày | ~165 (§8.2) | **165** |
> | Chi phí full-refresh | chưa biết, rủi ro spine (§8.3) | **762s**, gấp ~1,4 lần `rpt_card_portfolio`; incremental chỉ ~178s |
> | Idempotency nhánh incremental | chưa biết | **PASS** — fingerprint giống hệt cả ba lần in |
>
> **Vì sao con số mặt bằng là bằng chứng mạnh, không chỉ là một số đẹp:** calibration đo 1,609% bằng một `group by` phẳng trên 30 `date_key` gần nhất, chạy ad-hoc ngày 2026-07-24 trước khi model tồn tại; model tính lại bằng rolling window trên spine (merchant, mcc). Hai đường tính không chia sẻ một dòng code nào mà cho cùng một số ⇒ **phép gộp qua `mcc` không sót dòng nào**, đúng thứ §6 gọi là lỗi nguy hiểm nhất bảng này có thể mắc. Cùng với check reconciliation (tính lại độc lập từ `fact_transactions`) pass, đây là hai xác nhận độc lập cho cùng một rủi ro.
>
> **Vẫn chưa chứng minh:** hành vi T+1 và chuyện "merchant rụng khỏi bảng sau 30 ngày im lặng" — nguồn synthetic dừng ở `20191031` nên không demo được (Open Question #4). Metric **chưa publish cho BI**.
>
> Lệnh chạy đã stage sẵn dạng comment ở **STEP 6d** của `scripts/gold-dbt/deploy_gold_dbt_dev.sh` (`scripts/gold-dbt/README.md` §13.1). Hai điều §13.1 nhấn mạnh và không nên bỏ qua: `--vars '{batch_logical_date: 2019-10-31}'` là **bắt buộc kể cả ở lần `--full-refresh`** — thiếu nó thì check reconciliation so 0 với 0 và **pass oan**; và sau lần chạy đầu phải **đọc** `portfolio_error_rate_30d` so với mốc 1,609% của calibration, vì lệch xa là chữ ký của lỗi gộp `mcc` ở §6.

**Ba quyết định phát sinh lúc viết code** — đều nằm trong khoảng spec để mở, ghi lại để không phải suy luận lại:

| # | Quyết định | Lý do |
| - | ---------- | ----- |
| 1 | `primary_mcc` lấy bằng `row_number() over (partition by date_key, merchant_id order by transaction_count_30d desc, mcc asc)` rồi `= 1`, **không** dùng `max_by` | §5.1 đòi tie-break "`mcc` nhỏ nhất theo thứ tự chuỗi" là **định định**; `max_by` của Spark không định nghĩa tie-break. |
| 2 | `expected_failed_transactions_30d` được cast về `decimal(12,2)` **trước**, rồi `excess = failed − expected` mới tính từ giá trị đã làm tròn đó | Nhờ vậy check "Excess consistency" ở §9 so khớp **tuyệt đối** thay vì phải mang dung sai. Vế `expected = count × rate` vẫn cần dung sai 1 xu, vế `excess` thì không. |
| 3 | Check "Flagged-count in operating band" **bỏ qua khi partition ngày batch rỗng** — sai lệch có chủ ý so với câu chữ §9 | Không truyền `--vars` thì `batch_logical_date()` rơi về `current_date()`, partition rỗng, count = 0 → ngoài band → warn nổi ở **mọi** lần chạy dev, huấn luyện người đọc bỏ qua đúng cái warn này. Việc bắt "model ghi 0 dòng" (kể cả bẫy `2036-01-01` ở §2) thuộc về check reconciliation, vốn so **tập dòng** chứ không so một con số đếm. Cùng cách xử lý và cùng câu chữ với check "Unknown MCC share" của trend fact. |

---
## 9. Data Quality & Observability Checks

| Check Name                      | Target Column                                              | Rule/Condition                                                                                                                                                     | Threshold      | Severity | Frequency | Action on Fail        | Alert Channel |
| ------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | -------- | --------- | ---------------------- | -------------- |
| Grain uniqueness                | `date_key`, `merchant_id`                                  | `dbt_utils.unique_combination_of_columns`                                                                                                                            | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Grain keys not null             | `date_key`, `merchant_id`                                  | not null                                                                                                                                                             | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Measure not null                | mọi cột measure/cờ ở §5.1                                  | not null                                                                                                                                                             | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Date FK                         | `date_key`                                                 | `relationships` → `dim_dates.date_key`                                                                                                                               | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| MCC FK                          | `primary_mcc`                                              | `relationships` → `dim_merchant.mcc` (bucket `'-1'` là member hợp lệ)                                                                                                | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Rate consistency                | `error_rate_30d`                                           | `error_rate_30d = failed_transaction_count_30d / transaction_count_30d` trên mỗi dòng (so sánh có dung sai làm tròn ở `DecimalType(9,6)`)                              | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Flag consistency                | `is_abnormal_error_rate`                                   | `is_abnormal_error_rate = (error_rate_30d > applied_error_rate_threshold)` trên mỗi dòng                                                                              | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Volume floor enforced           | `transaction_count_30d`                                    | `transaction_count_30d >= applied_min_transaction_count` trên mỗi dòng — sàn là một phần của phạm vi bảng (§3), không phải bộ lọc tùy chọn                            | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Excess consistency              | `excess_failed_transactions_30d`                           | `excess = failed_transaction_count_30d − expected_failed_transactions_30d` (**so khớp tuyệt đối**, xem §8.3 quyết định 2), và `expected = transaction_count_30d * portfolio_error_rate_30d` (dung sai 1 xu do làm tròn `DecimalType(12,2)`) | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Baseline uniform per day        | `portfolio_error_rate_30d`                                 | Đúng 1 giá trị phân biệt cho mỗi `date_key` — chặn lỗi lỡ tính mặt bằng sau khi áp sàn hoặc theo nhóm                                                                 | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| **Reconcile → fact_transactions** | `transaction_count_30d`, `failed_transaction_count_30d`, tập merchant | Singular test scope theo `batch_logical_date()`: tính lại **độc lập từ `gold.fact_transactions`** (không qua trend fact) tập merchant qualified và cặp count/failed của cửa sổ `(D−29, D]`, so khớp **tuyệt đối** cả hai chiều (thừa merchant và thiếu merchant đều fail) | 0 lệch         | Critical | Per run   | Pipeline fail + halt   |                |
| Window completeness             | `window_day_count`                                         | `window_day_count between 1 and 30`; và `= 30` với mọi `date_key >= history_start + 29`                                                                               | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Flagged-count in operating band | `is_abnormal_error_rate`                                   | Số merchant gắn cờ trong ngày batch nằm trong khoảng **~10–50** (tiêu chí vận hành §3 calibration). Ra ngoài khoảng = tín hiệu tham số đã trôi khỏi hiệu lực, **không phải lỗi dữ liệu**. **Bỏ qua khi partition ngày batch rỗng** — xem §8.3 quyết định 3 | ngoài 10–50    | **Warn** | Per run   | Ghi cảnh báo, xem lại calibration |  (dự án chưa có alert channel) |

> **Check reconciliation là check quan trọng nhất của bảng này.** Nó cố ý đi vòng qua trend fact và tính lại từ `fact_transactions`, vì cái nó tồn tại để bắt là lỗi gộp thiếu `mcc` hoặc rơi bucket `'-1'` (§6) — lỗi mà một check tính lại từ chính trend fact sẽ không bao giờ thấy. So khớp tuyệt đối là khả thi vì trend fact **không lọc bỏ dòng nguồn nào**, khác `fact_user_monthly_snapshot` và `fact_customer_activity_daily` (cả hai đều drop `customer_key = '-1'`).
>
> Check "Flagged-count in operating band" chạy severity `warn` vì dự án chưa có alert channel (`transactions_fact.md` Open Question #9), giống cách xử lý "Unknown MCC share" của trend fact.

---
## 10. Security & Governance

| Column          | PII Level | Masking / Encryption Rule                                                                                     | Data Retention / Purge Policy |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| Toàn bộ cột     | NSA       | Grain là merchant × ngày, không có cá nhân nào nhận dạng được — không cột nào mang PII khách hàng hay dữ liệu thẻ | Chưa gán (nhất quán với trend fact) |

> `TODO(security)`: dự án chưa migrate sang Lake Formation nên không có column-level policy nào được enforce ở đây; retention/purge chưa gán ở cấp dự án.
>
> Lưu ý nghiệp vụ (không phải bảo mật kỹ thuật): bảng này gắn nhãn **"bất thường"** lên các merchant có thật. Với ràng buộc bằng chứng ở §1 và §4 của calibration doc, kết quả không được dùng làm căn cứ chấm dứt quan hệ đối tác mà không có điều tra riêng — nó là đầu vào cho shortlist, đúng như Decision #22 đã đóng khung.

---
## 11. Open Questions & Decision Log
### Open Questions

| #   | Question                                                                                                                                                                          | Blocking? | Owner         | Status |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | --------------- | ------ |
| 1   | Mặt bằng `portfolio_error_rate_30d` lấy trên chính cửa sổ đang xét, nên nếu toàn portfolio xấu đi cùng lúc thì `excess` co lại và bảng im lặng đúng lúc cần nói. Có cần thêm một mặt bằng dài hạn (vd trailing 365 ngày) làm mốc thứ hai không? | No | NghiemCanCode | Open |
| 2   | Giới hạn đã biết và chấp nhận từ Decision #22: ngưỡng tĩnh **không** phát hiện merchant xấu đi từ 5% lên 9% — nó luôn bị gắn cờ ở cả hai mức. Nếu sau này cần "vừa mới xấu đi", phải thêm cột so với chính merchant đó chứ không sửa ngưỡng. | No | NghiemCanCode | Open |
| 3   | ~~`rpt_card_portfolio` (chip adoption + issued vs active) vẫn hoãn theo Decision #20~~ — **đóng 2026-07-25 cùng ngày**: đã có spec `docs/metrics/card_portfolio_report.md` (Decision #26 đảo nốt Decision #20), Open Question #1 ở `metrics_layer.md` đóng hoàn toàn. **Cập nhật 2026-07-25 (chiều):** `rpt_card_portfolio` **đã implement** (verify offline, chưa chạy dev) — model NÀY nay là bảng reporting duy nhất còn ở trạng thái specced-chưa-build. Bảng kia là tiền lệ gần nhất để tham chiếu khi implement bảng này: cùng khuôn `insert_overwrite` + `batch_logical_date()`, cùng nguyên tắc một-đường-tính cho hai nhánh jinja, nhưng **ngược nhau ở chuyện lưu ratio** (bảng kia không lưu vì là grain để-roll-up, bảng này có lưu vì là grain tiêu thụ cuối). **Cập nhật 2026-07-25 (tối):** bảng này nay **cũng đã implement và đã chạy dev** (§8.3) — cả hai bảng reporting đều green trên dev, không còn bảng nào ở trạng thái chờ. | No | NghiemCanCode | **Resolved** |
| 4   | Nguồn synthetic tĩnh dừng ở `20191031`, nên nhịp T+1 và hành vi "merchant rụng khỏi bảng sau 30 ngày im lặng" chưa demo được trên dữ liệu chạy thật. | No | NghiemCanCode | Open |
| 5   | Chưa có alert channel ở cấp dự án (`transactions_fact.md` Open Question #9) — check "Flagged-count in operating band" hiện chỉ ghi warn vào log của dbt. | No | NghiemCanCode | Open |

### Decision Log

| Date       | Decision                                                                                                      | Rationale                                                                                                                                                                                                                                                                                                              | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 2026-07-25 | Bảng **chỉ chứa merchant qua sàn ≥ 50** (~165 dòng/ngày), không chứa toàn bộ ~10.433 merchant có giao dịch      | Phương án "mọi merchant + cờ `meets_volume_floor`" cho ~37 triệu dòng toàn lịch sử — gấp ~9 lần `fact_transactions` (4,27tr) trong khi 98,4% là đuôi dài mà sàn sinh ra để dọn. Phương án "chỉ merchant bị gắn cờ" (~12 dòng/ngày) thì rẻ hơn nữa nhưng không trả lời được "vì sao merchant X không có trong danh sách" và mọi lần đổi ngưỡng đều phải rebuild. Nhóm qualified là đúng đơn vị phân tích mà calibration đã đo. | NghiemCanCode    |
| 2026-07-25 | **Toàn lịch sử, partition theo `date_key`** (insert_overwrite), không phải bảng chỉ-trạng-thái-mới-nhất          | Giữ đúng pattern của cả bốn fact insert_overwrite trong repo. Quan trọng hơn: lịch sử gắn cờ là thứ phân biệt merchant tệ kinh niên với merchant xui một tháng — chính là loại merchant mà Decision #22 nói "không được phép bỏ sót". Chi phí chấp nhận được vì phạm vi dòng đã hẹp (~590k dòng). | NghiemCanCode    |
| 2026-07-25 | Tham số sống ở **dbt vars** và đồng thời **carry ra cột** `applied_error_rate_threshold` / `applied_min_transaction_count` | Vars: re-calibrate sửa một chỗ, và override được lúc chạy thử. Carry ra cột: tham số đã đổi một lần rồi (5% → 4,0%, Decision #24), nên dữ liệu lịch sử phải tự nói được nó tính bằng tham số nào thay vì bắt người đọc tra git. Chi phí là 2 cột hằng số — rẻ hơn nhiều so với một lần đọc sai. | NghiemCanCode    |
| 2026-07-25 | Thêm `portfolio_error_rate_30d` + `expected_` + `excess_failed_transactions_30d` làm cột xếp hạng                | Calibration §5.1 cảnh báo xếp hạng theo tỷ lệ thô đẩy bằng chứng yếu lên đầu, và §5 chứng minh bằng đo 4b: top danh sách bị merchant n = 51–105 chiếm chỗ. Chỉ để `transaction_count` cạnh tỷ lệ (khuyến nghị nguyên văn của registry §2.1) là chống bẫy bằng kỷ luật người query, trong khi mặc định của mọi BI tool là `order by` tỷ lệ. `excess` cho một cột sắp xếp đúng ngay từ đầu. Chọn `excess` thay vì cận dưới Wilson vì persona Marketing giải thích được nó trong một câu ("thừa 12 lỗi so với mặt bằng"), còn Wilson thì không. Mặt bằng tính trong model nên tự cập nhật, không đóng băng con số 1,609% của lần calibrate. | NghiemCanCode    |
| 2026-07-25 | 29 ngày đầu lịch sử **vẫn ghi dòng**, kèm cột `window_day_count`                                                | Không mất dữ liệu, và cột nói rõ dòng nào có cửa sổ cụt. Phương án bỏ hẳn (bắt đầu từ 2010-01-30) sạch ngữ nghĩa hơn nhưng vứt đi dữ liệu mà không ai đòi vứt. Đánh đổi được ghi nhận: thêm một cột mà >99,9% dòng bằng 30, và một cảnh báo nữa vào `.yml` (quy tắc 4 §5.1). | NghiemCanCode    |
| 2026-07-25 | Carry `primary_mcc` (+ `distinct_mcc_count`), **không** carry `mcc` như một chiều                               | Shortlist 12 dòng toàn ID trần thì câu hỏi đầu tiên luôn là "merchant này bán gì" — bắt join lại trend fact cho mỗi lần đọc là ma sát vô ích ở đúng chỗ metric được tiêu thụ. `distinct_mcc_count` đi kèm để người đọc thấy ngay nhãn đó xấp xỉ đến mức nào. Rủi ro là có người group by `primary_mcc` — chặn bằng cảnh báo bắt buộc trong `.yml` (quy tắc 3 §5.1) chứ không bằng cách bỏ cột, vì bỏ cột thì họ tự chế nhãn còn tệ hơn. | NghiemCanCode    |
| 2026-07-25 | Carry `total_spend_amount_30d`, không carry `total_inflow_amount_30d`                                            | Gross value là một trong ba tiêu chí chấp nhận của calibration §3 — "tệ nhưng to" và "tệ nhưng nhỏ" là hai quyết định partnership khác nhau, và persona sẽ hỏi nó ngay khi nhìn shortlist. Cột additive, lấy bằng `sum` nên gần như miễn phí. Inflow (hoàn tiền) không tham gia câu hỏi nào của bảng này, nên không thêm chỉ để "cho đủ bộ như bảng nguồn". | NghiemCanCode    |
| 2026-07-25 | **Lưu** `error_rate_30d` như một cột — cố ý khác `fact_daily_transaction_trend`, nơi ratio bị cấm lưu             | Decision #15 cấm lưu ratio ở trend fact vì bảng đó là bảng **để roll-up**, và một ratio lưu sẵn ở grain đó chắc chắn sẽ bị average sai. Bảng này ngược lại: nó là **grain tiêu thụ cuối**, tỷ lệ tại merchant × cửa sổ 30 ngày chính là metric, và không lưu thì mọi consumer phải tự chia — đúng thứ mà reporting layer sinh ra để chấm dứt. Rủi ro roll-up vẫn còn nguyên và được chặn bằng hai cảnh báo bắt buộc trong `.yml` (quy tắc 1 và 2 §5.1), cộng check "Rate consistency" ở §9. | NghiemCanCode    |
| 2026-07-25 | Check reconciliation tính lại từ **`fact_transactions`**, không từ `fact_daily_transaction_trend`                | Nguồn build là trend fact, nên một check tính lại từ chính trend fact sẽ trùng lỗi với model và pass cả khi gộp sai `mcc` — đúng cái bẫy mà quy tắc #6 của registry cảnh báo và là lỗi nguy hiểm nhất của bảng này (§6). Đi vòng qua fact gốc là cách duy nhất để check độc lập. So khớp tuyệt đối khả thi vì trend fact không lọc bỏ dòng nguồn nào. | NghiemCanCode    |
| 2026-07-25 | Nhánh incremental dùng **cùng một đường tính** với full-refresh (spine + window frame), chỉ thu hẹp phạm vi đọc   | Một nhánh incremental viết bằng `group by` phẳng trên 30 ngày đã lọc sẽ nhanh hơn và ngắn hơn, nhưng tạo ra hai biểu thức tính phải tự khớp nhau vĩnh viễn — loại sai lệch mà test khó bắt vì cả hai đều "chạy green". Giữ một đường tính, jinja chỉ đổi phạm vi. | NghiemCanCode    |
| 2026-07-25 | Spine cắt ở `last_txn_day + 29` của mỗi cặp (merchant, mcc)                                                     | Cắt là chính xác chứ không xấp xỉ: quá điểm đó cửa sổ trượt rỗng và dòng bị sàn loại dù sao. Không cắt thì spine trung gian phình lên ~37 triệu dòng cho một output ~165 dòng/ngày — cùng loại bẫy chi phí mà `fact_customer_activity_daily` §8.1 tránh bằng window frame thay vì range join. | NghiemCanCode    |
| 2026-07-25 | Mặt bằng portfolio tính **trước** khi áp sàn                                                                    | Đúng cách calibration §5 đo mốc 1,609% (trên toàn bộ 10.433 merchant, không phải 165 merchant qualified). Tính sau sàn sẽ cho một mặt bằng cao hơn giả tạo và làm `excess` co lại có hệ thống. Chặn bằng check "Baseline uniform per day" + đặt thứ tự bước rõ ở §8.1. | NghiemCanCode    |
