# Technical Specification: Card Portfolio Report

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
| v.0.0.1 | 2026-07-25 | NghiemCanCode | Initial spec qua Q&A với Claude — "nhà" cho **Dashboard C (Card Portfolio)**: chip adoption, issued vs active, reissue count, success rate theo card type. Đảo nửa còn lại của business spec Decision #20 (→ Decision #26), đóng nốt Open Question #1 ở `metrics_layer.md`. Model **chưa được implement**. |
| v.0.0.3 | 2026-07-25 | NghiemCanCode | **Đã chạy trên dev** — 35/35 green cả ba lần (full-refresh 551,6s + hai lần incremental 187s), fingerprint giống hệt nhau ⇒ **idempotent**. Bốn ẩn số của v.0.0.2 nay có số thật: **Δ = 0** (đóng Open Question #4), **17.955 dòng** (§8.2 thay ước tính 35–55 nghìn — thấp hơn vì dữ liệu chỉ có 5 tổ hợp segment thật), chi phí as-of join, và idempotency. Ghi nhận: lần chạy này diễn ra 2026-07-25 nhưng **không được ghi vào `scripts/gold-dbt/README.md` §18 ngay**, nên spec này và registry còn ghi "chưa chạy dev" tới tối cùng ngày; khôi phục được nhờ `dbt/logs/dbt.log`. Known Limitation §3.1 giữ nguyên — as-of join vẫn chưa được chứng minh vì nguồn synthetic tĩnh. |
| v.0.0.2 | 2026-07-25 | NghiemCanCode | **Model đã implement** (`dbt/models/marts/reporting/rpt_card_portfolio.sql` + `.yml`, verify offline — **chưa chạy trên dev**). §9 sinh ra **4** singular test thay vì 3: nửa "= 90 ngoài 89 ngày đầu" của check Window completeness không diễn đạt được bằng `accepted_range` nên phải tách ra file riêng. Ba quyết định implement mới, ghi vào Decision Log: guard `model_range` cho test reconcile issued, loại cả seed `'-2'` khi map `card_key → card_id`, và `active_card_count_90d` tính bằng conditional sum thay vì `count(distinct)`. Không sửa grain, cột, hay quy tắc aggregate nào của v.0.0.1. |

---

## 1. Overview & Business Context

> **Purpose:** Vật chất hóa toàn bộ số liệu của **Dashboard C — Card Portfolio** (business spec §6): issued vs active cards, **Chip Adoption Rate** (§4), card reissue count, và transaction success rate theo card type. Trả lời trực tiếp **success criterion 5** ("What share of our card base is chip-enabled, and does chip vs. non-chip affect success rate?"). Trước spec này, Dashboard C phải tự join `dim_cards` × `fact_transactions`; phần success-rate-theo-card-type của phép join đó dính đủ ba bẫy — as-of (quy tắc #5 registry), ratio-average (quy tắc #1), và cửa sổ trượt — nên nó không còn là "một `count/count` không đáng gói" như Decision #20 từng mô tả.
> **Primary consumers:** Dashboard C (Card Portfolio — summary section), success criterion 5

| Attribute    | Value                                              | Description                                                     |
| ------------ | -------------------------------------------------- | --------------------------------------------------------------- |
| SCD Type     | None                                               | Derived report — rebuild theo partition                          |
| Special type | Derived / reporting model                          | Nguồn: `gold.dim_cards` (issued side) + `gold.fact_transactions` (active side) |
| Grain        | 1 dòng / (`date_key`, `chip_segment`, `card_brand`) | Trạng thái danh mục thẻ của 1 segment thẻ tính đến cuối 1 ngày   |

> **Bảng này KHÔNG lưu ratio nào.** Chip Adoption Rate và Transaction Success Rate đều tính downstream từ các cột count — vì grain này chắc chắn bị BI roll-up (gộp `card_brand` để ra chip adoption toàn hàng, gộp `chip_segment` để so brand), và mọi ratio lưu sẵn ở một grain để-roll-up sẽ bị average sai (quy tắc #1 registry, Decision #15 business spec). Đây là điểm **cố ý khác** `rpt_merchant_error_daily` — bảng đó là consumption grain cuối nên được lưu ratio; bảng này thì không phải.

---
## 2. Metadata & Operational Info

| Attribute        | Value                                                                 | Description                                |
| ---------------- | --------------------------------------------------------------------- | ------------------------------------------- |
| Table name       | `gold.rpt_card_portfolio`                                             | Model đặt tại `models/marts/reporting/` — layer cho các model mã hóa metric definitions (Decision #17 business spec) |
| Layer            | Gold (reporting)                                                      |                                            |
| Source(s)        | `gold.dim_cards` (issued side: point-in-time count + thuộc tính segment + reissue); `gold.fact_transactions` (active side: cửa sổ 90 ngày); `gold.dim_dates` (date spine); `gold.fact_customer_activity_daily` (chỉ để test đối chiếu Active Card — không join khi build) | |
| Load strategy    | Incremental (`insert_overwrite` partition theo `date_key`)            | Mỗi partition ngày D phụ thuộc trạng thái `dim_cards` tại D + đúng 90 ngày `fact_transactions` → recompute độc lập, idempotent |
| Watermark column | Không cần — partition overwrite theo `batch_logical_date()`           |                                            |
| Frequency        | Daily batch T+1 (business spec §7)                                    |                                            |
| Orchestrator     |                                                                       | Chưa quyết định ở cấp dự án                |
| SLA              | None                                                                  |                                            |

> **Vì sao active side đọc `fact_transactions` chứ không phải `fact_daily_transaction_trend`:** trend fact có `unique_cards` nhưng ở grain `date × mcc × merchant` và distinct count không cộng được qua grain (quy tắc #2 registry) — nó không biết thẻ nào là thẻ nào, càng không biết thẻ đó thuộc segment `has_chip × card_brand` nào. Đếm distinct thẻ theo thuộc tính thẻ bắt buộc quay về fact gốc, nơi `card_key` còn nguyên.
>
> **`batch_logical_date()` là ngày DỮ LIỆU, không phải ngày chạy batch** — cùng quy ước với mọi insert_overwrite fact khác. Một lần chạy T+1 vào ngày D+1 **phải** truyền `--vars '{batch_logical_date: <D>}'`, nếu không macro lấy default `current_date()` và ghi đè một partition rỗng.
>
> **Bẫy `2036-01-01`:** giá trị sentinel "process everything" của các script legacy PySpark **không bao giờ** được dùng làm `batch_logical_date` ở đây — `dim_dates` kết thúc 2035-12-31, cửa sổ resolve ra rỗng và model ghi 0 dòng trong im lặng.

### 2.1. Physical Storage Layout

| Attribute                 | Value       |
| ------------------------- | ----------- |
| Table Format              | `Iceberg`   |
| Partitioning Columns      | `date_key`  |
| Z-Order / Clustering Keys | None        |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là trạng thái danh mục thẻ của 1 segment (`chip_segment` × `card_brand`) tính đến cuối 1 ngày.
> **Primary Key:** Composite (`date_key`, `chip_segment`, `card_brand`) — không surrogate key (đồng bộ mọi aggregate fact khác trong repo).
> **Uniqueness test:** `dbt_utils.unique_combination_of_columns` trên (`date_key`, `chip_segment`, `card_brand`)

**"Card type" = `chip_segment` × `card_brand`** (chốt qua Q&A 2026-07-25): `chip_segment` trả lời trực tiếp success criterion 5; `card_brand` là cách persona Marketing gọi "loại thẻ". `has_a_cvv` cố ý **không** vào grain — gần như hằng số trong dữ liệu, chỉ làm phình grain (Open Question #1). `num_card_issue` không vào grain mà thể hiện bằng cột bucket (§5.1) — đưa vào grain thì nhân 3 số dòng, trái với "kept lightweight" của §6 business spec.

**Cửa sổ trailing 90 ngày** = `(date_key − 89 ngày, date_key]` — 90 ngày dương lịch **tính cả ngày hiện tại**, cùng quy ước và cùng độ rộng với định nghĩa Active Card của `fact_customer_activity_daily`. Mọi cột hậu tố `_90d` đọc trên cửa sổ này.

**Mỗi thẻ rơi vào đúng 1 segment mỗi ngày** — segment lấy từ phiên bản SCD2 **as-of cuối ngày `date_key`** (`effective_from_date <= cuối ngày D < effective_to_date`), cho cả issued side lẫn active side. Hệ quả cần biết:

- Issued và active cùng mẫu số phân loại → tỷ lệ active/issued đọc được trực tiếp trên từng dòng, và `sum(issued_card_count)` / `sum(active_card_count_90d)` qua segment không đếm đôi thẻ nào.
- Một thẻ đổi thuộc tính giữa cửa sổ 90 ngày được tính trọn cho segment **hiện tại** (as-of D) của nó, kể cả giao dịch phát sinh khi nó còn ở segment cũ. Đây là trade-off có chủ ý — phương án "gán theo `card_key` tại từng giao dịch" chính xác hơn về giao dịch nhưng làm một thẻ active ở CẢ HAI segment, sinh bẫy đếm đôi mới (Decision Log).
- Với nguồn tĩnh hiện tại (mỗi thẻ 1 phiên bản) hai phương án cho kết quả hệt nhau — khác biệt chỉ xuất hiện khi nguồn có thay đổi thuộc tính thật.

**Dải ngày sinh dòng:** từ ngày giao dịch đầu tiên của `fact_transactions` (không phải từ 1900-01-01 — xem Known Limitation §3.1) đến điểm kết thúc spine theo tiền lệ `fact_customer_activity_daily`: full-refresh = ngày giao dịch lớn nhất có thật; incremental = đúng `batch_logical_date()`.

**Segment nào có dòng:** mỗi ngày, mọi tổ hợp (`chip_segment`, `card_brand`) có `issued_card_count > 0` **hoặc** `transaction_count_90d > 0`. Tổ hợp trống cả hai không sinh dòng. Bucket (`UNKNOWN`, `UNKNOWN`) tồn tại chủ yếu nhờ giao dịch sentinel (§6) — nó có thể có `issued_card_count = 0` mà `transaction_count_90d > 0`; đó là dòng hợp lệ, không phải lỗi.

### 3.1. Known Limitation — đường "portfolio evolved over time" hiện là đường ngang

`dim_cards` backdate `effective_from_date` của version 1 về `1900-01-01` (Decision Log `cards_dimension.md` v.0.0.3), và nguồn synthetic tĩnh nên mỗi thẻ chỉ có đúng 1 phiên bản. Hệ quả: **`issued_card_count` point-in-time là một hằng số ở mọi `date_key`**, và Chip Adoption Rate theo thời gian là một đường phẳng. Đây không phải lỗi model — logic point-in-time là đúng và sẽ tự cho đường cong thật ngay khi nguồn có thay đổi thuộc tính (cùng gốc nguyên nhân với caveat criteria 2/3/4 ở business spec §9; unblock theo Open Question #2 của `docs/known_issues/dbt_spark_relation_cache.md`). Các cột **active side** (`active_card_count_90d`, các count giao dịch `_90d`) không bị giới hạn này — giao dịch có phân bố thời gian thật nên các đường đó có hình dạng thật. Câu này phải xuất hiện trong `.yml` của model.

---
## 4. Data Lineage & Dependencies

### Upstream Dependencies

| Table/Source                        | Dependency Type    | Note                                                                 |
| ----------------------------------- | ------------------- | --------------------------------------------------------------------- |
| `gold.dim_cards`                    | Hard (must finish)  | Issued side: đếm phiên bản có hiệu lực point-in-time; nguồn thuộc tính segment (`has_chip`, `card_brand`) và `num_card_issue`; đồng thời map `card_key` → `card_id` cho active side |
| `gold.fact_transactions`            | Hard (must finish)  | Active side: giao dịch trong cửa sổ 90 ngày (`card_key`, `is_error`)  |
| `gold.dim_dates`                    | Hard (must finish)  | Date spine                                                            |
| `gold.fact_customer_activity_daily` | Soft (test-only)    | Chỉ dùng cho singular test đối chiếu Active Card (§9); model không đọc khi build |

### Downstream Consumers

| Type         | Name                    | Note                                                                 |
| ------------ | ----------------------- | --------------------------------------------------------------------- |
| BI Artifact  | Dashboard C             | Issued vs active, chip adoption, reissue count, success rate theo card type (business spec §6) |
| Query        | Success criterion 5     | Chip adoption toàn hàng tại ngày D: `sum(issued_card_count) filter (where chip_segment = 'CHIP') / sum(issued_card_count)` trên các dòng `date_key = D`. Success rate theo chip: `sum(successful_transaction_count_90d) / sum(transaction_count_90d)` group by `chip_segment` tại `date_key = D`. |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name                        | Data Type          | Key Type | Not Null | Transformation Logic                                                                                                       | Null Handling        | Allowed Range / Sample | Business Definition                                                                 |
| ---------------------------------- | ------------------ | -------- | -------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------------- | ----------------------- | ------------------------------------------------------------------------------------- |
| `date_key`                         | `IntegerType`      | PK, FK   | Y        | Date spine từ `dim_dates`                                                                                                    | Raise pipeline error  | `20100130`              | Ngày "tính đến cuối ngày này". FK → `dim_dates.date_key`; partition key.             |
| `chip_segment`                     | `StringType`       | PK       | Y        | Từ `has_chip` của phiên bản as-of cuối ngày D: `true → 'CHIP'`, `false → 'NON_CHIP'`, `null → 'UNKNOWN'`. Giao dịch `card_key = '-1'` → `'UNKNOWN'`. | Không xảy ra (null đã map về `'UNKNOWN'`) | `{'CHIP', 'NON_CHIP', 'UNKNOWN'}` | Nửa thứ nhất của "card type". Mã hóa string thay vì boolean để grain key not-null được và bucket UNKNOWN có chỗ đứng tường minh. |
| `card_brand`                       | `StringType`       | PK       | Y        | `card_brand` của phiên bản as-of cuối ngày D (đã default `'UNKNOWN'` từ `dim_cards`). Giao dịch `card_key = '-1'` → `'UNKNOWN'`. | Không xảy ra (dim đã default) | `{'VISA', 'MASTERCARD', 'AMEX', 'DISCOVER', 'UNKNOWN'}` | Nửa thứ hai của "card type". Type 1 trong dim nên không tạo phiên bản SCD2 — nhưng vẫn đọc từ phiên bản as-of để cùng một phép join với `chip_segment`. |
| `issued_card_count`                | `IntegerType`      |          | Y        | Đếm phiên bản `dim_cards` có hiệu lực tại cuối ngày D thuộc segment (mỗi `card_id` đúng 1 phiên bản hiệu lực nhờ test coverage/no-overlap), **loại 2 seed member `'-1'`/`'-2'`** | Raise pipeline error | ≥ 0 | Số thẻ đã phát hành đang tồn tại trong segment tại ngày D — **point-in-time theo SCD2** (quy tắc #5 registry). Mẫu số của Chip Adoption Rate. **Là stock, không phải flow — không cộng qua ngày** (quy tắc #9 registry). |
| `active_card_count_90d`            | `IntegerType`      |          | Y        | `count(distinct card_id)` của giao dịch trong cửa sổ `(D−89, D]`, segment gán theo phiên bản as-of cuối ngày D; đếm trên **NK `card_id`** (map `card_key` → `card_id` qua `dim_cards`); giao dịch `card_key = '-1'` **loại khỏi phép đếm** (không có định danh thẻ) nhưng **vẫn tính** vào các count giao dịch | Raise pipeline error | ≥ 0 | Số thẻ Active (định nghĩa §4: ≥1 giao dịch trailing 90 ngày) trong segment tại ngày D. Nhà thứ hai của metric Active Card — nhà chính là `fact_customer_activity_daily`, hai bảng có test đối chiếu (§9). |
| `transaction_count_90d`            | `IntegerType`      |          | Y        | `count(*)` giao dịch trong cửa sổ thuộc segment (kể cả giao dịch `card_key = '-1'` → bucket UNKNOWN)                        | Raise pipeline error  | ≥ 0                     | Mẫu số của Transaction Success Rate theo card type.                                  |
| `successful_transaction_count_90d` | `IntegerType`      |          | Y        | `count(*) where is_error = false` cùng phạm vi                                                                               | Raise pipeline error  | ≥ 0                     | Tử số của Transaction Success Rate (business spec §4).                               |
| `failed_transaction_count_90d`     | `IntegerType`      |          | Y        | `count(*) where is_error = true` cùng phạm vi                                                                                | Raise pipeline error  | ≥ 0                     | Giao dịch mang bất kỳ error code nào (Decision #6). `successful + failed = transaction_count_90d` theo cấu trúc — có check §9. |
| `cards_reissued_0_count`           | `IntegerType`      |          | Y        | Đếm thẻ issued trong segment có `num_card_issue = 0` (phiên bản as-of cuối ngày D)                                           | Raise pipeline error  | ≥ 0                     | Phân bố reissue — thẻ chưa cấp lại lần nào.                                          |
| `cards_reissued_1_count`           | `IntegerType`      |          | Y        | Đếm thẻ issued có `num_card_issue = 1`                                                                                       | Raise pipeline error  | ≥ 0                     | Thẻ cấp lại đúng 1 lần.                                                              |
| `cards_reissued_2plus_count`       | `IntegerType`      |          | Y        | Đếm thẻ issued có `num_card_issue >= 2`                                                                                      | Raise pipeline error  | ≥ 0                     | Thẻ cấp lại từ 2 lần trở lên. Ba bucket cộng lại ≤ `issued_card_count` — thẻ có `num_card_issue` NULL (gồm bucket UNKNOWN) không rơi vào bucket nào. |
| `total_card_reissue_count`         | `IntegerType`      |          | Y        | `sum(num_card_issue)` trên thẻ issued của segment; NULL tính 0                                                               | Raise pipeline error  | ≥ 0                     | Tổng số lần cấp lại. BI tính "reissue trung bình" đúng bằng `sum(total_card_reissue_count) / sum(cards_reissued_*)` ở mức đang xem — **không** lưu sẵn avg (quy tắc #1). |
| `window_day_count`                 | `IntegerType`      |          | Y        | `datediff(D, greatest(D − 89, history_start)) + 1`, với `history_start = min(ngày giao dịch)` của `fact_transactions`         | Raise pipeline error  | 1..90                   | Bằng 90 với mọi dòng trừ 89 ngày đầu lịch sử. Dòng `< 90` là cửa sổ cụt — chỉ ảnh hưởng cột `_90d`, không ảnh hưởng issued side; không so sánh cột `_90d` của dòng cửa sổ cụt với dòng cửa sổ đủ. |

> **Cảnh báo cộng dồn (bắt buộc ghi trong yml):**
> 1. **`issued_card_count` là stock, không phải flow.** Nó đo "đang tồn tại tại ngày D", cộng qua ngày là đếm cùng một thẻ nhiều lần. Nguy hiểm hơn các cờ `_90d` vì tên cột không có hậu tố nào cảnh báo — quy tắc #9 registry sinh ra cho chính cột này. Các cột bucket reissue và `total_card_reissue_count` cùng bản chất stock, cùng quy tắc.
> 2. **Không sum bất kỳ cột `_90d` nào qua nhiều `date_key`.** Cửa sổ hai ngày liền nhau chồng lấn 89 ngày (quy tắc #10 registry, mở rộng #7). Cột `_90d` chỉ đọc tại **một** `date_key`. Muốn count giao dịch của một khoảng thời gian thì cộng từ `fact_daily_transaction_trend`.
> 3. **Không lưu ratio nào — và đừng tự lưu.** Chip adoption, success rate, reissue trung bình đều tính lại từ count ở đúng mức aggregate đang xem (quy tắc #1). Grain này để-roll-up, một cột rate lưu sẵn sẽ bị average qua brand/chip sai im lặng.
> 4. **`active_card_count_90d` cộng qua segment được (mỗi thẻ đúng 1 segment/ngày), cộng qua ngày thì không** — cùng lý do với quy tắc #4 registry.
> 5. **Không so sánh dòng có `window_day_count < 90` với dòng cửa sổ đủ.**
> 6. **Câu ràng buộc Known Limitation §3.1** (issued flat với nguồn tĩnh) phải có trong description của model.

### 5.2. Schema Evolution Policy

> Chưa quyết định ở cấp dự án — đồng nhất với các spec khác.

---
## 6. Key Strategy & Special Members

> Không surrogate key (composite PK). Không seed member riêng — bucket (`'UNKNOWN'`, `'UNKNOWN'`) hình thành tự nhiên từ dữ liệu, không phải dòng seed.
>
> **Giao dịch `card_key = '-1'` được GIỮ, vào bucket (`UNKNOWN`, `UNKNOWN`)** — theo tiền lệ Decision #16 business spec (giữ bucket `mcc = '-1'` của trend fact), **không** theo tiền lệ `fact_customer_activity_daily` (drop `customer_key = '-1'`). Lý do: một giao dịch lỗi trên thẻ chưa resolve vẫn là giao dịch lỗi thật — loại nó là làm mẫu số success rate thiếu, và tổng bảng không tie back về `fact_transactions` được nữa (check reconciliation §9 dựa trên chính tính chất giữ-đủ này). Khác với activity fact, ở đây có chỗ hợp lệ cho giao dịch vô danh: nó không cần định danh thẻ để đóng góp vào count giao dịch của một segment.
>
> Hệ quả trong bucket UNKNOWN: giao dịch sentinel đóng góp vào `transaction_count_90d`/`successful_`/`failed_` nhưng **không** vào `active_card_count_90d` (không có `card_id` để đếm distinct) và **không** vào `issued_card_count` (2 seed member của `dim_cards` bị loại khỏi phép đếm issued). Một dòng UNKNOWN có transaction count > 0 mà issued = active = 0 là hình dạng bình thường của bucket này. Thẻ THẬT có `has_chip` NULL (nếu có) cũng rơi vào `chip_segment = 'UNKNOWN'` và khi đó có issued/active bình thường.

---
## 7. Relationship & FK Resolution

| FK Column Name | Parent Table                | Join Condition                              | Unmatched Key Handling |
| -------------- | --------------------------- | ------------------------------------------- | ----------------------- |
| `date_key`     | `gold.dim_dates.date_key`   | Spine sinh từ chính `dim_dates`             | Không phát sinh         |
| `chip_segment` | *(không có parent)*         | Giá trị dẫn xuất từ `dim_cards.has_chip` — chặn bằng `accepted_values` | — |
| `card_brand`   | *(không có parent)*         | Kế thừa từ `dim_cards.card_brand` (đã có `accepted_values` ở dim) — lặp lại `accepted_values` tại đây | — |

**Hai phép join khi build (đều không fan-out):**

1. **Issued side / as-of cuối ngày D:** `dim_cards` với `effective_from_date <= cuối ngày D < effective_to_date` — mỗi `card_id` khớp đúng 1 phiên bản nhờ test no-overlap/coverage của dim.
2. **Active side:** `fact_transactions.card_key = dim_cards.card_key` (khớp surrogate chính xác, chỉ để lấy NK `card_id` — cùng kỹ thuật với `fact_customer_activity_daily` §7), rồi `card_id` → phiên bản as-of cuối ngày D để lấy segment. `card_key = '-1'` fail join → bucket UNKNOWN theo thiết kế.

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. `history_start` = `min(date_key)` của `fact_transactions` (scan riêng, không lọc theo ngày batch) — cho `window_day_count` và điểm bắt đầu spine. Rẻ: Iceberg trả lời từ metadata partition.
2. Dựng date spine từ `dim_dates`: từ `history_start` đến điểm kết thúc theo chế độ chạy (§3). Ở nhánh `is_incremental()`, spine thu về đúng 1 ngày `D = batch_logical_date()`.
3. **Issued side:** với mỗi ngày D của spine, join `dim_cards` theo as-of cuối ngày D (loại seed `'-1'`/`'-2'`), derive `chip_segment`, group by (D, `chip_segment`, `card_brand`) → `issued_card_count`, 3 bucket reissue, `total_card_reissue_count`.
4. **Active side:** đọc `fact_transactions`; ở nhánh incremental **lọc ngay ở bước đọc** về `date_key ∈ [D − 89, D]` — filter đẩy xuống được vì mỗi ngày output chỉ cần đúng 90 ngày dữ liệu (khác `fact_customer_activity_daily`, nơi cần toàn lịch sử). Map `card_key` → `card_id` (sentinel → NULL). Gộp về mức (`card_id`, ngày giao dịch) trước — count/success/fail mỗi ngày mỗi thẻ, dòng `card_id` NULL giữ riêng.
5. Với mỗi ngày D của spine: cửa sổ `(D−89, D]` trên kết quả bước 4 — count giao dịch là rolling sum (additive), `active_card_count_90d` là distinct `card_id` có hoạt động trong cửa sổ (kỹ thuật window frame trên spine thẻ × ngày, cùng khuôn `fact_customer_activity_daily` §8.1 — không range-join spine × giao dịch). Gán segment bằng join `card_id` → phiên bản as-of cuối ngày D.
6. Full outer join issued side (bước 3) với active side (bước 5) trên (D, `chip_segment`, `card_brand`); null-fill các measure về 0. Tính `window_day_count`.
7. Lọc về partition ngày batch (chỉ ở nhánh incremental) — logic bước 3–6 **giữ nguyên hệt nhau ở cả hai nhánh**, jinja chỉ thu hẹp phạm vi đọc và phạm vi spine (cùng nguyên tắc một-đường-tính với `rpt_merchant_error_daily` §8.1 bước 9).
8. `insert_overwrite` partition `date_key`.

| Attribute           | Value                                                                                              |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| Merge / upsert keys | Không merge — overwrite nguyên partition `date_key`                                                 |
| Idempotency         | Partition ngày D chỉ phụ thuộc trạng thái `dim_cards` + 90 ngày fact → recompute độc lập, rerun idempotent |
| Failure / retry     | Fail + halt khi vi phạm Critical checks (§9); an toàn retry                                         |

Không có tham số dbt vars mới — cửa sổ 90 ngày là một phần của định nghĩa Active Card (§4 business spec), không phải tham số điều chỉnh được; ranh giới bucket reissue (0/1/2+) hardcode (Decision Log).

### 8.2 Backfill & Historical Load Strategy

> Full history build ở lần chạy đầu / full-refresh. Bảng nằm trong chuỗi full-refresh restatement point-in-time (`transactions_fact.md` Decision Log v.0.0.3) vì phụ thuộc `fact_transactions` và `dim_cards`.
>
> Quy mô **đã đo trên dev 2026-07-25**: **17.955 dòng** toàn lịch sử, **5 dòng/ngày**. Thấp hơn ước tính ban đầu (35–55 nghìn) vì ước tính đó cho ≤ 15 tổ hợp segment/ngày, còn dữ liệu synthetic chỉ sinh ra **5** tổ hợp `(chip_segment, card_brand)` có thật. Vẫn là bảng nhỏ nhất trong các bảng reporting. Số liệu: `scripts/gold-dbt/README.md` §18.

---
## 9. Data Quality & Observability Checks

| Check Name                      | Target Column                                              | Rule/Condition                                                                                                                                                     | Threshold      | Severity | Frequency | Action on Fail        | Alert Channel |
| ------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | -------- | --------- | ---------------------- | -------------- |
| Grain uniqueness                | `date_key`, `chip_segment`, `card_brand`                   | `dbt_utils.unique_combination_of_columns`                                                                                                                            | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Grain keys not null             | `date_key`, `chip_segment`, `card_brand`                   | not null                                                                                                                                                             | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Segment values                  | `chip_segment`, `card_brand`                               | `accepted_values` — `{'CHIP','NON_CHIP','UNKNOWN'}` / `{'VISA','MASTERCARD','AMEX','DISCOVER','UNKNOWN'}`                                                            | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Measure not null                | mọi cột measure ở §5.1                                     | not null                                                                                                                                                             | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Date FK                         | `date_key`                                                 | `relationships` → `dim_dates.date_key`                                                                                                                               | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Success/fail additivity         | `successful_transaction_count_90d`, `failed_transaction_count_90d` | `successful + failed = transaction_count_90d` trên mỗi dòng                                                                                                    | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Reissue bucket consistency      | 3 cột bucket, `issued_card_count`                          | `cards_reissued_0_count + _1_count + _2plus_count <= issued_card_count` trên mỗi dòng (chênh lệch = thẻ có `num_card_issue` NULL)                                     | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |
| Active ≤ issued (segment thật)  | `active_card_count_90d`, `issued_card_count`               | `active_card_count_90d <= issued_card_count` trên mỗi dòng **trừ** bucket (`UNKNOWN`, `UNKNOWN`) — thẻ active tại D luôn có phiên bản hiệu lực tại D (coverage tới 9999) | 0 violations | Critical | Per run   | Pipeline fail + halt   |                |
| **Reconcile issued → dim_cards** | `issued_card_count`                                       | Singular test scope theo `batch_logical_date()`: `sum(issued_card_count)` tại D = số phiên bản `dim_cards` có hiệu lực tại cuối ngày D (loại seed member) — tính lại độc lập, so khớp tuyệt đối | 0 lệch | Critical | Per run   | Pipeline fail + halt   |                |
| **Reconcile transactions → fact_transactions** | các cột count `_90d`                        | Singular test scope theo `batch_logical_date()`: tổng `transaction_count_90d` / `successful_` / `failed_` qua mọi segment tại D = count tương ứng của **toàn bộ** giao dịch trong `(D−89, D]` từ `fact_transactions` — khả thi so khớp tuyệt đối vì bảng không drop dòng nào (kể cả sentinel, §6) | 0 lệch | Critical | Per run   | Pipeline fail + halt   |                |
| **Cross-check Active Card ↔ activity fact** | `active_card_count_90d`                        | Singular test scope theo `batch_logical_date()`: `sum(active_card_count_90d)` của bảng này tại D = `sum(active_card_count_90d)` của `fact_customer_activity_daily` tại D **+ Δ**, với `Δ` = số thẻ mà toàn bộ giao dịch trong cửa sổ đều có `customer_key = '-1'` (tính từ `fact_transactions` — activity fact drop các giao dịch đó nên không thấy các thẻ này). Đẳng thức tuyệt đối, không dung sai; trên dev hiện tại kỳ vọng `Δ = 0`. | 0 lệch | Critical | Per run   | Pipeline fail + halt   |                |
| Window completeness             | `window_day_count`                                         | `window_day_count between 1 and 90`; và `= 90` với mọi `date_key >= history_start + 89`                                                                              | 0 violations   | Critical | Per run   | Pipeline fail + halt   |                |

> **Check cross-check Active Card là lý do bảng này được phép tồn tại cạnh `fact_customer_activity_daily`.** Metric Active Card từ nay có hai nhà (nhà chính: activity fact, break down theo segment KHÁCH; nhà phụ: bảng này, break down theo thuộc tính THẺ), và registry §1 đòi "đúng 1 nơi vật chất hóa" — ngoại lệ này chỉ đứng vững khi có test chứng minh hai nhà không trôi khỏi nhau. Hai bảng dùng hai đường tính khác nhau (activity fact: distinct card theo khách; bảng này: distinct card theo segment as-of) nên test này là test chéo thật, không phải so một biểu thức với chính nó. Số hạng `Δ` làm đẳng thức chính xác tuyệt đối: hai bảng cố ý khác nhau ở đúng một chỗ — activity fact drop giao dịch `customer_key = '-1'` (spec của nó §6), bảng này giữ (§6 ở đây) — và `Δ` đo đúng chỗ khác nhau đó. Test này đồng thời bắt được cả giả định "mỗi thẻ thuộc đúng 1 khách" mà activity fact dựa vào khi cộng `active_card_count_90d` qua khách: thẻ xuất hiện dưới 2 khách sẽ làm vế activity fact lớn hơn và test fail.
>
> Check reconcile transactions cố ý so với `fact_transactions` chứ không với trend fact — cùng nguyên tắc "check phải độc lập với đường build" của `rpt_merchant_error_daily` §9; ở đây fact gốc cũng chính là nguồn build nên check chủ yếu bắt lỗi rơi dòng khi gán segment (join as-of sai làm mất giao dịch thay vì đẩy về UNKNOWN).

---
## 10. Security & Governance

| Column          | PII Level | Masking / Encryption Rule                                                                                     | Data Retention / Purge Policy |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| Toàn bộ cột     | NSA       | Grain là segment thẻ × ngày — mọi cột đều là count gộp, không cá nhân hay thẻ đơn lẻ nào nhận dạng được; không mang PAN/CVV/PII (tuân Compliance note business spec §8) | Chưa gán (nhất quán với các bảng reporting khác) |

> `TODO(security)`: dự án chưa migrate sang Lake Formation nên không có column-level policy nào được enforce; retention/purge chưa gán ở cấp dự án.

---
## 11. Open Questions & Decision Log
### Open Questions

| #   | Question                                                                                                                                                                          | Blocking? | Owner         | Status |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | --------------- | ------ |
| 1   | `has_a_cvv` cố ý loại khỏi grain (gần như hằng số trong dữ liệu). Nếu sau này cần phân tích theo CVV, thêm nó vào grain là breaking change của PK — cân nhắc bảng riêng thay vì sửa grain. | No | NghiemCanCode | Open |
| 2   | `expires_month`/`expires_year` có trong `dim_cards` nhưng bảng này chưa có measure "thẻ hết hạn" (expired share tại ngày D). §3 business spec không hỏi, nhưng là mở rộng tự nhiên nếu Dashboard C cần. | No | NghiemCanCode | Open |
| 3   | Known Limitation §3.1: issued side phẳng cho tới khi nguồn sinh được thay đổi thuộc tính (`docs/known_issues/dbt_spark_relation_cache.md` Open Question #2). Khi unblock, cần verify lại là đường cong nhúc nhích thật. | No | NghiemCanCode | Open |
| 4   | Số hạng `Δ` của cross-check Active Card kỳ vọng = 0 trên dev hiện tại (chưa quan sát thấy giao dịch resolve được thẻ mà không resolve được khách). Nếu `Δ > 0` thường trực, cân nhắc ghi `Δ` ra một cột audit thay vì chỉ nằm trong test. **Cập nhật 2026-07-25:** test đã implement (`dbt/tests/rpt_card_portfolio_active_card_cross_check.sql`, Δ tính bằng `max(case when customer_key != '-1' ...)` group by `card_id`) nhưng **chưa đo**. **Đã đo cùng ngày (STEP 6c, `scripts/gold-dbt/README.md` §18): `Δ = 0`, `residual = 0`, hai bảng cùng cho `active = 3.385` — đúng kỳ vọng.** Không cần cột audit. Lưu ý phạm vi: đây là số của **một** `date_key` (20191031) trên nguồn synthetic tĩnh; Δ có thể khác 0 nếu nguồn về sau sinh ra giao dịch resolve được thẻ mà không resolve được khách — khi đó singular test sẽ fail và câu hỏi này mở lại. | No | NghiemCanCode | **Resolved 2026-07-25** — Δ đo được = 0 |
| 5   | Chưa có orchestrator/alert channel ở cấp dự án (`transactions_fact.md` Open Question #9).                                                                                          | No | NghiemCanCode | Open |

### Decision Log

| Date       | Decision                                                                                                      | Rationale                                                                                                                                                                                                                                                                                                              | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 2026-07-25 | **Phạm vi = toàn bộ Dashboard C**, đảo nửa còn lại của Decision #20 (→ business spec Decision #26)              | Decision #20 hoãn `rpt_card_portfolio` với lý do "chip adoption chỉ là count/count trên dim_cards, không có bẫy aggregate" — đúng cho chip adoption đứng một mình, nhưng Dashboard C và success criterion 5 đòi cả success rate theo card type, thứ buộc join `fact_transactions` × `dim_cards` as-of và dính quy tắc #1/#5 registry. Một khi bảng phải tồn tại cho phần khó, gói luôn issued/active/reissue vào cùng grain gần như miễn phí và cho Dashboard C đúng 1 nguồn SELECT. | NghiemCanCode |
| 2026-07-25 | Grain = `date_key × chip_segment × card_brand`, không phải `date × card_key` và không phải snapshot-hiện-tại      | `date × card_key` (~6k thẻ × ~3.6k ngày) là bảng hàng chục triệu dòng cho một "summary section" §6 nói rõ kept lightweight, và BI vẫn phải tự aggregate — tức vẫn tự dẫm bẫy. Snapshot-hiện-tại thì mất chiều thời gian, không trả lời "how has the portfolio evolved" (§3 business spec). Segment × ngày là điểm giữa: vài chục dòng/ngày, additive qua segment, giữ lịch sử. | NghiemCanCode |
| 2026-07-25 | "Card type" = `has_chip` × `card_brand`; loại `has_a_cvv` và `num_card_issue` khỏi grain                         | `has_chip` trả lời success criterion 5 nguyên văn; `card_brand` là nghĩa đời thường của "loại thẻ" với persona Marketing; 2×5 tổ hợp giữ bảng nhỏ. `has_a_cvv` gần như hằng số — thêm chỉ phình grain (Open Question #1). `num_card_issue` là câu hỏi phân bố, trả lời bằng cột bucket rẻ hơn nhân 3 grain. | NghiemCanCode |
| 2026-07-25 | `chip_segment` mã hóa string `{'CHIP','NON_CHIP','UNKNOWN'}` thay vì boolean nullable                            | Grain key phải not-null test được, và bucket UNKNOWN cần chỗ đứng tường minh thay vì "NULL nghĩa là gì thì đọc doc". Trade-off: lệch kiểu với `dim_cards.has_chip` (boolean) — chấp nhận, vì đây là cột trình bày của reporting layer, không phải cột dimension. | NghiemCanCode |
| 2026-07-25 | Issued count = **point-in-time theo SCD2** (phiên bản hiệu lực tại cuối ngày D), không phải `is_current` lặp lại mỗi ngày | `is_current` gán trạng thái thẻ hôm nay cho mọi ngày quá khứ — nguyên văn lỗi mà quy tắc #5 registry cấm, và làm đường chip adoption phẳng **vĩnh viễn** kể cả khi nguồn có thay đổi thật. Point-in-time đúng ngay bây giờ và tự cho đường cong thật khi nguồn unblock. Phương án "ngày phát hành thật" bất khả thi: `dim_cards` không có cột issue date. | NghiemCanCode |
| 2026-07-25 | Thẻ active gán segment theo phiên bản **as-of cuối ngày D**, không theo `card_key` của từng giao dịch             | Mỗi thẻ đúng 1 segment/ngày → issued và active cùng mẫu số, tỷ lệ active/issued đọc trực tiếp trên dòng, sum qua segment không đếm đôi. Phương án theo-giao-dịch chính xác hơn về transaction nhưng một thẻ đổi chip giữa cửa sổ sẽ active ở cả hai segment — sinh một bẫy đếm đôi mới phải document và test. Với nguồn tĩnh hai phương án trùng kết quả; chọn phương án ít bẫy hơn. | NghiemCanCode |
| 2026-07-25 | Cột `_90d` dùng cửa sổ **trailing 90 ngày** khớp định nghĩa Active Card, không phải 30 ngày và không phải daily   | Mọi con số trên một dòng nói về cùng một cửa sổ — active 90d cạnh success rate 30d là mời đọc nhầm. Daily thì additive sạch nhưng bắt BI tự cộng — đúng thứ reporting layer sinh ra để chấm dứt; tổng theo khoảng bất kỳ đã có `fact_daily_transaction_trend` lo. Trade-off ghi nhận: thêm bẫy cửa sổ trượt → quy tắc #10 registry. | NghiemCanCode |
| 2026-07-25 | **Không lưu ratio** — cố ý khác `rpt_merchant_error_daily`                                                       | Bảng merchant là consumption grain cuối (mỗi dòng một merchant, không ai roll-up tiếp) nên lưu `error_rate_30d` được. Bảng này ngược lại: cách dùng chuẩn là roll-up (gộp brand ra chip adoption toàn hàng, gộp chip ra so sánh brand) — một cột rate lưu sẵn ở grain để-roll-up chắc chắn có ngày bị average (quy tắc #1). Hai bảng reporting chọn khác nhau vì hình dạng tiêu thụ khác nhau; cả hai cùng một nguyên tắc. | NghiemCanCode |
| 2026-07-25 | Reissue = 3 bucket (0/1/2+) + `total_card_reissue_count`; ranh giới bucket hardcode                              | Bucket cho phân bố ("how frequently are cards reissued" §3), sum + count cho trung bình đúng ở mọi mức aggregate; chỉ-avg thì vừa là ratio lưu sẵn vừa mất phân bố; chỉ sum+count thì mất phân bố. Ranh giới 0/1/2+ hardcode chứ không vars: đổi ranh giới là đổi NGHĨA cột (breaking change phải đi qua spec), không phải tinh chỉnh tham số như ngưỡng 4% của bảng merchant. | NghiemCanCode |
| 2026-07-25 | Giao dịch `card_key = '-1'` giữ vào bucket (`UNKNOWN`,`UNKNOWN`) — theo tiền lệ Decision #16, không theo tiền lệ activity fact | Giao dịch lỗi trên thẻ chưa resolve vẫn là lỗi thật; drop làm mẫu số success rate thiếu và phá khả năng reconcile tuyệt đối với `fact_transactions`. Activity fact drop được vì grain của nó là KHÁCH — không có khách thì dòng vô nghĩa; ở đây grain là segment thẻ, và "chưa biết thẻ nào" chính là một segment hợp lệ. | NghiemCanCode |
| 2026-07-25 | Spine từ **ngày giao dịch đầu tiên**, không phải từ 1900-01-01                                                   | Backdate 1900 của `dim_cards` là kỹ thuật phục vụ as-of join, không phải sự kiện nghiệp vụ "phát hành thẻ năm 1900" — sinh ~40 nghìn ngày × segment toàn giá trị lặp là rác. Ngày giao dịch đầu tiên là mốc sớm nhất mà bảng có thông tin thật để nói. | NghiemCanCode |
| 2026-07-25 | Giữ point-in-time + ghi **Known Limitation** (§3.1) cho hiện tượng issued-phẳng, thay vì chế proxy issue date     | "Ngày giao dịch đầu của thẻ = ngày phát hành" là một định nghĩa bịa: thẻ phát hành chưa dùng biến mất khỏi mẫu số, chip adoption lệch không kiểm soát được. Nói thẳng "đường này phẳng vì nguồn tĩnh, không phải lỗi model" trung thực hơn — cùng giọng với caveat criteria 2/3/4 của business spec §9. | NghiemCanCode |
| 2026-07-25 | Active Card có **hai nhà** (nhà chính: `fact_customer_activity_daily`; nhà phụ: bảng này) + cross-check test bắt buộc | Registry §1 đòi "đúng 1 nơi vật chất hóa", nhưng hai câu hỏi chính đáng cần hai grain: "active theo segment khách" (Dashboard B) và "active theo thuộc tính thẻ" (Dashboard C) — không grain nào derive được từ grain kia vì distinct count không cộng qua grain (quy tắc #2). Ngoại lệ chỉ đứng vững kèm test đối chiếu tuyệt đối (§9) — hai đường tính độc lập phải ra cùng con số, trôi là fail ngay thay vì âm thầm. | NghiemCanCode |
| 2026-07-25 | `insert_overwrite` partition `date_key`, một đường tính cho cả hai nhánh                                          | Cùng khuôn với cả bốn insert_overwrite fact của repo; partition ngày D độc lập hoàn toàn (dim state + 90 ngày fact) nên rerun idempotent. Nhánh incremental không viết đường tính riêng — jinja chỉ thu hẹp phạm vi đọc/spine (nguyên tắc đã chốt ở `rpt_merchant_error_daily`: hai đường tính phải tự khớp nhau vĩnh viễn là loại sai lệch test khó bắt). | NghiemCanCode |
| 2026-07-25 (implement) | Test reconcile issued mang guard **`model_range`**: chỉ so khớp khi ngày batch nằm trong `min(date_key)..max(date_key)` mà model thật sự đã build | Test này không thể rơi về `0 = 0` như hai test reconciliation cùng họ: vế nguồn của chúng là scan `fact_transactions` có lọc ngày (rỗng ngoài lịch sử), còn vế nguồn ở đây là `dim_cards`, có phiên bản hiệu lực ở **mọi** ngày kể cả hôm nay. Không guard thì một lần `--full-refresh` không truyền `--vars` (macro rơi về `current_date()`) sẽ so `0` với toàn bộ danh mục thẻ và fail trên một lần chạy không làm gì sai. Guard theo *khoảng đã build* chứ không theo "có dòng cho ngày D" — vì cách sau chính là bug silent-pass mà `coalesce(...,0)` sinh ra để chống: partition thiếu ở **giữa** khoảng vẫn nằm trong `min..max` và vẫn fail ồn ào. | NghiemCanCode |
| 2026-07-25 (implement) | Khi map `card_key → card_id` cho active side, loại **cả hai** seed member `'-1'` và `'-2'`, không chỉ `'-1'` như §5.1 viết | Seed `'-2'` mang `card_id = 'NOT APPLICABLE'` — một natural key trông như thật. Nếu một giao dịch nào đó resolve về `card_key = '-2'`, nó sẽ vào `active_card_count_90d` như một "thẻ" giả thay vì rơi về bucket UNKNOWN. Trên dữ liệu hiện tại `fact_transactions` chỉ dùng fallback `'-1'` nên hai cách cho kết quả hệt nhau; chặt hơn ở đây không mất gì. Activity fact chỉ loại `'-1'` vì nó đi qua `customer_key`, không phải cùng tình huống. | NghiemCanCode |
| 2026-07-25 (implement) | `active_card_count_90d` tính bằng **conditional sum** trên spine thẻ × ngày, không phải `count(distinct card_id)` | Sau bước window, mỗi (thẻ, ngày) đúng 1 dòng, nên `sum(case when transaction_count_90d > 0 then 1 else 0 end)` **chính là** distinct count — rẻ hơn `count(distinct)` (không cần shuffle theo card_id) và loại bỏ hoàn toàn khả năng một thẻ bị đếm ở hai segment. Ghi lại vì §8.1 bước 5 nói "distinct `card_id`" nên đọc code dễ tưởng thiếu. Khác `fact_customer_activity_daily`, nơi phải dùng `collect_set`/`flatten` vì grain của nó là KHÁCH nên một dòng chứa nhiều thẻ. | NghiemCanCode |

---

> **Trạng thái implement: đã build VÀ ĐÃ CHẠY TRÊN DEV 2026-07-25 — 35/35 green cả ba lần chạy, Δ = 0.** Model `dbt/models/marts/reporting/rpt_card_portfolio.sql` + `rpt_card_portfolio.yml` (description chứa đủ 6 cảnh báo §5.1 và câu Known Limitation §3.1; 30 schema test attach). §9 sinh ra **4** singular test tại `dbt/tests/` — ba check reconcile/cross-check như spec dự kiến, cộng `rpt_card_portfolio_window_completeness.sql` cho nửa "= 90 ngoài 89 ngày đầu" mà `accepted_range` không diễn đạt được. Verify: `dbt parse` + `dbt ls` + render cả hai nhánh jinja rồi parse offline bằng sqlglot dialect `spark`.
>
> **Kết quả chạy dev 2026-07-25** (chi tiết + giờ giấc ở `scripts/gold-dbt/README.md` §18) — bốn thứ trước đó "chưa chứng minh được" nay đã có số:
>
> | Câu hỏi | Kết quả |
> | ------- | ------- |
> | Δ của cross-check (Open Question #4 kỳ vọng 0) | **Δ = 0**, `residual = 0`, `report_active = activity_active = 3.385` ⇒ trên dev không có thẻ nào mà mọi giao dịch trong cửa sổ đều mang `customer_key = '-1'`. Open Question #4 **đóng**. |
> | Số dòng thật | **17.955** (5 dòng/ngày) — §8.2 đã cập nhật, thấp hơn ước tính 35–55 nghìn vì chỉ 5 tổ hợp segment tồn tại thật. |
> | Chi phí as-of range join broadcast trên full refresh | 551,6s cho full-refresh; 187,1s và 187,3s cho hai lần incremental. |
> | Idempotency nhánh incremental | **PASS** — fingerprint giống hệt nhau cả ba lần in (trước / giữa / sau hai lần chạy incremental). |
>
> Nhật ký chạy này **suýt bị mất**: nó không được ghi vào §18 ngay sau khi chạy, nên tới tối 2026-07-25 cả người chạy lẫn `metrics_layer.md` đều còn ghi "chưa chạy dev"; chỉ khôi phục được nhờ `dbt/logs/dbt.log` chưa bị xoá. Đây là lý do §17.2 của README tồn tại.
>
> **Vẫn chưa chứng minh:** tính đúng của as-of join (nguồn synthetic tĩnh nên mỗi thẻ chỉ có 1 phiên bản — test SCD2 hiện pass rỗng nghĩa, và Known Limitation §3.1 giữ nguyên: đường chip adoption vẫn phẳng).
