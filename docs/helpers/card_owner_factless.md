# Technical Specification: Card Owner Factless

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ----------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.3       |                                                |
| Owner (table) |               |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-24    |                                                |
### Changelog

| Version | Date       | Author        | Change                                                                                 |
| ------- | ---------- | ------------- | --------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft (title/table name mix "Account" vs "Card", copy-paste leftover từ fact_transactions) |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại toàn bộ theo Q&A (xem mục 11 Decision Log) — chốt entity là card, grain là current-only pairs, không SCD2 |
| v.0.0.3 | 2026-07-24 | NghiemCanCode | Implement thành dbt model. Bổ sung quyết định loại special member `-1`/`-2` khỏi cả hai phía join; sửa lại rationale đã lỗi thời của Decision Log #2 (fact_transactions nay dùng as-of join, không còn `is_current`) |

---

## 1. Overview & Business Context

> **Purpose:** Bảng này không đo lường sự kiện — nó **capture một mối quan hệ tồn tại**: customer nào đang sở hữu card nào, tại trạng thái hiện tại. Câu hỏi nghiệp vụ mà bảng này trả lời:
> - *"Card này đang thuộc về customer nào?"*
> - *"Customer này đang sở hữu những card nào?"*
>
> Đây là **factless fact table** (Kimball) — không có measure, chỉ ghi nhận sự tồn tại của quan hệ ownership giữa `gold.dim_customers` và `gold.dim_cards`, cả hai đều là SCD2 nhưng được resolve theo **phiên bản hiện tại** (`is_current = true`) — xem Decision Log #1, #2 và bản đính chính 2026-07-24.
>
> ⚠️ **Lưu ý:** kể từ khi `fact_transactions` được restate sang as-of range join (`transactions_fact.md` v.0.0.3, 2026-07-23), bảng này là **nơi duy nhất trong gold còn resolve dimension theo `is_current`**. Quyết định giữ current-only vẫn đứng vững (chưa consumer nào cần point-in-time ownership), nhưng nó không còn "nhất quán với `fact_transactions`" như bản v.0.0.2 viết.
> **Primary consumers:** Chưa xác định — hiện tại chưa có BI dashboard/data product nào dùng trực tiếp; xem Open Question #1.

| Attribute    | Value                                          | Description |
| ------------ | ----------------------------------------------- | ----------- |
| SCD Type     | None                                             | Ownership được coi là cố định (1 card ↔ 1 customer vĩnh viễn) ở tầng này; xem Decision Log #3, #4. |
| Special type | Factless Fact (bridge, current-state snapshot)   | Không có measure; chỉ ghi nhận sự tồn tại quan hệ ownership. |
| Grain        | Mỗi dòng là 1 cặp `(customer_key, card_key)` đang là phiên bản hiện tại của cả hai dimension | |

---
## 2. Metadata & Operational Info

| Attribute        | Value                        | Description                                                                                     |
| ----------------- | ----------------------------- | --------------------------------------------------------------------------------------------------- |
| Table name        | `gold.card_owner_factless`    | Sửa lại từ tiêu đề "Account Owner Factless" — project không có `dim_accounts`, entity duy nhất là card (xem Decision Log #1). |
| dbt model          | `dbt/models/marts/card_owner_factless.sql` | Schema/test: `card_owner_factless.yml`; 2 singular test trong `dbt/tests/`. Lệnh chạy (comment sẵn) ở STEP 3 của `scripts/gold-dbt/deploy_gold_dbt_dev.sh`. |
| Layer              | Gold                           |                                                                                                       |
| Source(s)          | `gold.dim_cards`, `gold.dim_customers` (chỉ dòng `is_current = true`, đã loại special member `-1`/`-2`) | Join trên `dim_cards.customer_id = dim_customers.customer_id`. Không đọc trực tiếp từ silver. |
| Load strategy      | Full refresh (truncate + reload mỗi run) | Bảng derive hoàn toàn từ trạng thái hiện tại của 2 dimension, không phải sự kiện bất biến để append (xem Decision Log #5). Trong dbt: `materialized: table` (kế thừa từ `marts` trong `dbt_project.yml`) — trên dbt-spark là create-or-replace, đúng nghĩa truncate + reload. Không bao giờ cần `--full-refresh` hay `--vars`. |
| Watermark column   | None                           | Không cần — full refresh không dùng watermark.                                                     |
| Frequency          |                                | Chưa quyết định ở cấp dự án (giống mọi bảng gold khác hiện tại).                                    |
| Orchestrator       |                                | Chưa quyết định ở cấp dự án.                                                                        |
| SLA                | None                           |                                                                                                       |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| -------------------------- | --------- |
| Table Format                | `Iceberg` |
| Partitioning Columns        | None      |
| Z-Order / Clustering Keys   | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng là 1 cặp `(customer_key, card_key)` mà tại đó `dim_cards.customer_id = dim_customers.customer_id` và cả hai đều đang ở phiên bản `is_current = true`. Không có chiều thời gian riêng (không `valid_from`/`valid_to`/`is_active`) — bảng luôn phản ánh trạng thái *tại thời điểm chạy job* (xem Decision Log #6).
> **Natural Key (NK):** `(customer_key, card_key)`
> **Primary Key:** Không có surrogate key riêng — NK cũng là PK, giống cách `trans_error_bridge` xử lý (bridge không cần SK vì không bị FK từ bảng khác trỏ vào).
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT (customer_key, card_key))`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source        | Dependency Type    | Note |
| --------------------- | ------------------- | ---- |
| `gold.dim_cards`       | Hard (must finish)  | Chỉ dòng `is_current = true`; cung cấp `card_key` và `customer_id` (owner). |
| `gold.dim_customers`   | Hard (must finish)  | Chỉ dòng `is_current = true`; cung cấp `customer_key`, join qua `customer_id`. |

### Downstream Consumers

| Type         | Name | Note |
| ------------ | ---- | ---- |
| Table        |      | Chưa có — xem Open Question #1 |
| BI Artifact  |      | Chưa có — xem Open Question #1 |
| Data Product |      | Chưa có — xem Open Question #1 |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name    | Data Type    | Key Type | Not Null | Transformation Logic                                                              | Null Handling                          | Allowed Range / Sample | Business Definition |
| --------------- | ------------ | -------- | -------- | -------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------- | ------------------------------------------------------------ |
| `customer_key`  | `StringType` | NK       | Y        | `gold.dim_customers.customer_key` WHERE `is_current = true`                            | N/A — inner join loại bỏ cặp không resolve được | —                         | Customer đang sở hữu card này, tại phiên bản hiện tại của customer. |
| `card_key`      | `StringType` | NK       | Y        | `gold.dim_cards.card_key` WHERE `is_current = true`                                     | N/A — inner join loại bỏ cặp không resolve được | —                         | Card đang được sở hữu, tại phiên bản hiện tại của card.       |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
>
> **Vì sao inner join, không phải left join + Unknown member (-1):** `dim_cards.customer_id` có thể null (card chưa resolve được owner — xem `cards_dimension.md` mục 5.1). Một card không có owner thì không có gì để "bridge", nên bị loại khỏi bảng này thay vì insert với `customer_key = '-1'` (xem Decision Log #7).
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------- | ------------------ | ----------------- |
| Add new column       |                     |                     |
| Drop column           |                     |                     |
| Rename column         |                     |                     |
| Change data type     |                     |                     |
| Change nullability   |                     |                     |
| Reorder columns       |                     |                     |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec khác (`cards_dimension.md`, `transaction_errors_bridge.md`).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute            | Value |
| ---------------------- | ----- |
| Strategy               | None  |
| Input columns          |       |
| Policy                 |       |
| Stability guarantee    |       |

> Không cần surrogate key: bảng không bị bất kỳ FK nào từ fact/dim khác trỏ vào, chỉ được truy vấn theo `customer_key` hoặc `card_key`.
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member | Key value | Khi nào dùng |
| ------ | --------- | ------------ |
| None   |           | Không áp dụng — bảng không phải parent table của 1 FK cần default member. Card/customer không resolve được owner bị loại khỏi bảng (mục 5.1), không map `-1`. |

> **Special member của 2 dimension nguồn phải bị loại trừ (bổ sung v.0.0.3).** `dim_cards` seed sẵn `card_key = '-1'` với `customer_id = 'UNKNOWN'` và `card_key = '-2'` với `customer_id = 'NOT APPLICABLE'`; `dim_customers` seed đúng cặp `customer_id` đối xứng, và cả 4 dòng đều `is_current = true`. Nếu join nguyên xi theo mục 8.1, inner join sẽ khớp chúng với nhau và sinh ra 2 dòng vô nghĩa `(-1, -1)` và `(-2, -2)` — "customer Unknown đang sở hữu card Unknown". Model lọc `card_key not in ('-1','-2')` và `customer_key not in ('-1','-2')` ở cả hai CTE (xem Decision Log 2026-07-24).
### 6.3. Special Type Handling
> **Special Type:** Factless Fact (bridge, current-state snapshot)

| Aspect                              | Rule |
| -------------------------------------- | ---- |
| Weighting factor                       | Không áp dụng — không có measure để phân bổ. |
| Ownership thay đổi (card đổi chủ)      | **Ngoài scope hiện tại.** `snapshot_cards` không track `customer_id` trong `check_cols` (xem `dbt/snapshots/snapshot_cards.sql`), nên `dim_cards` không thể phản ánh việc đổi chủ sở hữu dù nguồn có đổi. Spec này tạm giả định 1 card luôn thuộc về đúng 1 customer trong suốt vòng đời — xem Open Question #2 và Decision Log #4. |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | -------------- | --- |
| None   | —              | Bảng không tự track lịch sử — luôn phản ánh trạng thái hiện tại của `dim_cards`/`dim_customers` tại thời điểm full-refresh (xem Decision Log #3). |

---
## 7. Relationship & FK Resolution

| FK Column Name   | Parent Table                       | Join Condition                                                      | Unmatched Key Handling                                                   |
| ----------------- | ------------------------------------ | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `customer_key`    | `gold.dim_customers.customer_key`    | `dim_cards.customer_id = dim_customers.customer_id` (cả hai `is_current = true`) | Cặp không resolve được (customer_id null hoặc không khớp) bị loại khỏi kết quả (inner join). |
| `card_key`        | `gold.dim_cards.card_key`            | (cùng điều kiện trên)                                                      | (cùng xử lý trên)                                                                |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `gold.dim_cards` WHERE `is_current = true`, loại `card_key in ('-1','-2')` (special member — xem mục 6.2).
2. Đọc `gold.dim_customers` WHERE `is_current = true`, loại `customer_key in ('-1','-2')`.
3. Inner join hai tập trên qua `dim_cards.customer_id = dim_customers.customer_id`.
4. Truncate + reload toàn bộ `gold.card_owner_factless` với `(customer_key, card_key)` kết quả (full refresh — không merge/upsert).

| Attribute            | Value                                                                 |
| ---------------------- | ---------------------------------------------------------------------- |
| Merge / upsert keys    | Không áp dụng — full refresh, không phải incremental merge.            |
| Idempotency            | Full refresh tự nhiên idempotent — mỗi lần chạy tính lại toàn bộ từ trạng thái hiện tại của 2 dim. |
| Failure / retry        | Fail + halt khi vi phạm PK uniqueness (mục 3); an toàn để retry vì không có side-effect ngoài overwrite toàn bảng. |

### 8.2 Backfill & Historical Load Strategy

> Không cần backfill riêng — vì là full refresh, lần chạy đầu tiên đã là "toàn bộ trạng thái hiện tại", không có khái niệm nạp lại lịch sử.

---
## 9. Data Quality & Observability Checks

| Check Name                              | Target Column                    | Rule/Condition                                                                              | Threshold      | Severity | Frequency | Action on Fail | Alert Channel |
| ------------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------- | -------- | --------- | -------------------- | --------------- |
| PK uniqueness                              | `(customer_key, card_key)`         | unique & not null                                                                                 | 0 violations      | Error    | per run   | fail + halt           |                 |
| Not null                                   | `customer_key`, `card_key`          | not null                                                                                          | 0 violations      | Error    | per run   | fail + halt           |                 |
| customer_key tồn tại trong dim_customers   | `customer_key`                      | phải khớp `gold.dim_customers.customer_key` WHERE `is_current = true`                            | 0 violations      | Error    | per run   | fail + halt           |                 |
| card_key tồn tại trong dim_cards           | `card_key`                          | phải khớp `gold.dim_cards.card_key` WHERE `is_current = true`                                    | 0 violations      | Error    | per run   | fail + halt           |                 |

> **Hiện thực (v.0.0.3):** PK uniqueness = `dbt_utils.unique_combination_of_columns`, not null = generic `not_null`, cả hai khai báo trong `card_owner_factless.yml`. Hai check FK phải viết thành **singular test** (`dbt/tests/card_owner_factless_customer_exists_in_dim.sql`, `..._card_exists_in_dim.sql`) chứ không dùng generic `relationships` được, vì `relationships` không filter được phía parent theo `is_current = true`.
>
> **Giới hạn của 2 check FK:** trong cùng một lần `dbt build`, chúng gần như là tautology — bảng này được select thẳng ra từ chính 2 dimension đã filter `is_current`. Giá trị thật là bắt **staleness giữa các lần chạy**: project build theo từng lệnh `--select` riêng (`scripts/gold-dbt/deploy_gold_dbt_dev.sh`), nên `dim_cards`/`dim_customers` có thể sinh version SCD2 mới — đóng đúng version mà bảng này đang trỏ tới — mà bridge chưa được rebuild.

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:**
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column         | PII Level | Masking / Encryption Rule                                                        | Data Retention / Purge Policy |
| --------------- | --------- | ----------------------------------------------------------------------------------- | -------------------------------- |
| `customer_key`  | NSA       | Surrogate key (hash), không tự thân là PII — không cần masking riêng.               |                                   |
| `card_key`      | NSA       | Surrogate key (hash), không tự thân là PII — không cần masking riêng.               |                                   |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question                                                                                                             | Blocking? | Owner         | Status |
| --- | ------------------------------------------------------------------------------------------------------------------------ | --------- | --------------- | ------ |
| 1   | Có BI dashboard / data product nào sẽ dùng trực tiếp `card_owner_factless` không, hay hiện tại chưa có consumer cụ thể? | No        | NghiemCanCode  | Open   |
| 2   | Trong dữ liệu thực tế, có tồn tại trường hợp 1 card đổi chủ sở hữu (customer_id thay đổi) không? Cần kiểm tra sau khi có dữ liệu thật — nếu có, cần bổ sung `customer_id` vào `check_cols` của `snapshot_cards` (xem cùng Open Question ở `cards_dimension.md`). | No        | NghiemCanCode  | Open   |
| 3   | Frequency / Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án.                                        | No        | NghiemCanCode  | Open   |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision                                                                                          | Rationale                                                                                                                                       | Decided by     |
| ---------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 2026-07-23 | Table name = `gold.card_owner_factless`, entity = card (không phải "account" generic)                | Project chỉ có `dim_cards`, không có `dim_accounts`; tiêu đề "Account Owner Factless" và tên code legacy `account_owner_factless.py` là leftover terminology. | NghiemCanCode |
| 2026-07-23 | Grain = current-only pairs, không phải historical version overlap                                    | `fact_transactions.sql` (dòng 70-75) chỉ resolve `customer_key`/`card_key` bằng `is_current = true`, không point-in-time theo timestamp giao dịch — không có consumer nào trong project cần point-in-time join, nên xây historical overlap bridge sẽ là phức tạp thừa. | NghiemCanCode |
| 2026-07-23 | Bảng không tự SCD2 — không track lịch sử thay đổi ownership ở tầng này                                | Ownership tạm giả định cố định (1 card = 1 customer vĩnh viễn); việc track đổi chủ (nếu cần) thuộc về `snapshot_cards`/`dim_cards`, không phải bridge này. | NghiemCanCode |
| 2026-07-23 | `snapshot_cards.check_cols` hiện không có `customer_id` → `dim_cards` không thể phản ánh đổi chủ sở hữu dù nguồn đổi | Phát hiện khi review `dbt/snapshots/snapshot_cards.sql`; ghi nhận là giới hạn hiện tại, không sửa trong scope spec này (xem Open Question #2). | NghiemCanCode |
| 2026-07-23 | Load strategy = Full refresh (truncate + reload), không phải append/incremental merge                | Bảng derive hoàn toàn từ trạng thái current của 2 dimension mỗi lần chạy — không phải sự kiện bất biến để append như `trans_error_bridge`.        | NghiemCanCode |
| 2026-07-23 | Bỏ hoàn toàn cột thời gian (`valid_from_date`/`valid_to_date`/`is_active`) so với bản draft v.0.0.1   | Với grain current-only + ownership cố định, các cột này hoặc luôn là hằng số (valid_to=9999, is_active=true) hoặc trùng lặp với `dim_customers.effective_from_date` — không mang thêm thông tin. | NghiemCanCode |
| 2026-07-23 | Inner join thay vì left join + Unknown member (-1) khi `customer_id` không resolve                    | Một card không có owner thì không có quan hệ để "bridge" — khác với dimension, bảng này không có FK từ bảng khác trỏ vào nên không cần default member để giữ referential integrity. | NghiemCanCode |
| 2026-07-24 | Loại special member `-1`/`-2` của **cả hai** dimension trước khi join                                  | `dim_cards` seed `card_key '-1'` với `customer_id 'UNKNOWN'` và `'-2'` với `'NOT APPLICABLE'`, `dim_customers` seed đúng cặp đối xứng, cả 4 dòng `is_current = true` → join nguyên xi theo mục 8.1 sẽ sinh 2 cặp giả `(-1,-1)`, `(-2,-2)`. Mục 6.2 đã chốt bảng này không có default member, nên phải lọc. Phát hiện khi implement model. | NghiemCanCode |
| 2026-07-24 | **Đính chính rationale của Decision Log #2** (grain current-only): lý do "nhất quán với `fact_transactions`" không còn đúng | `fact_transactions` đã được restate sang as-of range join trên `effective_from_date`/`effective_to_date` (`transactions_fact.md` v.0.0.3, 2026-07-23), không còn resolve bằng `is_current`. Quyết định **giữ nguyên** current-only vì lý do còn lại vẫn vững: chưa có consumer nào cần point-in-time ownership (Open Question #1 vẫn Open), và bảng này vốn được định nghĩa là current-state snapshot. Ghi nhận để không ai đọc lại #2 rồi tưởng code đang lệch spec. | NghiemCanCode |

