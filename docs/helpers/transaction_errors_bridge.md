# Technical Specification: Transaction Error Bridge

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

| Version | Date       | Author        | Change                                                                                     |
| ------- | ---------- | ------------- | ------------------------------------------------------------------------------------------- |
| v.0.0.1 |            | NghiemCanCode | Initial draft (đã bị copy-paste nhầm nội dung từ Card Dimension spec)                       |
| v.0.0.2 | 2026-07-23 | NghiemCanCode | Viết lại toàn bộ theo đúng ngữ cảnh bridge lỗi giao dịch (Q&A, xem mục 11 Decision Log)      |

---

## 1. Overview & Business Context

> **Purpose:** Ghi nhận lý do (một hoặc nhiều) khiến một giao dịch bị đánh dấu lỗi (`fact_transactions.is_error = true`). Đây không phải dimension — nó là bridge table cho mối quan hệ nhiều-nhiều giữa 1 giao dịch lỗi và (các) lý do lỗi của nó.
> **Primary consumers:** `gold.fact_transactions` (referential/DQ check cho `is_error`)

| Attribute    | Value                                            | Description |
| ------------ | ------------------------------------------------- | ----------- |
| SCD Type     | None                                               | Giao dịch lỗi là sự kiện immutable, không có version/lịch sử thay đổi. |
| Special type | Bridge (multi-valued, không cần weighting factor)  | 1 transaction lỗi có thể có nhiều error_reason. |
| Grain        | Mỗi dòng là 1 cặp (transaction lỗi, lý do lỗi)     |             |

---
## 2. Metadata & Operational Info

| Attribute        | Value                     | Description                                                                 |
| ----------------- | ------------------------- | ----------------------------------------------------------------------------- |
| Table name        | `gold.trans_error_bridge` | Sửa lại từ `gold.dim_customer` (lỗi copy-paste ở bản draft v.0.0.1)          |
| Layer              | Gold                       |                                                                               |
| Source(s)          | `silver.silver_transactions` (cột `errors`), qua `stg_transactions` |                                     |
| Load strategy      | Append (insert-only)       | Immutable — không update/merge (xem Decision Log #2)                        |
| Watermark column   | `silver_transactions.timestamp` | Giống cách `fact_transactions` xử lý, vì `silver_transactions` không có `_updated_at` (xem Decision Log #6) |
| Frequency          |                            | Chưa quyết định ở cấp dự án (giống mọi bảng gold khác hiện tại)             |
| Orchestrator       |                            | Chưa quyết định ở cấp dự án                                                  |
| SLA                | None                       |                                                                               |
### 2.1. Physical Storage Layout

| Attribute                 | Value     |
| -------------------------- | --------- |
| Table Format                | `Iceberg` |
| Partitioning Columns        | None      |
| Z-Order / Clustering Keys   | None      |

---
## 3. Grain Definition

> **Grain:** Mỗi dòng ứng với 1 giá trị trong mảng `errors` (đã chuẩn hóa/split tại silver) của 1 giao dịch. Một `transaction_id` có thể xuất hiện nhiều dòng nếu có nhiều lý do lỗi cùng lúc.
> **Natural Key (NK):** `(transaction_id, error_reason)`
> **Uniqueness test:** `COUNT(*) = COUNT(DISTINCT (transaction_id, error_reason))`

---
## 4. Data Lineage & Dependencies
### Upstream Dependencies

| Table/Source          | Dependency Type    | Note |
| ----------------------- | ------------------- | ---- |
| `silver.silver_transactions` (qua `stg_transactions`) | Hard (must finish) | Cột `errors` (array, đã chuẩn hóa tại silver) là nguồn duy nhất. |

### Downstream Consumers

| Type         | Name                     | Note                                                                                          |
| ------------- | ------------------------- | ----------------------------------------------------------------------------------------------- |
| Table         | `gold.fact_transactions`  | Existence check: mọi `transaction_id` có `is_error = true` phải có ≥1 record tương ứng ở đây. Không join trực tiếp lúc load — chỉ kiểm tra ở tầng DQ (mục 9). |
| BI Artifact   |                            | Chưa xác định — xem Open Question #1                                                          |
| Data Product  |                            |                                                                                                 |

---
## 5. Column Definitions

### 5.1. Columns

| Column Name     | Data Type    | Key Type | Not Null | Transformation Logic                                                                  | Null Handling                          | Allowed Range / Sample                                                                                                     | Business Definition                                                                    |
| ---------------- | ------------ | -------- | -------- | ----------------------------------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `transaction_id` | `StringType` | NK       | Y        | `stg_transactions.transaction_id` (= `silver.silver_transactions.transaction_id`)         | Raise pipeline error                     | —                                                                                                                                | Định danh giao dịch lỗi, tham chiếu ngược tới `gold.fact_transactions.transaction_id`.     |
| `error_reason`   | `StringType` | NK       | Y        | Explode từng phần tử của mảng `silver.silver_transactions.errors` (đã chuẩn hóa **UPPER** tại silver — giữ nguyên UPPER, không re-case) | Skip record + log warning (giá trị lạ, không khớp enum) | `["INSUFFICIENT BALANCE", "BAD CVV", "BAD EXPIRATION", "TECHNICAL GLITCH", "BAD CARD NUMBER", "BAD PIN", "BAD ZIPCODE"]` | Lý do cụ thể khiến giao dịch bị đánh dấu lỗi.                                               |

> **Quy ước:** **Key Type** chỉ ghi ở cột này (PK/SK/NK/FK). Không annotate thêm trong cột Data Type để tránh "hai nguồn sự thật".
>
> **Nguồn sự thật khi mâu thuẫn:** nếu `is_error` và mảng `errors` không khớp nhau (VD: `is_error=false` nhưng `errors` không rỗng), bridge được build dựa trên `errors` không rỗng, bất kể `is_error` nói gì (xem Decision Log #3).
### 5.2. Schema Evolution Policy

| Change Type        | Pipeline Behavior | Action Required |
| ------------------- | ------------------ | ----------------- |
| Add new column       |                     |                     |
| Drop column           |                     |                     |
| Rename column         |                     |                     |
| Change data type     |                     |                     |
| Change nullability   |                     |                     |
| Reorder columns       |                     |                     |

> Chưa quyết định ở cấp dự án — đồng nhất với tình trạng hiện tại của các spec dimension khác (`cards_dimension.md`, v.v.).

---
## 6. Key Strategy & Special Members
### 6.1. Surrogate Key Generation

| Attribute            | Value |
| ---------------------- | ----- |
| Strategy               | None  |
| Input columns          |       |
| Policy                 |       |
| Stability guarantee    |       |

> Không cần surrogate key: bridge không được join trực tiếp bởi FK từ fact/dim khác, chỉ được truy vấn theo `transaction_id`.
### 6.2. Unknown / Default Member
> Bắt buộc với mọi dimension để fact giữ được referential integrity khi FK không resolve.

| Member | Key value | Khi nào dùng |
| ------ | --------- | ------------ |
| None   |           | Không áp dụng — bridge không phải parent table của 1 FK cần default member. |
### 6.3. Special Type Handling
> **Special Type:** Bridge (multi-valued)

| Aspect                             | Rule |
| ------------------------------------ | ---- |
| Weighting factor                     | Không áp dụng — bridge chỉ dùng để validate tồn tại, không dùng để phân bổ lại số đo (measure) của fact. |
| Giá trị `error_reason` lạ (ngoài enum) | Skip record + log warning (xem Decision Log #4). |
### 6.4. SCD Type 2 - Change Tracking

| Column | Trigger SCD2? | Why |
| ------ | -------------- | --- |
| None   | —              | Giao dịch lỗi immutable, không có SCD2 (xem Decision Log #2). |

---
## 7. Relationship & FK Resolution

| FK Column Name   | Parent Table                    | Join Condition                                  | Unmatched Key Handling                                                                                     |
| ----------------- | -------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `transaction_id`  | `gold.fact_transactions.transaction_id` | `trans_error_bridge.transaction_id = fact_transactions.transaction_id` | N/A tại thời điểm load (bridge được derive trực tiếp từ cùng nguồn `stg_transactions` nên luôn resolve được). Được verify ngược lại bằng DQ check ở mục 9 (Error bridge ↔ fact consistency). |

---
## 8. Execution & Loading Strategy

### 8.1 Pipeline Flow (Pseudo-code / Steps)

1. Đọc `stg_transactions`, giữ lại các dòng có mảng `errors` không rỗng (nguồn sự thật ưu tiên `errors`, không phải cờ `is_error` — xem Decision Log #3).
2. Explode mảng `errors` → mỗi phần tử thành 1 dòng `(transaction_id, error_reason)`.
3. Validate `error_reason` theo enum chuẩn (mục 5.1); giá trị lạ → skip record + log warning.
4. Insert append-only vào `gold.trans_error_bridge` (immutable — không update/merge).

| Attribute            | Value                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| Merge / upsert keys    | None — append-only insert, không update record đã tồn tại.                                |
| Idempotency            | Lọc theo `silver_transactions.timestamp` (incremental, giống `fact_transactions`) để rerun không tạo trùng lặp. |
| Failure / retry        | Fail + halt khi vi phạm PK uniqueness / not-null (mục 9); an toàn để retry vì table không có side-effect ngoài insert. |

### 8.2 Backfill & Historical Load Strategy

> Full backfill toàn bộ lịch sử `silver_transactions` hiện có, chạy 1 lần khi migrate — đồng nhất cách các dimension khác trong dự án đã backfill (`dim_customers`, `dim_cards`, v.v.).

---
## 9. Data Quality & Observability Checks

| Check Name                        | Target Column                    | Rule/Condition                                                                 | Threshold        | Severity | Frequency | Action on Fail          | Alert Channel |
| ----------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------- | ------------------ | -------- | --------- | -------------------------- | --------------- |
| PK uniqueness                       | `(transaction_id, error_reason)`   | unique & not null                                                                | 0 violations       | Error    | per run   | fail + halt                |                 |
| Not null                            | `transaction_id`, `error_reason`   | not null                                                                         | 0 violations       | Error    | per run   | fail + halt                |                 |
| Error reason enum validity          | `error_reason`                     | giá trị nằm trong 7 enum ở mục 5.1                                              | 0 unexpected values | Warning  | per run   | skip record + log warning  |                 |
| Error bridge ↔ fact consistency     | `transaction_id`                   | mọi `transaction_id` trong bridge phải tồn tại trong `fact_transactions` với `is_error = true` (chiều ngược của check "Error bridge consistency" trong `transactions_fact.md` mục 9) | 0 violations        | Critical | per run   | fail + halt                |                 |

---
## 10. Security & Governance

> **Notes:** Dự án chưa được migrate sang Lake Formation, nhưng vẫn sẽ đề cập
> **Quy ước:** 
> 1. `Masking / Encryption Rule` = lúc đang lưu thì bảo vệ thế nào (ai đọc được dạng rõ).
> 2. `Data Retention / Purge Policy` = giữ bao lâu rồi xóa/anonymize — phục vụ compliance (GDPR/CCPA) và quản trị chi phí storage.
> 3.  Các level PII: Direct Identifier (DI), Quasi-Identifier (QI), Sensitive Attribute (SA), Non-sensitive Attribute (NSA)

| Column           | PII Level | Masking / Encryption Rule | Data Retention / Purge Policy |
| ------------------ | --------- | ---------------------------- | -------------------------------- |
| `transaction_id`   |           | Chưa xác định — xem Open Question #4 (đồng bộ với `transactions_fact.md` mục 10, cũng đang bỏ trống). |  |
| `error_reason`      | NSA       | Không cần masking — chỉ là phân loại lỗi kỹ thuật/nghiệp vụ, không tiết lộ thông tin cá nhân. |  |

---
## 11. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không. `Yes` = chưa giải quyết thì không được ship; `No` = vẫn ship được, xử lý sau.

| #   | Question                                                                                          | Blocking? | Owner         | Status |
| --- | ---------------------------------------------------------------------------------------------------- | --------- | --------------- | ------ |
| 1   | Có BI dashboard / data product nào sẽ dùng trực tiếp `trans_error_bridge` không, hay hiện tại chỉ phục vụ DQ check của `fact_transactions`? | No        | NghiemCanCode  | Open   |
| 2   | Trong dữ liệu thực tế, có tồn tại trường hợp `is_error` và `errors` mâu thuẫn nhau không? Cần validate rule ở mục 5.1 sau khi backfill lần đầu. | No        | NghiemCanCode  | Open   |
| 3   | Frequency / Orchestrator cho toàn bộ gold layer chưa được quyết định ở cấp dự án.                    | No        | NghiemCanCode  | Open   |
| 4   | PII level của `transaction_id` chưa xác nhận (đồng thời cũng đang bỏ trống ở `transactions_fact.md`). | No        | NghiemCanCode  | Open   |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision                                                                                  | Rationale                                                                                                          | Decided by     |
| ---------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 2026-07-23 | Grain = `(transaction_id, error_reason)`, không phải quan hệ 1-1 với transaction              | Cột `errors` ở silver là array chuẩn hóa; 1 giao dịch có thể có nhiều lý do lỗi cùng lúc → đúng ngữ nghĩa bridge nhiều-nhiều. | NghiemCanCode    |
| 2026-07-23 | Bảng immutable/append-only, không SCD2                                                        | Giao dịch lỗi là sự kiện xảy ra 1 lần, không có trạng thái "resolve" cần track lịch sử.                              | NghiemCanCode    |
| 2026-07-23 | Khi `is_error` và `errors` mâu thuẫn, ưu tiên `errors` làm nguồn sự thật                       | `errors` là dữ liệu chi tiết hơn; `is_error` chỉ là cờ tổng hợp suy ra từ `errors`.                                   | NghiemCanCode    |
| 2026-07-23 | `error_reason` lạ (ngoài 7 enum) → skip record + log warning, không map `UNKNOWN`, không raise error | Enum đã được chuẩn hóa ở silver nên giá trị lạ nhiều khả năng là lỗi dữ liệu cần điều tra riêng, nhưng chưa nghiêm trọng tới mức halt cả pipeline. | NghiemCanCode    |
| 2026-07-23 | Table name = `gold.trans_error_bridge`                                                        | Sửa lỗi copy-paste `gold.dim_customer` từ bản draft v.0.0.1; khớp với tên model dbt hiện có.                         | NghiemCanCode    |
| 2026-07-23 | Backfill = full historical load                                                               | Đồng bộ cách các dimension khác trong dự án (`dim_customers`, `dim_cards`,...) đã backfill khi migrate.              | NghiemCanCode    |
| 2026-07-23 | Watermark = `silver_transactions.timestamp`                                                   | `silver_transactions` không có cột `_updated_at`; đồng nhất cách `fact_transactions` đã xử lý cùng vấn đề này.       | NghiemCanCode    |
| 2026-07-23 | Thêm cột `errors` (array) vào `sources.yml`/`stg_transactions.sql`                             | Cột tồn tại thật ở schema silver gốc (`silver_schema.py`) nhưng chưa từng được khai báo trong dbt project — là nguồn duy nhất để build bridge, bắt buộc phải wire vào trước khi build model. | NghiemCanCode    |
| 2026-07-23 | Load strategy = Incremental (append), không full load                                         | Quy mô bridge tăng theo số giao dịch (giống `fact_transactions`, khác các dimension tham chiếu nhỏ như `dim_geo`); full refresh mỗi lần chạy sẽ tốn kém khi dữ liệu lớn dần. | NghiemCanCode    |
| 2026-07-23 | Thêm cột `timestamp` vào output ngoài danh sách mục 5.1, chỉ để làm watermark incremental      | Mục 5.1 chỉ liệt kê `transaction_id`/`error_reason`, nhưng mục 2 yêu cầu watermark = `timestamp` — không có cột nào để tham chiếu `max(timestamp) from {{ this }}` nếu không persist nó ra output. Không dùng `_updated_at` (dù tồn tại ở schema silver gốc) để tránh mở lại quyết định đã chốt ở `transactions_fact.md`. | NghiemCanCode    |
| 2026-07-23 | Implement singular test 2 chiều cho "Error bridge ↔ fact consistency" (mục 9)                  | Đóng luôn gap được note chéo ở cả `transaction_errors_bridge.md` và `transactions_fact.md` — cả 2 chiều consistency (bridge→fact, fact→bridge) đều cần vì bridge không join trực tiếp lúc load. | NghiemCanCode    |
| 2026-07-24 | `error_reason` giữ **UPPER** (enum: `INSUFFICIENT BALANCE`, `BAD CVV`, ...), sửa từ Title Case ở mục 5.1 + `accepted_values` test + code | Convention toàn dự án: dữ liệu category lưu UPPER xuyên suốt (khớp `card_brand`, `gender`, `transaction_type`, `am_pm`). Silver đã chuẩn hóa `errors` thành UPPER (`data_transform.py` `trim_and_upper_col`); spec/code bridge trước đó lọc Title Case → **không khớp dòng nào → bridge rỗng → 100% giao dịch lỗi fail check fact→bridge** (phát hiện lần đầu fact_transactions build thành công, 2026-07-24). Bridge nay giữ UPPER, không re-case. | NghiemCanCode |
