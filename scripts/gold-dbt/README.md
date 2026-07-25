# `deploy_gold_dbt_dev.sh` — sổ tay vận hành

Tài liệu đi kèm `scripts/gold-dbt/deploy_gold_dbt_dev.sh`, script dựng EMR Serverless session rồi chạy dbt cho tầng Silver → Gold trên môi trường **dev**. Cả thư mục `scripts/gold-dbt/` gom mọi thứ chỉ phục vụ pipeline dbt gold này: script chính, Glue Crawler tạm cho silver, script sinh seed ngày lễ, policy IAM, và SQL calibrate.

Trong script chỉ còn **code**: tiêu đề bước, số mục của README này, và lệnh. Mọi giải thích "vì sao viết như vậy", mọi cảnh báo và mọi kết quả đã chạy đều nằm ở đây. Các câu SQL dài (probe, calibrate) nằm trong `scripts/gold-dbt/sql/*.sql` — script chỉ `cat` file đó vào `dbt show --inline`, không nhúng SQL trực tiếp.

> Tham chiếu dùng **số mục** (`README.md §5`) chứ không dùng số dòng: số dòng lệch ngay khi ai đó thêm một khối lệnh, còn số mục thì không.

---
## 1. Cách dùng

Script chạy **từ trên xuống**. Mặc định **mọi lệnh dbt đều đang comment** — bỏ comment đúng khối muốn chạy, chạy, rồi comment lại. Riêng STEP 0 → STEP 2 (dựng session) thì luôn chạy, không comment.

```bash
bash scripts/gold-dbt/deploy_gold_dbt_dev.sh
```

Điều kiện trước: `scripts/.env.runtime` phải tồn tại (sinh bởi `_dev_setup.sh`, dùng chung với hai job PySpark cũ nên vẫn nằm ở `scripts/` gốc, không phải trong `gold-dbt/`), và profile dbt `aws_pipeline` phải có ở `~/.dbt/profiles.yml` (`type: spark`, `method: session`, đọc biến `SPARK_REMOTE` mà script export).

Thứ tự trong file có ý nghĩa: một khối đặt sai vị trí sẽ chạy trước thứ nó phụ thuộc. Xem §14 cho ví dụ cụ thể (`card_owner_factless` phải nằm sau STEP 4).

### 1.1. `scripts/gold-dbt/sql/`

Mọi câu SQL dài hơn một dòng (probe, fingerprint, calibrate) sống ở `scripts/gold-dbt/sql/*.sql`, không nhúng trong script. Cách gọi luôn là:

```bash
poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
  --inline "$(cat "$SQL_DIR/<tên_file>.sql")"
```

`SQL_DIR` được set ở STEP 0 (`$SCRIPT_DIR/sql`, với `SCRIPT_DIR` là chính thư mục `gold-dbt/`). Mỗi file `.sql` là một câu lệnh tự đứng độc lập, `{{ ref(...) }}` render qua Jinja của dbt như bất kỳ model nào — không phải template ghép chuỗi.

Sáu file `calibrate_abnormal_error_rate_*.sql` của STEP 9 (§16) lặp lại cùng các CTE nền (`window_days`, `merchant_window`, và tuỳ file là `floors`/`qualified`) trong mỗi file thay vì dùng chung một biến base rồi nối chuỗi: đánh đổi lặp code để mỗi file là SQL đọc được và chạy được độc lập, không phụ thuộc thứ tự gọi hay cách bash ghép chuỗi.

---
## 2. STEP 0 — Biến & môi trường

Đọc `scripts/.env.runtime`. Thiếu file thì dừng ngay với thông báo, thay vì chạy tiếp rồi chết ở chỗ khó hiểu hơn.

---
## 3. STEP 0b — Đảm bảo Glue database `default` tồn tại

Spark bật Hive support (`spark.sql.catalogImplementation=hive`, cần cho `SparkSessionCatalog` ở §5) **luôn probe một database tên `default`** lúc khởi động. Đây là quy ước của Hive, không liên quan gì tới database mà pipeline này thực sự dùng.

Glue Data Catalog trần thì **không** có sẵn `default` (khác Hive metastore thật, vốn tự tạo). Thiếu nó, session chết ngay lúc boot:

```
not authorized to perform: glue:GetDatabase on resource: database/default
```

(hoặc `CreateDatabase`, nếu execution role không được phép tự tạo).

Tạo một lần giữ cho execution role chỉ cần quyền đọc.

**Trên dev, `default` do owner tạo tay bằng AWS Console** (để chạy Hive table), nên bước này luôn rơi vào nhánh "already exists. Skipping" và chưa từng thật sự tạo gì. Nó là lưới an toàn cho environment mới. Ghi rõ vì đây là **hạ tầng tạo tay, không nằm trong state của Terraform** — `terraform destroy`/dựng lại environment sẽ không mang nó về; chỉ script này mới.

---
## 4. STEP 0c — Grant Glue nằm ngoài Terraform

Hai lỗ hổng không liên quan nhau, cùng vá bằng một inline policy trên execution role. Tên policy giữ là `TEMP-finance-silver-read` để lần chạy đứt gánh trước đó vẫn được trap ở §6 dọn đúng — **tên có chữ TEMP nhưng chỉ statement thứ 1 là tạm**, xem dưới.

**1. `finance_silver`** — database do Glue Crawler tạo (`scripts/gold-dbt/glue_crawler.sh`), không có trong `terraform/environments/dev/compute/iam.tf`. Chỉ đọc. **Cố ý không Terraform-hoá**: silver có kế hoạch migrate sang Iceberg, lúc đó database này và cả statement này biến mất.

**2. `table/default/*`** — snapshot materialization của dbt-spark ghi staging view vào đó. Với target Iceberg nó **cố tình bỏ catalog và schema** khỏi tên view (Iceberg catalog không hỗ trợ `create view <schema>.<name>`), nên `gold_staging.snapshot_customers` đẻ ra `default.snapshot_customers__dbt_tmp`. Cần create/describe/drop chứ không chỉ đọc.

Statement thứ 2 **ở lại đây vĩnh viễn — cố ý không đưa vào Terraform** (Decision Log 2026-07-25 trong `docs/known_issues/dbt_spark_relation_cache.md`). Lý do: `default` không phải hạ tầng của project mà là scratch namespace bị Spark và dbt-spark ép phải có; bản thân database đó được tạo tay bằng Console và có §3 ở trên làm lưới an toàn, đằng nào cũng nằm ngoài Terraform — nên tách riêng phần quyền vào Terraform chỉ xé một mối quan tâm ra hai chỗ. **Đừng xoá statement này.** Nó chỉ bắt đầu lỗi khi relation cache hoạt động trở lại (`dbt/macros/spark_list_relations_without_caching.sql`) và snapshot chuyển từ `create or replace table` sang `merge into`; xem §8.1 của tài liệu đó.

> **Hai statement có vòng đời khác nhau — đừng gộp làm một.** Statement 1 chết khi silver lên Iceberg; statement 2 thì không, vì cả hai lý do tồn tại của nó (Hive probe `default` lúc boot ở §3, và staging view của snapshot **Iceberg**) đều độc lập với tầng silver. Vì thế `glue:GetDatabase` trên `database/default` đã được thêm vào statement 2 ngày 2026-07-25: trước đó nó chỉ nằm ở statement 1, nên đến ngày xoá statement 1 là session chết ngay lúc boot với đúng lỗi ở §3.

**Vì sao gọi `put-role-policy` vô điều kiện mỗi lần chạy** thay vì kiểm tra trước: lệnh này vốn ghi đè tại chỗ, nên gọi thẳng tránh được việc âm thầm giữ lại một policy body cũ nếu trap dọn dẹp của lần trước không kịp chạy.

---
## 5. STEP 1 — Tạo Spark session

**`sessionEnabled=true` phải set bằng CLI:** tính đến 2026-07-16, Terraform chưa hỗ trợ tham số này, nên script gọi `update-application` mỗi lần.

**Vì sao `spark_catalog` = `SparkSessionCatalog` chứ không phải `SparkCatalog`:** `SparkCatalog` chỉ resolve được bảng Iceberg, nên không nhìn thấy bảng Silver non-Iceberg do Glue Crawler tạo. `SparkSessionCatalog` fallback về Hive/Glue metastore cho những bảng đó, nhờ vậy Silver (Hive) và Gold (Iceberg) cùng resolve dưới một catalog mặc định.

> Lựa chọn này là nguyên nhân gốc của bug relation cache — xem `docs/known_issues/dbt_spark_relation_cache.md` §2. Nó **cố ý và cần thiết**, không phải cấu hình sai; và cũng không thể tách catalog để né (§2.1 của tài liệu đó).

Sau khi `start-session`, script poll `session.state` mỗi 5 giây cho tới `STARTED`/`IDLE`; gặp `FAILED`/`TERMINATED` thì in `stateDetails` rồi thoát 1.

---
## 6. Cleanup trap

Từ lúc session lên, `trap cleanup EXIT` đảm bảo **mọi đường thoát** — thành công, lỗi, hay Ctrl-C — đều: terminate session, gỡ inline policy tạm, stop application. Không có đường nào để lại session cháy tiền.

---
## 7. STEP 2 — Lấy `SPARK_REMOTE`

Đổi session endpoint + auth token thành URL Spark Connect (`sc://...:443`), export ra `SPARK_REMOTE` để dbt profile đọc, và set `DBT_PROJECT_DIR`.

---
## 8. STEP 3 — Lệnh steady-state

Nhóm lệnh chạy hằng ngày, mỗi lệnh một cụm model. Bỏ comment cái cần.

### 8.1. `batch_logical_date` nghĩa là NGÀY DỮ LIỆU

Với hai fact incremental (`fact_daily_transaction_trend`, `fact_customer_activity_daily`), macro `batch_logical_date()` chỉ **ngày của dữ liệu**, không phải ngày chạy batch. Một lần chạy T+1 vào ngày D+1 **bắt buộc** truyền `--vars '{batch_logical_date: <D>}'`; nếu ỷ vào mặc định `current_date()` thì nó ghi đè partition D+1 rỗng và để nguyên D.

### 8.2. `card_owner_factless`

Bridge trạng thái hiện tại trên `dim_cards`/`dim_customers` (`docs/helpers/card_owner_factless.md`).

**Không bao giờ dùng `--full-refresh`, không bao giờ dùng `--vars`:** model là `materialized: table`, mỗi lần chạy đã dựng lại toàn bộ từ phiên bản hiện tại của hai dimension — đó **chính là** chiến lược full-refresh mà spec mô tả (section 2). Nó không có partition, không watermark, không ngày batch.

Phải chạy **sau** khi `dim_cards` và `dim_customers` đã cập nhật. Chạy trên dimension cũ không làm hỏng dữ liệu (lần sau tính lại), nhưng hai singular test staleness sinh ra đúng để bắt thứ tự ngược — rebuild dimension sau đó và để bảng này trỏ vào các version đã đóng.

Indirect selection để mặc định (eager) — đó là thứ kéo hai singular test vào. Parent còn lại của chúng (`dim_cards`, `dim_customers`) lúc này đã tồn tại nên eager không thể fail như ở STEP 4. Kỳ vọng **5 test**: 1 `unique_combination_of_columns`, 2 `not_null`, 2 FK/staleness.

**Lệnh của nó đã dời xuống STEP 7 (§14)** cho đợt rebuild 2026-07-24 — lý do ở §14.

---
## 9. STEP 4 — Chuỗi point-in-time

`docs/facts/transactions_fact.md` Decision Log 2026-07-23. Hai stage.

**Vì sao chạy lại dù đã đánh DONE:** mọi lần chạy trước đều sinh lại `customer_key`/`card_key`, vì chúng hash `dbt_valid_from` của snapshot mà lỗi relation cache lại dựng lại snapshot mỗi run (`docs/known_issues/dbt_spark_relation_cache.md` §4.1). Trừ khi lần chạy cuối tình cờ build dimension và fact trong cùng một chuỗi, các FK đang nằm trong `fact_transactions` đều trỏ vào khoá không còn tồn tại. Rebuild cả chuỗi một lượt rẻ hơn là đi chứng minh điều ngược lại.

> **Điều kiện tiên quyết cho STEP 5 và STEP 6:** đợt restatement này phải chạy xong ít nhất một lần. Hai step đó aggregate thẳng từ `fact_transactions`, nên chạy trên fact chưa restate sẽ âm thầm đóng băng lại cách quy kết `is_current` cũ.

### 9.1. Stage 4a — điều kiện tiên quyết

`fact_transactions` join các dimension lookup/tĩnh (`dim_dates`, `dim_times`, `dim_geo`, `dim_merchant`), nên chúng phải tồn tại trước khi fact rebuild — lỗi "gold.dim_dates not found" gặp trước đây là do thiếu bảng, không phải bug trong SQL của fact.

**Không `--full-refresh` ở stage này:** snapshot giữ lịch sử SCD2 vĩnh viễn và tuyệt đối không được full-refresh; còn các dim Type-1/tĩnh là create-or-replace nên không cần.

Seed `us_holidays` được chọn kèm trong `dbt build` thay vì chạy `dbt seed` riêng, để không thể lỡ bỏ qua nó — `dbt build` nạp seed và xếp nó trước `dim_dates` (model có `ref` tới seed).

**`--indirect-selection cautious`:** mặc định dbt kéo eager mọi test có ít nhất một parent được chọn, ở đây sẽ lôi vào các FK test thuộc `fact_transactions`/`dim_customers` — những model chưa build tới stage 4b. `cautious` chỉ giữ test có **toàn bộ** parent nằm trong selection.

### 9.2. Stage 4b — restate chuỗi point-in-time

`--full-refresh` để đợt backdate version-1 chạm được các dòng dimension đã materialize, và để fact rebuild toàn bộ theo quy tắc as-of (cũng bắt buộc cho cột `customer_age_at_transaction` mới). `trans_error_bridge` đi kèm để các cross test error-bridge của `fact_transactions` có bảng đối chiếu.

`--indirect-selection cautious` cùng lý do stage 4a: eager sẽ lôi vào các reconciliation test của `fact_daily_transaction_trend` (model thuộc pass sau, không build ở đây) rồi fail vì thiếu bảng.

> **`--full-refresh` KHÔNG đụng tới snapshot.** dbt-core 1.11.12 hardcode `full_refresh_mode=False` trong snapshot materialization, nên cờ này chỉ rebuild model. Đúng ý ở đây — nhưng cũng có nghĩa **không có cờ nào reset được snapshot**; muốn reset chỉ có cách drop bảng.

---
## 10. STEP 5 — Build lần đầu `fact_daily_transaction_trend`

`docs/facts/daily_transaction_trend_fact.md` v.0.0.2. Aggregate fact-of-fact, chỉ đọc `gold.fact_transactions`, nên **phải** nằm sau đợt restatement STEP 4 (§9).

**`--full-refresh`** vì đây là lần materialize đầu tiên: model là incremental/insert_overwrite, chạy trần sẽ chỉ đụng đúng một partition `batch_logical_date()` thay vì aggregate toàn lịch sử.

**Cố ý không `--vars`.** Dưới `--full-refresh`, bộ lọc ngày `is_incremental()` bị bỏ qua hoàn toàn nên mặc định `current_date()` của macro không ảnh hưởng gì tới thứ được build; ba singular test theo ngày batch khi đó chỉ đánh giá trên partition hôm nay (nhiều khả năng rỗng) và pass tầm thường — chúng có chống chia-cho-0 và NULL sum. Còn lần chạy incremental steady-state thì **có** cần `--vars` với ngày dữ liệu (§8.1).

Indirect selection để mặc định (eager), khác STEP 4: eager chính là thứ kéo hai reconciliation singular test vào, parent còn lại của chúng là `fact_transactions`. Thêm `cautious` ở đây sẽ âm thầm bỏ chúng và chỉ chạy `unknown_mcc_share`. Kỳ vọng **24 test**.

---
## 11. STEP 6 — Build lần đầu `fact_customer_activity_daily`

`docs/facts/customer_activity_daily_fact.md` v.0.0.2. Model đầu tiên của reporting layer (`models/marts/reporting/`). Fact-of-fact dẫn xuất: đọc `gold.fact_transactions`, cộng `dim_customers`/`dim_cards` lấy natural key và `dim_dates` làm spine ngày — nên **phải** sau restatement STEP 4 (§9), và nó thừa hưởng cách quy kết point-in-time của chuỗi đó.

**`--full-refresh`** cùng lý do STEP 5.

**Không `--vars`, và lý do mạnh hơn STEP 5:** dưới `--full-refresh` thì `is_incremental()` false, nên model **không hề tham chiếu** macro — spine tự neo vào ngày cuối cùng thực sự có giao dịch (spec section 3, Decision Log 2026-07-24). Không cần biết ngày đó là ngày nào. Mặc định `current_date()` của macro khi ấy chỉ chạm tới reconciliation test, vốn so partition hôm nay (không tồn tại) với 0 dòng nguồn và pass tầm thường.

Indirect selection để mặc định (eager) giống STEP 5 và cùng lý do: parent còn lại của reconciliation test là `fact_transactions` và `dim_customers`, nên `cautious` sẽ âm thầm bỏ nó và chỉ chạy 14 generic test. Kỳ vọng **15 test**.

---
## 12. STEP 6b — Smoke test nhánh incremental

Chỉ chạy sau khi STEP 6 thành công. Chạy lại model **không** `--full-refresh` nhưng **có** ngày dữ liệu, hai lần, in fingerprint của partition trước / giữa / sau (`scripts/gold-dbt/sql/activity_partition_fingerprint.sql`). Ba lần in giống hệt nhau chứng minh `insert_overwrite` ghi đè đúng một partition và idempotent.

Ngày dữ liệu là `date_key` lớn nhất trong `fact_transactions` — probe `scripts/gold-dbt/sql/fact_transactions_date_bounds.sql` trả lời `min_dk=20100101`, `max_dk=20191031`, tức **2019-10-31**.

**Kết quả và ý nghĩa: `docs/known_issues/dbt_spark_relation_cache.md` §6.1.** Đây là lần đầu tiên trong lịch sử dự án một nhánh `is_incremental()` được thực thi.

Bật lại để verify sau bất kỳ thay đổi nào ở logic incremental, cách partition, hay version dbt-spark.

Chi tiết vận hành:
- Các lệnh `dbt show` fingerprint **không** có `|| true`: chúng *chính là* bài test, fail thì phải dừng. Riêng lệnh probe min/max giữ `|| true` vì nó chỉ là chẩn đoán, và `set -e` sẽ làm cả script chết trước khi tới STEP 7.
- dbt sẽ bỏ partial-parse cache giữa các lệnh xen kẽ `--vars` / không `--vars` ("config vars have changed") — bình thường, tốn vài giây.

---
## 13. STEP 6c — Build lần đầu + smoke test `rpt_card_portfolio`

`docs/metrics/card_portfolio_report.md` v.0.0.2. Model thứ hai của reporting layer, "nhà" của toàn bộ Dashboard C. Grain `date_key × chip_segment × card_brand`, `insert_overwrite` theo `date_key`.

**Phải sau STEP 6/6b**, không phải vì build phụ thuộc (build chỉ cần `fact_transactions`, `dim_cards`, `dim_dates`) mà vì **test** phụ thuộc: `rpt_card_portfolio_active_card_cross_check` so `sum(active_card_count_90d)` của bảng này với của `fact_customer_activity_daily` tại cùng `date_key`. Chưa có bảng kia thì test đó vô nghĩa.

### `--vars` là BẮT BUỘC, kể cả ở lần `--full-refresh` — khác STEP 5 và STEP 6

Đây là điểm khác duy nhất so với hai step trước, và nó không phải tuỳ chọn. Ở STEP 5/6, `--full-refresh` làm `is_incremental()` false nên model không chạm macro, còn reconciliation test thì so partition-hôm-nay (không tồn tại) với 0 dòng nguồn và pass tầm thường. Ở đây **một trong bốn singular test không có tính chất đó**: `rpt_card_portfolio_issued_reconciliation` tính lại từ `dim_cards`, mà mọi thẻ đều có phiên bản hiệu lực ở **mọi** ngày kể cả hôm nay — vế nguồn không bao giờ rỗng. Test đó có guard `model_range` chặn đúng tình huống này (spec Decision Log 2026-07-25, mục implement), nhưng dựa vào guard để cứu một lệnh thiếu `--vars` là dựa vào một cơ chế phòng hộ chứ không phải chạy đúng — truyền ngày dữ liệu thì cả 4 test mới thật sự kiểm được cái chúng sinh ra để kiểm.

Ngày dữ liệu vẫn là **2019-10-31** (`max(date_key)` của `fact_transactions`, §12).

### Ba lần in fingerprint

Cùng khuôn STEP 6b: `scripts/gold-dbt/sql/card_portfolio_partition_fingerprint.sql`, in trước / giữa / sau hai lần chạy incremental. Ba lần giống hệt nhau chứng minh `insert_overwrite` ghi đè đúng một partition và idempotent.

Fingerprint cộng `issued_card_count` / `active_card_count_90d` / các count `_90d` **qua segment trong đúng một `date_key`** — chiều duy nhất các cột đó additive. Không bao giờ cộng qua ngày (spec §5.1, registry quy tắc #9 và #10). Đây là fingerprint, không phải metric.

### Chẩn đoán Δ, chạy một lần là đủ

`scripts/gold-dbt/sql/card_portfolio_cross_check_delta.sql` in ba số hạng của cross-check thay vì assert chúng. Δ = số thẻ mà **mọi** giao dịch trong cửa sổ 90 ngày đều có `customer_key = '-1'`: những thẻ đó vô hình với `fact_customer_activity_daily` (bảng đó drop giao dịch sentinel) nhưng active ở bảng này (giữ lại), nên đẳng thức hai bảng chỉ đóng sau khi cộng Δ. Spec Open Question #4 kỳ vọng **Δ = 0** trên dev — nếu ra khác, ghi số vào Open Question đó, đừng để lại trong script (§17.2).

Giữ `|| true` vì đây là chẩn đoán, không phải cổng — cổng là singular test. Cột `residual` phải bằng 0.

### Kỳ vọng số test

Indirect selection để mặc định (eager) như STEP 5/6 và cùng lý do: parent còn lại của 4 singular test là `fact_transactions` / `dim_cards` / `fact_customer_activity_daily`, nên `cautious` sẽ âm thầm bỏ cả 4 — và bỏ luôn test `relationships` của `date_key` (parent thứ hai là `dim_dates`), còn **29**. Kỳ vọng **34 test** với eager.

---
### 13.1. STEP 6d — Build lần đầu + smoke test `rpt_merchant_error_daily`

`docs/metrics/merchant_error_daily_report.md` v.0.0.2. Model thứ ba của reporting layer, "nhà" của metric **Abnormal Error Rate (merchant)**. Grain `date_key × merchant_id`, `insert_overwrite` theo `date_key`. Đánh số phụ `13.1` chứ không phải §14 để không phải đánh số lại toàn bộ README và ~25 tham chiếu chéo tới nó.

**Phụ thuộc STEP 5, không phụ thuộc STEP 6/6c.** Nguồn measure duy nhất là `fact_daily_transaction_trend` (spec §4); test reconciliation đọc thêm `fact_transactions`, cũng đã có từ STEP 4. Đặt sau 6c là cho gọn thứ tự đọc, không phải ràng buộc — chạy được ngay sau STEP 5.

#### `--vars` là BẮT BUỘC kể cả ở lần `--full-refresh` — giống STEP 6c, khác lý do

Var **không** đổi gì ở build: `--full-refresh` làm `is_incremental()` false nên model không chạm macro. Nó quyết định **test soi ngày nào**. Không truyền thì `batch_logical_date()` rơi về `current_date()`, chọn một partition rỗng trên dữ liệu 2019, và `rpt_merchant_error_daily_reconciliation` so 0 với 0 rồi pass mà chưa kiểm gì. Đó là **check quan trọng nhất của bảng** (spec §9) — để nó chạy rỗng còn tệ hơn không chạy, vì nó để lại một dấu xanh.

Khác STEP 6c ở chỗ: ở đó vế nguồn (`dim_cards`) không bao giờ rỗng nên thiếu `--vars` làm test **fail oan**; ở đây cả hai vế cùng rỗng nên thiếu `--vars` làm test **pass oan**. Hỏng theo hai chiều ngược nhau, cùng một cách chữa.

Ngày dữ liệu vẫn là **2019-10-31** (§12).

#### Ba lần in fingerprint

`scripts/gold-dbt/sql/merchant_error_partition_fingerprint.sql`, in trước / giữa / sau hai lần chạy incremental, cùng khuôn STEP 6b/6c. Ba lần giống hệt nhau chứng minh `insert_overwrite` ghi đè đúng một partition và idempotent.

Fingerprint cộng các cột `_30d` **qua merchant trong đúng một `date_key`** — chiều duy nhất chúng additive. Không bao giờ cộng qua ngày: hai cửa sổ liền nhau chồng lấn 29 ngày (spec §5.1, registry quy tắc #7). `window_txns` ở đây **không phải** "có bao nhiêu giao dịch", nó là 30 ngày lịch sử đếm một lần cho mỗi merchant qualified. Không cột ratio nào bị sum — thay vào đó in `min`/`max` của `portfolio_error_rate_30d`, hai số **phải bằng nhau**.

#### Những con số phải ĐỌC, không chỉ diff

Bốn test Critical pass vẫn chưa nói tham số còn hợp lệ. Đọc bảng này ngay sau lần chạy đầu:

| Số | Kỳ vọng | Lệch nhiều nghĩa là gì |
| -- | ------- | ---------------------- |
| `baseline_min` = `baseline_max` | ≈ `0.016090` | Bằng nhau là bắt buộc (test `baseline_uniform` gác). **Giá trị** lệch xa 1,609% là chữ ký của **gộp `mcc` sai** — lỗi nguy hiểm nhất của bảng (spec §6). Đây là số đáng xem trước nhất. |
| `flagged_merchants` | ~12 | Ngoài 10–50 thì test `flagged_count_band` (severity `warn`) sẽ kêu. Đó là tín hiệu **tham số đã trôi**, không phải lỗi dữ liệu — xử lý bằng cách calibrate lại (`docs/metrics/abnormal_error_rate_calibration.md`), không phải sửa model. |
| `partition_rows` | ~165 | Số merchant qua sàn 50 trong cửa sổ. |
| `total_rows` | ~590k | Ước tính spec §8.2, **chưa từng đo**. Lệch xa thì xem lại phạm vi spine. |

#### Kỳ vọng số test

Indirect selection để mặc định (eager): **34 test** (30 schema + 4 singular). `cautious` cho **30** — bỏ đúng 4 singular test, vì parent còn lại của chúng là `fact_transactions` / `fact_daily_transaction_trend` nằm ngoài selection.

#### Rủi ro chi phí đã biết ở lần full-refresh

Spine dựng ở mức (merchant, mcc) từ ngày giao dịch đầu của cặp tới ngày cuối + 29 (spec §8.1 bước 3, §8.3). Merchant hoạt động suốt 10 năm vẫn sinh ~3.590 dòng spine cho mỗi cặp, nên bảng trung gian có thể lên vài chục triệu dòng cho một output ~590k dòng. Chấp nhận để giữ **một đường tính duy nhất** cho cả hai nhánh jinja (spec Decision Log 2026-07-25). Nhánh incremental chỉ đọc 30 ngày nên rẻ — nếu full-refresh chậm bất thường thì đó là chỗ để nhìn, và số liệu thật cần được ghi ngược vào spec §8.2.

---
## 14. STEP 7 — `card_owner_factless`, chạy CUỐI

Tài liệu đầy đủ của model này ở §8.2. Lệnh nằm ở cuối file vì script chạy từ trên xuống, mà khối mô tả nó lại nằm phía trên STEP 4. Chạy nó trước stage 4b sẽ dựng bridge từ những phiên bản dimension mà 4b sau đó đóng lại — đúng cái thứ tự staleness mà hai singular test của nó sinh ra để bắt.

---
## 15. STEP 8 — Kiểm tra độ ổn định của snapshot

Tiêu chí 2 và 3 ở `docs/known_issues/dbt_spark_relation_cache.md` §6: chạy lại `dbt snapshot` trên nguồn không đổi, in `row_count` / bounds của `dbt_valid_from` / `open_rows` trước và sau (`scripts/gold-dbt/sql/snapshot_stability.sql`). Output giống hệt nhau nghĩa là merge **hành xử** như merge chứ không phải recreate — lần chạy trước đó mới chỉ chứng minh snapshot **phát ra** `merge into`.

**Kết quả và diễn giải: cùng tài liệu, §6.1.** Lưu ý nó **không** làm bộ test SCD2 trở nên có ý nghĩa; §4.3 và §7 của tài liệu đó vẫn nguyên hiệu lực.

Chạy độc lập được: `dbt snapshot` chỉ đụng bảng snapshot, không đụng dim/fact dẫn xuất. Bật lại sau mỗi lần nâng dbt-spark — macro override `dbt/macros/spark_list_relations_without_caching.sql` là bản sao hình dạng của internals adapter nên có thể lệch.

---
## 16. STEP 9 — Calibrate tham số Abnormal Error Rate

Sáu phép đo `dbt show` trên `fact_daily_transaction_trend`, để thay ngưỡng tạm (5%) và sàn volume tạm (50) của metric Abnormal Error Rate bằng giá trị có căn cứ. **Đã chạy 2026-07-24, chốt ngưỡng 4,0% / sàn 50** (§18). SQL của cả 6 nằm ở `scripts/gold-dbt/sql/calibrate_abnormal_error_rate_{1_baseline,2_volume_distribution,3_error_distribution,4_threshold_flags,4b_top_merchants,5_dispersion}.sql` (§1.1 giải thích vì sao mỗi file lặp lại CTE thay vì dùng chung).

**READ-ONLY.** Không build model, không ghi partition nào. Vì thế không cần `--vars`, và bẫy `2036-01-01` (§17.1) không áp dụng.

**Phương pháp, tiêu chí chấp nhận, kết quả và tham số chốt: `docs/metrics/abnormal_error_rate_calibration.md`.** Ghi số vào **đó**, đừng để lại trong script.

Hai điều phải biết trước khi sửa SQL:

- `merchant_window` group by `merchant_id` **một mình**, cộng qua mọi dòng `mcc` kể cả bucket `'-1'`. Đó là quy tắc aggregate #6 của `docs/metrics/metrics_layer.md` §3: group thêm theo `mcc`, hoặc lọc bỏ `'-1'`, đều làm mẫu số thiếu và thổi phồng mọi tỷ lệ ở đây.
- Các sàn volume ứng viên nằm trong CTE `floors` (`values (50), (100), (200)`), giống hệt nhau ở measure 2, 3, 4 và 5 — sửa danh sách đó thì cả bốn đi theo, mỗi sàn một dòng kết quả. Riêng **measure 4b cố định ở sàn 50**: nhiệm vụ của nó là trả lời "sàn 50 có đủ không", nên nó phải tiếp tục cho thấy sàn 50 nhận vào những ai.

Lần chạy đầu (2026-07-24) dùng một sàn 50 duy nhất và ngưỡng 3/5/8/10%. Kết quả buộc phải mở rộng cả hai lưới: 8% và 10% không bắt được merchant nào, 5% chỉ bắt 6, còn measure 4b cho thấy đầu danh sách toàn merchant 51–66 giao dịch. **Measure 5 (dispersion) được thêm giữa chừng** vì bốn phép đo gốc không trả lời được câu hỏi đứng trước mọi câu hỏi khác — có hiệu ứng merchant thật để phát hiện không, hay mọi shortlist đều là dương tính giả. Nó cho dispersion 2,45–3,45, tức có tín hiệu thật. Chi tiết số liệu và lập luận: `docs/metrics/abnormal_error_rate_calibration.md`.

> **Đừng đọc measure 4b như một mẫu thống kê.** Nó sắp theo error rate, mà merchant volume thấp mới sinh được tỷ lệ cực đoan, nên bảng này thiên lệch có hệ thống về phía n nhỏ. Trong lần chạy 2026-07-24, dùng nó để nhẩm tỷ lệ dương tính giả đã cho ra kết luận ngược hẳn với measure 5. Nó là công cụ chẩn đoán định tính, không phải nguồn để ước lượng.

Không `|| true`: khác probe ở STEP 6b, kết quả ở đây *chính là* sản phẩm cần lấy nên fail phải dừng. Measure 1 trả 1 dòng, measure 2/3/4/5 mỗi cái 3 dòng (một dòng mỗi sàn) — đều nằm trong giới hạn 5 dòng mặc định của `dbt show`; chỉ 4b cần `--limit`, và file SQL của nó **không** được tự mang `limit` — xem §17.3. Mọi file đều giữ ở ≤ 6 cột vì §17.4.

---
## 17. Bẫy chung

### 17.1. KHÔNG dùng lại `2036-01-01`

Đó là sentinel "xử lý tất cả" mà `scripts/deploy_gold_job_dev.sh` và `deploy_silver_job_dev.sh` truyền cho các job PySpark cũ. **Với dbt thì nó hỏng âm thầm.**

`dim_dates` kết thúc ở 2035-12-31 (`dbt_project.yml`, var `dim_date_end_date`), nên `date_key = 20360101` không khớp dòng spine nào: model sẽ tính cửa sổ trên toàn bộ lịch 2010–2035 rồi ghi **ZERO dòng**, không ghi đè partition nào, trong khi reconciliation test vẫn pass vì cả hai vế đều bằng 0.

Áp dụng cho cả `fact_customer_activity_daily`, `fact_daily_transaction_trend` và `fact_user_monthly_snapshot`.

### 17.2. Script không ghi trạng thái — §18 mới là nơi tra

Trong script **không có** dấu `DONE`/`PASSED` nào: nó chỉ chứa code. Muốn biết bước nào đã chạy, chạy khi nào, kết quả ra sao thì tra **§18**. Mọi lệnh trong script đều đang comment sẵn, nên trạng thái "đã chạy hay chưa" không đọc được từ chính file đó.

### 17.3. File SQL cho `dbt show` không được tự mang `limit` ở cuối

`dbt show --limit N` **nối thêm chuỗi `limit N` vào cuối** câu SQL đã compile, chứ không bọc nó trong subquery. Nên nếu file SQL đã tự kết thúc bằng `limit 20` thì cái gửi xuống Spark là `... limit 20 limit 25` → `[PARSE_SYNTAX_ERROR] Syntax error at or near 'limit'`.

Đã dính đúng lỗi này ở measure 4b trong lần chạy đầu 2026-07-24. Quy ước từ đó: **số dòng chỉ đặt ở `--limit` trên dòng lệnh**, file SQL giữ `order by` và thôi.

Lưu ý `limit` **bên trong** một CTE hay subquery thì hoàn toàn bình thường — `window_days` vẫn dùng `limit 30` để lấy 30 ngày gần nhất. Bẫy chỉ nằm ở `limit` cuối cùng của câu ngoài cùng.

Hệ quả nữa: vì phần đuôi được nối thẳng vào text, câu SQL ngoài cùng cũng không nên kết thúc bằng dấu `;`.

### 17.4. `dbt show` chỉ in được 6 cột, tên cột tối đa 20 ký tự

`dbt show` **cắt bớt cột thừa thành `| ... |`** và số bị mất luôn — không moi lại được từ log lẫn từ `target/run_results.json` (artifact đó không chứa dữ liệu preview).

Nguyên nhân nằm ở `dbt/task/show.py`: nó gọi `table.print_table(output=output, max_rows=None)` mà **không truyền `max_columns`**, nên agate áp mặc định của mình:

| Giới hạn agate | Giá trị | Hậu quả |
| -------------- | ------- | ------- |
| `max_columns` | 6 | cột thứ 7 trở đi biến thành `| ... |` |
| `max_column_width` | 20 | tên/giá trị dài hơn bị cắt thành `expected_failed_a...` |
| `max_precision` | 3 | số thập phân chỉ hiện 3 chữ số |

**`--printer-width` KHÔNG sửa được cái này.** Đã thử ở lần chạy 16:44 ngày 2026-07-24: log xác nhận `'printer_width': '200'` vào đúng, nhưng output vẫn mất cột y như cũ, vì hai giới hạn trên không liên quan gì tới độ rộng terminal. Đừng lặp lại thí nghiệm đó.

Hai cách thật sự dùng được:

1. **Giữ query ≤ 6 cột và tên cột ≤ 20 ký tự.** Đây là quy ước của repo, cả 6 measure ở STEP 9 đều tuân theo, và mỗi file SQL ghi rõ cột nào đã bị bỏ để nhường chỗ.
2. **`--output json`** — `show.py` dùng `table.to_json()` cho nhánh này, không qua agate rendering nên không bị giới hạn nào. Dùng khi thật cần nhiều cột, đổi lại log khó đọc hơn.

### 17.5. Log dbt là nguồn tra kết quả, không cần copy từ terminal

`dbt/logs/dbt.log` giữ nguyên bảng kết quả của mọi lần `dbt show` (dòng `Previewing inline node:`), kèm timestamp và cả câu SQL đã compile. Không cần bôi đen terminal:

```bash
grep -n "Previewing inline node" -A 4 dbt/logs/dbt.log | sed 's/\x1b\[[0-9;]*m//g' | tail -40
```

`sed` để bỏ mã màu ANSI. File này append liên tục qua nhiều lần chạy nên nhớ đối chiếu timestamp để lấy đúng lần chạy mình cần.

---
## 18. Lịch sử chạy

**Đây là nơi duy nhất ghi bước nào đã chạy** — script không mang trạng thái (§17.2). Mọi step trong bảng dưới đều đã xong và đang comment trong script; **không cần chạy lại** trừ khi có lý do ở cột ghi chú.

Đợt rebuild toàn bộ chuỗi gold, dev, **2026-07-24**:

| Giờ | Step | Kết quả | Khi nào chạy lại |
| --- | ---- | ------- | ---------------- |
| 10:04 | 4a | 35/35 green — snapshot phát `merge into` lần đầu tiên | Khi cần restate lại chuỗi point-in-time |
| 10:10 | 4b | 82/82 green | Như trên |
| 10:13 | 5 | 25/25 green (24 test kỳ vọng + model) | Chỉ khi cần dựng lại toàn lịch sử |
| 10:17 | 6 | 16/16 green (15 test kỳ vọng + model) | Như trên |
| 10:18 | 7 | 6/6 green (model + 5 test kỳ vọng) | Mỗi lần dimension được rebuild (§14) |
| 10:36–10:44 | 6b | PASS — fingerprint giống nhau cả 3 lần, xem `docs/known_issues/dbt_spark_relation_cache.md` §6.1 | Sau mỗi thay đổi logic incremental / partition / version dbt-spark |
| 10:44 | 8 | PASS — snapshot ổn định, cùng tài liệu §6.1 | Sau mỗi lần nâng dbt-spark |
| 17:12–17:15 | 9 | **PASS** — cả 6 measure. Chốt tham số Abnormal Error Rate: **ngưỡng 4,0%, sàn 50**. Số liệu ở `docs/metrics/abnormal_error_rate_calibration.md` §5 | Khi nguồn có biến động thật, hoặc trước khi build `rpt_merchant_error_daily` |

Đợt `rpt_card_portfolio`, dev, **2026-07-25** (giờ theo đồng hồ của `dbt/logs/dbt.log`):

| Giờ | Step | Kết quả | Khi nào chạy lại |
| --- | ---- | ------- | ---------------- |
| 04:33–04:42 | 6c full-refresh | **PASS=35/35** (1 model + 34 test), 551,6s | Chỉ khi cần dựng lại toàn lịch sử |
| 04:43 | 6c fingerprint 1 | `total_rows=17955, partition_rows=5, issued=6146, active=3385, window_txns=343077, window_failed=5546` | — |
| 04:43–04:46 | 6c incremental 1 | **PASS=35/35**, 187,1s — fingerprint **giống hệt** baseline | Sau mỗi thay đổi logic incremental / partition |
| 04:47–04:50 | 6c incremental 2 | **PASS=35/35**, 187,3s — fingerprint vẫn giống hệt ⇒ **idempotent** | Như trên |
| 04:51 | 6c chẩn đoán Δ | `report_active=3385, activity_active=3385, delta_cards=0, residual=0` ⇒ **Δ = 0 đúng như spec Open Question #4 kỳ vọng** | Chạy một lần là đủ |

> **Đợt này suýt bị mất khỏi tài liệu.** Nó chạy xong 2026-07-25 nhưng không được ghi vào §18 ngay, và tới tối cùng ngày thì cả người chạy lẫn `docs/metrics/metrics_layer.md` đều còn ghi là "chưa chạy dev". Cứu được nhờ `dbt/logs/dbt.log` chưa bị xoá — **đừng dựa vào may mắn đó**: §17.2 nói script không mang trạng thái, nên bước nào chạy xong mà không ghi vào §18 thì coi như chưa từng chạy. Ghi ngay sau khi chạy.
>
> Hai số đáng chú ý: `total_rows = 17.955` **thấp hơn** ước tính 35–55 nghìn của spec §8.2 (ước tính đó nay đã có số thật để thay), và `partition_rows = 5` — đúng 5 tổ hợp `(chip_segment, card_brand)` có mặt trong dữ liệu synthetic.

Đợt `rpt_merchant_error_daily`, dev, **2026-07-25** (cùng ngày, sau đợt trên):

| Giờ | Step | Kết quả | Khi nào chạy lại |
| --- | ---- | ------- | ---------------- |
| 07:39–07:52 | 6d full-refresh | **PASS=35/35**, WARN=0, 762,0s (12m42s) | Chỉ khi cần dựng lại toàn lịch sử, hoặc khi đổi tham số và muốn áp cho quá khứ |
| 07:52 | 6d fingerprint 1 | `total_rows=584908, partition_rows=165, flagged_merchants=12, window_txns=80008, window_failed=1398, baseline=0.016090` | — |
| 07:53–07:56 | 6d incremental 1 | **PASS=35/35**, 180,5s — fingerprint **giống hệt** baseline | Sau mỗi thay đổi logic incremental / partition |
| 07:56–07:59 | 6d incremental 2 | **PASS=35/35**, 174,9s — fingerprint vẫn giống hệt ⇒ **idempotent** | Như trên |

> **Mọi con số đều trúng dự đoán, kể cả số quan trọng nhất.** `baseline = 0.016090` **khớp đúng đến chữ số cuối** với mặt bằng 1,609% mà calibration đo độc lập bằng truy vấn ad-hoc ngày 2026-07-24 (§16, `docs/metrics/abnormal_error_rate_calibration.md` §5). Hai đường tính hoàn toàn khác nhau — một bên `group by` phẳng trên 30 `date_key` gần nhất, một bên rolling window trên spine (merchant, mcc) — cho cùng một số. Đó là bằng chứng mạnh nhất hiện có rằng **phép gộp qua `mcc` không sót dòng nào**, đúng thứ mà §6 của spec gọi là lỗi nguy hiểm nhất bảng này có thể mắc. `flagged_merchants = 12` cũng khớp calibration, và nằm trong band 10–50 nên test `flagged_count_band` im lặng (WARN=0).
>
> `total_rows = 584.908` so với ước tính ~590 nghìn của spec §8.2, `partition_rows = 165` so với ~165 — cả hai trúng.
>
> Chi phí đúng như cảnh báo ở §13.1: full-refresh **762s**, gấp ~1,4 lần `rpt_card_portfolio` (552s), do spine (merchant, mcc) × ngày. Nhánh incremental chỉ ~178s, rẻ hơn cả 6c — đúng như thiết kế, vì nó chỉ đọc 30 ngày.

Probe `min/max date_key` của STEP 6b (§12) trả `min_dk=20100101`, `max_dk=20191031` → ngày dữ liệu là **2019-10-31**. Con số này còn đúng chừng nào nguồn synthetic chưa đổi.

> Green ở bảng này chứng minh model build được, test khớp, cột đúng kiểu. Nó **chưa** chứng minh tính đúng của as-of join (nguồn synthetic tĩnh nên test SCD2 hiện pass rỗng nghĩa) — xem `docs/metrics/metrics_layer.md` §2 và `docs/known_issues/dbt_spark_relation_cache.md` §7.
