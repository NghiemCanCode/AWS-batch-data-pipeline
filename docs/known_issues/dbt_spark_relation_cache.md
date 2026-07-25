# Known Issue: dbt-spark relation cache rỗng — snapshot xoá lịch sử SCD2 mỗi run

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.3       |                                                |
| Owner (issue) | NghiemCanCode |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-25    |                                                |
| Severity      | **High**      | Không sai số hiện tại, nhưng huỷ lịch sử SCD2 và làm bộ test SCD2 mất hiệu lực |
| Status        | **Fixed — đã verify trên dev 2026-07-24** | Macro override `dbt/macros/spark_list_relations_without_caching.sql`. Toàn bộ tiêu chí §6 đã pass. §7 (nguồn tĩnh) **vẫn Open** và vẫn chặn SCD2 demo. |

### Changelog

| Version | Date       | Author        | Change                                                                                     |
| ------- | ---------- | ------------- | -------------------------------------------------------------------------------------------- |
| v.0.0.1 | 2026-07-24 | NghiemCanCode | Ghi nhận lần đầu sau khi truy được đúng dòng code gây lỗi trong dbt-spark và đo được phạm vi hậu quả. |
| v.0.0.2 | 2026-07-24 | NghiemCanCode | Sửa xong và verify trên dev. Bổ sung §2.2 (nâng lên dbt-spark 1.11.0 biến bug từ fail-open thành fail-loud), §6.1 (kết quả verify thực tế), §8.1 (hệ quả IAM: staging view của snapshot rơi vào Glue db `default`). Open Question #1 và #3 đã đóng. |
| v.0.0.3 | 2026-07-25 | NghiemCanCode | **Đảo ngược quyết định Terraform-hoá grant `default`**: nó ở lại vĩnh viễn trong STEP 0c của script. §8.1 viết lại — đính chính hai chỗ sai của v.0.0.2 (grant **chưa từng** có trong `iam.tf`; "xoá khỏi script sau khi apply" là chỉ dẫn phá session), và nói rõ vòng đời: grant `default` **không** chết theo silver, khác `FinanceSilverRead`. Vá kèm một bẫy: `glue:GetDatabase` trên `database/default` trước đây chỉ nằm trong statement sắp bị xoá, nay đã thêm vào statement vĩnh viễn. Open Question #5 đóng, #6 mở kế thừa (orchestrator chạy ngoài script). |

---
## 1. Tóm tắt

> **Đây là bug nuốt lỗi (error swallowing) của dbt-spark, không phải bug trong code của project.** Một exception handler bắt lỗi không nhận diện được rồi trả về "schema rỗng" thay vì báo lỗi. dbt vì thế tin rằng mọi bảng trong `gold` và `gold_staging` chưa tồn tại, nên snapshot phát `create or replace table` thay vì `merge into` — **xoá sạch lịch sử SCD2 sau mỗi lần chạy mà vẫn báo "OK"**.

Phân loại: **fail-open**. Lẽ ra phải fail loud thì lại trả kết quả rỗng trông hợp lệ. Nguy hiểm hơn crash, vì không có tín hiệu nào để phát hiện.

**Môi trường:** `dbt-core 1.11.12`, `dbt-spark 1.10.3` (khi phát hiện) → `1.11.0` (khi sửa), EMR Serverless + Glue Catalog + Iceberg.

> **CẬP NHẬT v.0.0.2 — đã sửa.** Override `spark__list_relations_without_caching`
> tại `dbt/macros/spark_list_relations_without_caching.sql`. Verify trên dev
> 2026-07-24: snapshot phát `merge into`, `dbt_valid_from` giữ nguyên qua 2 lần
> chạy liên tiếp, incremental ghi đúng 1 partition và idempotent. Chi tiết §6.1.
>
> **Phần còn Open là §7 (nguồn dữ liệu tĩnh), không phải bug này.** Vì mỗi entity
> vẫn chỉ có 1 version, các cảnh báo ở §4.3 (bộ test SCD2 pass rỗng nghĩa) và
> §4.4 (as-of join chưa được kiểm chứng) **vẫn còn nguyên hiệu lực**. Sửa cache
> mới chỉ đảm bảo lịch sử **sẽ** tích được từ nay, chứ chưa tạo ra lịch sử nào.

---
## 2. Nguyên nhân kỹ thuật

Chuỗi nhân quả 3 bước:

**Bước 1 — điều kiện kích hoạt (môi trường).** `scripts/gold-dbt/deploy_gold_dbt_dev.sh` (khối `CONFIGURATION_OVERRIDES`) đặt `spark.sql.catalogImplementation=hive` và `spark_catalog = SparkSessionCatalog`. Cấu hình này là **cố ý và cần thiết**: silver là bảng Hive do Glue Crawler tạo, gold là Iceberg, cả hai phải cùng resolve dưới một catalog. Hệ quả phụ: lệnh `SHOW TABLE EXTENDED` đi qua đường Hive client của Glue, client này đòi đọc `StorageDescriptor#InputFormat` của từng bảng — bảng Iceberg có `InputFormat` **null hợp lệ** (Glue ghi `table_type: ICEBERG`) ⇒ ném `HiveException`.

```
org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table dim_cards.
StorageDescriptor#InputFormat cannot be null for table: dim_cards
```

Lệnh chết ngay ở bảng Iceberg đầu tiên theo thứ tự alphabet — `dim_cards` trong `gold`, `snapshot_cards` trong `gold_staging`.

**Bước 2 — bug thật (dbt-spark).** Trong `dbt/adapters/spark/impl.py`, hàm `list_relations_without_caching` (khoảng dòng 232-270) bắt `DbtRuntimeError` và **chỉ** nhận diện đúng một chuỗi lỗi để chuyển sang đường xử lý Iceberg:

```python
elif "SHOW TABLE EXTENDED is not supported for v2 tables" in errmsg:
    # đường Iceberg: show tables + describe extended  ← đúng, nhưng không bao giờ tới
else:
    logger.debug(f"Error while retrieving information about {schema_relation}: {errmsg}")
    return []          # ← lỗi Glue rơi vào đây
```

Điều đáng nói: **adapter đã có sẵn đường xử lý đúng cho Iceberg** (`list_relations_show_tables_without_caching` + `_get_relation_information_using_describe`). Nó chỉ không bao giờ được kích hoạt vì điều kiện vào cửa được viết bằng so khớp chuỗi lỗi cứng, mà lỗi của Glue có nội dung khác.

**Bước 3 — hậu quả trực tiếp.** `return []` không phân biệt được với "schema này không có bảng nào". Cache quan hệ rỗng vĩnh viễn ⇒ `adapter.get_relation()` luôn trả `None` ⇒ mọi materialization rẽ nhánh theo `old_relation` đều đi nhánh "tạo mới".

### 2.1. Vì sao không sửa được bằng cách tách catalog

Hướng "sửa cho đúng chuẩn" — đưa gold sang một catalog Iceberg riêng (`SparkCatalog` thay vì `SparkSessionCatalog`), để `SHOW TABLE EXTENDED` ném đúng lỗi v2 mà adapter nhận diện được — **đã kiểm tra và không khả thi**. `dbt/adapters/spark/relation.py` (`SparkRelation.__post_init__`) raise thẳng:

```python
raise DbtRuntimeError("Cannot set database in spark!")
```

dbt-spark không có khái niệm catalog. Và cũng không bỏ được `SparkSessionCatalog`, vì khi đó silver (Hive) sẽ không resolve được nữa.

> Khi silver migrate sang Iceberg (kế hoạch đã có, chưa spec), lý do "phải giữ
> `SparkSessionCatalog` vì silver là Hive" sẽ biến mất — nhưng **vẫn không tách
> catalog được**, vì rào cản thật là dòng raise ở trên, không phải silver.

### 2.2. Vì sao nó đột nhiên đổi từ "im lặng" sang "chết ngay" (2026-07-24)

Ban đầu bug này chỉ nuốt lỗi. Sau khi `pyproject.toml` ghim `dbt-spark[session] (==1.11.0)` (trước đó là `>=1.10.3,<2.0.0`), **mọi lệnh `dbt build` chết ngay ở bước dựng relation cache**, trước khi bất kỳ node nào chạy:

```
Encountered an error:
Runtime Error
  Runtime Error
    Runtime Error
      org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table
      card_owner_factless. StorageDescriptor#InputFormat cannot be null
```

Nguyên nhân: dbt-spark 1.11.0 đã sửa đúng nhánh `else` mô tả ở §2 bước 2 — đổi từ `logger.debug(...); return []` thành `raise`. Tức là **upstream đã tự giải quyết Open Question #4**: lỗi không nhận diện được giờ fail loud thay vì trả rỗng.

Hai điểm dễ nhầm khi gặp lỗi này:

- **Tên bảng trong thông báo lỗi không phải thủ phạm.** `SHOW TABLE EXTENDED` chết ở **bảng Iceberg đầu tiên theo alphabet** trong schema. Trước đó là `dim_cards`; sau khi `card_owner_factless` ra đời thì thành nó. Model đó hoàn toàn vô can.
- **Đây không phải bug mới.** Cùng một defect, chỉ khác chế độ báo lỗi. Hạ version về 1.10.3 sẽ làm hết lỗi nhưng khôi phục lại đúng cái bug âm thầm xoá lịch sử — **không được làm vậy**.

---
## 3. Phạm vi ảnh hưởng

| Đối tượng | Hành vi thực tế | Kết quả có sai không? |
| --------- | ---------------- | --------------------- |
| `snapshot_customers`, `snapshot_cards` | `create or replace table` thay vì `merge into` | **Có — lịch sử bị xoá** |
| `dim_customers`, `dim_cards` | Rebuild toàn bộ thay vì merge | Không (dựng lại từ nguồn đầy đủ), nhưng surrogate key đổi — xem §4.1 |
| `fact_transactions` | Rebuild toàn bộ thay vì merge | Không, chỉ chậm |
| `fact_daily_transaction_trend`, `fact_customer_activity_daily` | `is_incremental()` = false ⇒ build full history thay vì 1 partition | Không, chỉ chậm |
| `us_holidays` (seed) | **Vỡ ra tiếng** (`TABLE_OR_VIEW_ALREADY_EXISTS`) | Đã workaround — xem §6 |

Seed là đối tượng duy nhất báo lỗi rõ ràng, vì materialization của seed rẽ nhánh theo `old_relation` và luôn bị đẩy vào `create_csv_table`. Mọi thứ còn lại chịu đựng trong im lặng, vì materialization của chúng vốn đã phát `create or replace table`.

---
## 4. Hậu quả

### 4.1. Surrogate key bị sinh lại khác nhau sau mỗi lần chạy — **nặng nhất**

`customer_key` và `card_key` được hash từ `dbt_valid_from` của snapshot:

```sql
md5(concat_ws('||', customer_id, cast(dbt_valid_from as string)))
```

Khi snapshot bị dựng lại mỗi run, `dbt_valid_from` = thời điểm chạy run đó. Nghĩa là **toàn bộ khoá đại diện đổi giá trị sau mỗi run**, phá thẳng cam kết "stability guarantee" mà `dim_customers.sql` / `dim_cards.sql` tuyên bố chính là mục đích của việc hash `dbt_valid_from` gốc (spec section 6.1).

**Hệ quả vận hành:** dimension và fact **buộc phải build trong cùng một lần chạy, luôn luôn**. Nhưng `scripts/gold-dbt/deploy_gold_dbt_dev.sh` lại được thiết kế để chạy từng cụm riêng biệt — chỉ cần chạy `stg_customers snapshot_customers dim_customers` mà không rebuild fact là mọi `customer_key` trong `fact_transactions`, `fact_user_monthly_snapshot`, `fact_customer_activity_daily` và `card_owner_factless` trở thành mồ côi. Bug này làm toàn bộ thiết kế incremental trở nên vô dụng: không bao giờ được rebuild dimension một mình.

> **✅ ĐÃ GIẢI QUYẾT (2026-07-24).** `dbt_valid_from` nay giữ nguyên qua các run
> (verify ở §6.1), nên surrogate key ổn định và **ràng buộc "phải build cùng một
> lần chạy" ở trên không còn nữa** — rebuild dimension riêng lẻ đã an toàn.
> Chuỗi STEP đầy đủ trong `scripts/gold-dbt/deploy_gold_dbt_dev.sh` vẫn nên chạy theo thứ
> tự vì lý do khác (`card_owner_factless` đọc trạng thái current của 2 dimension,
> xem STEP 7), không phải vì bug này nữa.
>
> Đoạn trên giữ lại làm bối cảnh lịch sử: mọi khoá đại diện sinh ra **trước**
> 2026-07-24 05:58 đều đã bị thay thế, nên đừng đối chiếu với bất kỳ số liệu
> `customer_key`/`card_key` nào ghi lại từ trước mốc đó.

> **Điểm sáng:** tình huống này **vỡ ra tiếng**. Các generic relationships test và hai singular test staleness của `card_owner_factless` (`dbt/tests/card_owner_factless_customer_exists_in_dim.sql`, `..._card_exists_in_dim.sql`) bắt đúng ca này — chính comment trong test đó đã nói nó tồn tại để canh staleness giữa các run.

### 4.2. Lịch sử SCD2 bị huỷ, không phục hồi được

Snapshot là nơi **duy nhất** lịch sử tồn tại; dimension chỉ là dẫn xuất. Mỗi run xoá và dựng lại từ ảnh chụp hiện tại của silver — không có bản sao nào ở đâu khác để khôi phục.

Hiện tại chưa mất gì, vì nguồn đang tĩnh (§7). Nhưng đây là quả bom hẹn giờ: **ngày nào silver đổi thật, lịch sử tích được sẽ bị xoá ở run kế tiếp mà không có cảnh báo nào.**

### 4.3. Bộ test SCD2 đang xanh nhưng không chứng minh điều gì

Khi mỗi entity chỉ có **đúng một** version, các test được viết ra để canh SCD2 pass một cách rỗng nghĩa:

| Test | Vì sao pass vô nghĩa |
| ---- | --------------------- |
| `dim_customers_scd2_interval_coverage.sql` (và bản `dim_cards`) | Nhánh `gaps` cần tối thiểu 2 version mới có thể trả về dòng nào → luôn rỗng. Nhánh `bounds` với một version duy nhất 1900→9999 → luôn phủ đủ. |
| `dim_customers_scd2_interval_no_overlap.sql` (và bản `dim_cards`) | Một version thì không thể chồng lấn với chính nó. |
| `dim_customers_current_uniqueness.sql` (và bản `dim_cards`) | Một version thì đương nhiên duy nhất `is_current`. |

**Không được dùng bộ test này làm bằng chứng cho bất cứ điều gì về SCD2** cho tới khi cache được sửa và có version thứ 2 thật.

### 4.4. Đợt restatement point-in-time v.0.0.3 hiện chưa được kiểm chứng

Thay đổi kiến trúc lớn nhất của vòng vừa rồi — chuyển `fact_transactions` từ join `is_current` sang **as-of range join** theo khoảng hiệu lực — hiện cho ra **con số giống hệt** cách làm cũ. Lý do: mỗi entity chỉ có một version, backdate về `1900-01-01`, nên as-of join luôn khớp version 1 bất kể timestamp giao dịch.

Code có thể đúng hoặc sai; hiện **không có cách nào biết được**. Nó đang ở trạng thái *chưa được kiểm chứng*, chứ không phải *đã được kiểm chứng là đúng*.

Kéo theo: **Success Criteria 2, 3, 4** của business spec (báo cáo segment theo thuộc tính quá khứ; before/after đổi income bracket; before/after relocation) không trả lời được.

### 4.5. Chi phí và mô hình batch

Mỗi run rebuild toàn bộ `fact_transactions` trên EMR Serverless. Batch T+1 mô tả ở business spec §7 thực chất đang là **full reload mỗi ngày**.

Kèm theo một rủi ro tiềm ẩn: logic watermark incremental (`{% if is_incremental() %}` trong `dim_customers`, `dim_cards`, `fact_transactions`, và bộ lọc `batch_logical_date()` trong hai fact insert_overwrite) **chưa từng một lần được thực thi**. Nó sẽ chạy lần đầu tiên đúng vào lúc bug này được sửa — đừng coi đó là code đã chạy ổn.

> **Cập nhật 2026-07-24 — mới xong một nửa.** Nhánh `batch_logical_date()` của
> `fact_customer_activity_daily` đã chạy thật và đúng (§6.1). Còn lại **vẫn chưa
> từng chạy**: nhánh `is_incremental()` của `dim_customers`, `dim_cards`,
> `fact_transactions`, và của `fact_daily_transaction_trend` — mọi lần build
> chúng đều dùng `--full-refresh`. Vẫn áp dụng nguyên cảnh báo trên cho nhóm này.

---
## 5. Cái gì KHÔNG bị ảnh hưởng

Để không đánh giá quá mức: **các con số hiện tại trong fact và dimension không sai.** Các model incremental tuy rebuild toàn bộ thay vì merge, nhưng chúng dựng lại từ nguồn đầy đủ nên kết quả đúng — chỉ chậm và tốn.

Cụ thể: Dashboard A (Merchant & Category), Dashboard C (Card Portfolio), và phần **không** point-in-time của Dashboard B đọc ra số đúng. Thứ hỏng là **lịch sử**, không phải **trạng thái hiện tại**.

---
## 6. Bằng chứng & cách tự kiểm chứng

**Bằng chứng đã thu được** (`dbt/logs/dbt.log`, 2026-07-24, quanh dòng 64501-64533):

```
HiveException: Unable to fetch table snapshot_cards. StorageDescriptor#InputFormat cannot be null
Spark adapter: Error while retrieving information about gold_staging: Runtime Error
HiveException: Unable to fetch table dim_cards. StorageDescriptor#InputFormat cannot be null
Spark adapter: Error while retrieving information about gold: Runtime Error
```

**Cách kiểm chứng sau khi sửa — đây là tiêu chí đúng, đừng dùng tiêu chí khác:**

1. **Không tin dòng "snapshot OK" trong output của dbt.** Nó xanh trong cả hai trường hợp. Phải grep log xem SQL phát ra là `merge into` hay `create or replace table`.
2. Chạy snapshot 2 lần liên tiếp, kiểm `count(*)` của `gold_staging.snapshot_customers` **không đổi**.
3. Kiểm `dbt_valid_from` của một `customer_id` bất kỳ **không đổi** giữa hai run (đây là test cho §4.1).

**Workaround đã có sẵn trong repo:** `dbt/macros/spark_create_csv_table.sql` override `spark__create_csv_table` để phát `create or replace table`, xử lý riêng ca seed vỡ ra tiếng ở §3. Workaround này sẽ trở thành thừa (nhưng vô hại) sau khi bug gốc được sửa.

### 6.1. Kết quả verify thực tế (dev, 2026-07-24)

Cả 3 tiêu chí trên đều PASS. Chạy qua `scripts/gold-dbt/deploy_gold_dbt_dev.sh`, toàn bộ chuỗi STEP 4a→8 xanh (35 + 82 + 25 + 16 + 6 node).

**Tiêu chí 1 — SQL phát ra là `merge into`.** Grep log `dbt/logs/dbt.log` quanh 10:04 và 10:44: 2 lệnh `merge into gold_staging.snapshot_*`, **0** lệnh `create or replace table gold_staging.snapshot_*`. Trước khi sửa thì ngược lại hoàn toàn.

**Tiêu chí 2 + 3 — snapshot ổn định.** Chạy `dbt snapshot` lần nữa trên nguồn không đổi, in trạng thái trước/sau (STEP 8 của script, nay đã comment lại kèm số liệu):

| snapshot | row_count | min_vf = max_vf | open_rows |
| -------- | --------- | --------------- | --------- |
| snapshot_cards | 6146 | 2026-07-24 05:58 | 6146 |
| snapshot_customers | 2000 | 2026-07-24 05:58 | 2000 |

Hai bản in **giống hệt nhau**. Điểm mấu chốt là `dbt_valid_from` vẫn đứng ở 05:58 — mốc lần recreate cuối cùng của kỷ nguyên bug — chứ không nhảy lên 10:44. Nếu snapshot bị dựng lại thì toàn bộ `dbt_valid_from` đã phải bằng thời điểm chạy. **Surrogate key đã ổn định**, §4.1 được giải quyết.

`min_vf = max_vf` và `open_rows = row_count` phản ánh đúng nguồn tĩnh (§7): mỗi entity 1 version mở. Đây là lý do §4.3 vẫn đúng — **kết quả này không hề làm bộ test SCD2 trở nên có ý nghĩa.**

**Bonus — logic incremental chạy lần đầu tiên (§4.5).** `fact_customer_activity_daily` chạy `--vars '{batch_logical_date: 2019-10-31}'` (ngày dữ liệu lớn nhất trong `fact_transactions`; `min_dk=20100101`, `max_dk=20191031`). Fingerprint của partition in 3 lần — trước run 1, giữa 2 run, sau run 2 — đều bằng nhau:

```
total_rows=4275134  partition_rows=1219  active_customers=1206  active_cards=3385
```

Chứng minh 3 điều: insert_overwrite tái tạo đúng kết quả của full-refresh, chỉ ghi đè đúng 1 partition (`total_rows` không đổi), và idempotent. Với ngày dữ liệu thật, test reconciliation lần đầu có cả hai vế khác 0 thay vì `0 = 0` vô nghĩa của các run full-refresh.

**Cảnh báo vận hành còn lại:** logic watermark của `dim_customers` / `dim_cards` / `fact_transactions` (nhánh `is_incremental()` trong chính các model đó) **vẫn chưa từng chạy** — mọi lần build đều dùng `--full-refresh`. Chỉ có 2 fact insert_overwrite là đã được thực thi thật.

---
## 7. Vấn đề liên đới: nguồn dữ liệu tĩnh

**Sửa bug này KHÔNG đủ để demo được SCD2.** Đây là vấn đề thứ hai, độc lập.

`silver_users` đến từ file tĩnh `bronze/users_data.csv` qua `src/aws_pipeline/transformation/bronze_to_silver.py` (`transform_users`). Không có cơ chế nào làm `yearly_income` / `city` / `state` thay đổi giữa các lần chạy. Mà `snapshot_customers` dùng `strategy='check'` trên đúng `['income_bracket', 'city', 'state']`.

Nghĩa là kể cả khi `merge into` chạy đúng, **sẽ không bao giờ có version 2** — không có gì thay đổi để phát hiện. Success Criteria 2/3/4 vẫn bị chặn.

Cần một quyết định riêng về cách tạo biến động thuộc tính (xem Open Question #2 bên dưới).

---
## 8. Hướng sửa — ĐÃ THỰC HIỆN 2026-07-24

> Đã hiện thực hoá tại `dbt/macros/spark_list_relations_without_caching.sql`,
> đúng như phương án dưới đây, không lệch điểm nào. Giữ nguyên phần mô tả để
> hiểu vì sao macro được viết như vậy.

Override `spark__list_relations_without_caching` trong `dbt/macros/` — cùng kiểu workaround đã dùng cho `spark_create_csv_table.sql`:

1. Chạy `show tables in <schema> like '*'` thay cho `show table extended`. Lệnh này chỉ liệt kê tên (Spark gọi `client.listTables`, trả về tên, không fetch metadata từng bảng), nên **không** đụng `StorageDescriptor` và không dính lỗi.
2. Dựng lại kết quả thành 4 cột `(namespace, tableName, isTemporary, information)` — đúng shape mà `_get_relation_information` kỳ vọng — với cột `information` tổng hợp là chuỗi `'Provider: iceberg'`.

**Cột `information` là bắt buộc, không phải trang trí:** `dbt/include/spark/macros/materializations/snapshot.sql` (dòng ~19 và ~47) rẽ nhánh theo `target_relation.is_iceberg` để đặt tên temp view trong `merge into` — Iceberg catalog không hỗ trợ `create view` có schema/catalog. `incremental/strategies.sql` cũng đọc `existing_relation.is_iceberg`.

**Vì sao hardcode `'Provider: iceberg'` là an toàn ở project này** (đã kiểm tra 2026-07-24):
- Không có model nào materialized là `view` — nếu có, hardcode sẽ nhãn sai view thành table và `drop table` sẽ lệch với `drop view`.
- `gold` và `gold_staging` toàn bộ là Iceberg theo `dbt_project.yml` (`+file_format: iceberg` cho `staging`, `marts`, `seeds`).
- `finance_silver` (Hive) không nằm trong target schema nên không bao giờ được đưa vào cache.

> **Nếu hai điều kiện trên đổi** (thêm view, hoặc thêm bảng non-Iceberg vào gold), phải chuyển sang phương án đầy đủ: lặp `describe extended` từng bảng trong Jinja để lấy `Provider` thật, đúng như `_get_relation_information_using_describe` của adapter làm.

**Đã verify trên dev 2026-07-24:** giả định "`show tables` không đụng `StorageDescriptor`" **đúng** — Open Question #1 đóng. Xem §6.1.

**Đánh đổi đã chấp nhận:** cột `information` tổng hợp không còn khối schema từng cột mà `show table extended` vốn trả về, nên `dbt docs generate` sẽ báo 0 column cho các bảng trong `gold`/`gold_staging` (`parse_columns_from_information` không parse ra gì). Chỉ ảnh hưởng catalog artifact, không ảnh hưởng run — và đường Iceberg gốc của chính dbt-spark cũng có đúng gap này.

### 8.1. Hệ quả kéo theo: staging view của snapshot rơi vào Glue database `default`

Cache hoạt động trở lại làm lộ ra một lỗ hổng IAM chưa từng gặp, vì trước đó snapshot không bao giờ đi tới bước này:

```
User: ...EMRExecutionRoleDev... is not authorized to perform: glue:GetTable on
resource: .../table/default/snapshot_cards__dbt_tmp
```

`spark_build_snapshot_staging_table` (trong `dbt/include/spark/macros/materializations/snapshot.sql`) khi gặp target Iceberg sẽ **cố tình bỏ catalog và schema** khỏi tên staging view — Iceberg catalog không hỗ trợ `create view <schema>.<name>` — rồi `spark__snapshot_merge_sql` tham chiếu lại bằng identifier trần. Tên không qualify thì Spark resolve vào current database, tức `default`. Nên `gold_staging.snapshot_customers` đẻ ra `default.snapshot_customers__dbt_tmp`.

dbt tạo, describe, rồi drop view đó, nên execution role cần trên `table/default/*`: `GetTable`, `GetTables`, `CreateTable`, `UpdateTable`, `DeleteTable`, cộng `GetPartition` / `GetPartitions` / `BatchGetPartition` (Glue Hive client fetch partition metadata kể cả với view — thiếu nhóm này lỗi lần hai).

Cấp ở **đúng một nơi**: `scripts/gold-dbt/deploy_gold_dbt_dev.sh` STEP 0c, statement `DbtSnapshotStagingViewInDefaultDb`. **Cố ý không đưa vào Terraform** (Decision Log 2026-07-25) — `default` không phải hạ tầng của project mà là scratch namespace do Spark/dbt-spark ép ra.

**Database `default` không do Terraform quản lý, cũng không do script tạo trong thực tế:** nó được owner **tạo tay bằng AWS Console** để chạy Hive table. STEP 0b của script chỉ là lưới an toàn — nó `get-database` trước, thấy đã tồn tại thì skip, nên trên dev nhánh `create-database` chưa từng chạy. Hệ quả cần biết: đây là **hạ tầng tạo tay, không có trong state của Terraform**. Dựng một environment mới (staging/prod) sẽ không có `default`; lúc đó STEP 0b mới thật sự tạo nó. Giữ quyền ở cạnh script vì thế nhất quán với cách bản thân database được quản lý — cả hai đều nằm ngoài Terraform.

> **Cảnh báo v.0.0.3 — bản v.0.0.2 của mục này ghi sai hai chỗ.** Nó nói grant đã nằm trong `iam.tf` và "đây là nhà vĩnh viễn của nó, xoá khỏi script sau khi apply". Thực tế `iam.tf` **chưa bao giờ** có `database/default` hay `table/default/*` (policy `EMRServerlessGlueCatalogAccessDev` chỉ liệt kê `catalog`, `gold`, `gold_staging`), nên `terraform apply` không đóng được lỗ hổng này — và theo quyết định 2026-07-25 thì cũng không cần đóng ở đó nữa.

**Vòng đời: vĩnh viễn, KHÔNG chết theo silver.** Đây là chỗ dễ nhầm nhất vì chữ `default` xuất hiện ở cả hai statement của inline policy. Statement `FinanceSilverRead` đúng là tạm — `finance_silver` biến mất khi silver migrate sang Iceberg. Nhưng `DbtSnapshotStagingViewInDefaultDb` tồn tại vì hai lý do độc lập hoàn toàn với tầng silver:

1. Spark bật Hive support luôn probe database `default` lúc boot (README §3) — quy ước Hive, và config `SparkSessionCatalog` vẫn giữ sau khi silver lên Iceberg.
2. Staging view của snapshot rơi vào `default` là vì target **là Iceberg**. Silver lên Iceberg làm lý do này mạnh thêm, không mất đi.

**Hệ quả bắt buộc khi migrate silver:** lúc xoá statement `FinanceSilverRead`, quyền `glue:GetDatabase` trên `database/default` sẽ mất theo nếu không cẩn thận — trước 2026-07-25 action đó **chỉ** nằm trong `FinanceSilverRead`, còn `DbtSnapshotStagingViewInDefaultDb` không có. Thiếu nó là Spark chết ngay lúc boot với đúng lỗi `AccessDenied` ở README §3. Đã vá 2026-07-25 bằng cách thêm `glue:GetDatabase` vào statement `DbtSnapshotStagingViewInDefaultDb`, để nó tự đứng được một mình. **Xoá `FinanceSilverRead` giờ an toàn; xoá `DbtSnapshotStagingViewInDefaultDb` thì không bao giờ.**

---
## 9. Open Questions & Decision Log
### Open Questions
> `Blocking?` = câu hỏi này có chặn việc go-live / approve spec không.

| #   | Question | Blocking? | Owner | Status |
| --- | -------- | --------- | ----- | ------ |
| 1   | Phương án override macro (§8) có thực sự chạy được trên EMR Serverless + Glue không — cụ thể `show tables in <schema>` có tránh được lỗi `StorageDescriptor` như suy luận không? Chỉ xác nhận được bằng một lần chạy thật. | No | NghiemCanCode | **Closed 2026-07-24 — CÓ.** Verify trên dev, §6.1. |
| 2   | Cách tạo biến động thuộc tính cho nguồn tĩnh (§7): (a) script sinh biến động trên một lát `silver_users` chạy giữa các lần snapshot — trung thực với luồng batch hơn; hay (b) seed một bảng change-event tổng hợp rồi apply trước khi snapshot. | No | NghiemCanCode | **Open — giờ là thứ duy nhất chặn SCD2 demo.** Cache đã sửa xong, nên đây là nút thắt còn lại cho Success Criteria 2/3/4. |
| 3   | Sau khi sửa, có nên gỡ workaround `dbt/macros/spark_create_csv_table.sql` không, hay giữ lại như lớp phòng vệ? | No | NghiemCanCode | **Closed 2026-07-24 — giữ lại.** Xem Decision Log. |
| 4   | Có nên báo bug ngược lên upstream `dbt-spark` không (nhánh `else` nên fail loud thay vì `return []`, hoặc nới điều kiện nhận diện lỗi Iceberg)? | No | NghiemCanCode | **Closed 2026-07-24 — không cần.** Upstream đã tự sửa ở 1.11.0 (§2.2): nhánh `else` giờ `raise`. Phần "nới điều kiện nhận diện lỗi Iceberg" thì vẫn chưa, nhưng macro override đã xử lý nên không còn động lực báo lên. |
| 5   | ~~Khi nào `terraform apply` grant `default` (§8.1), và ai làm?~~ — **Closed 2026-07-25: không đưa vào Terraform, câu hỏi không còn hiệu lực.** Grant sống vĩnh viễn trong STEP 0c của script. Rủi ro mà câu hỏi này nêu ra thì vẫn còn nguyên và giờ là vĩnh viễn: ai chạy dbt **ngoài** `deploy_gold_dbt_dev.sh` (session thủ công, orchestrator sau này) sẽ dính `AccessDenied`. Khi có orchestrator, nó phải tự cấp lại grant này — chuyển thành Open Question #6. | No | NghiemCanCode | **Closed 2026-07-25** |
| 6   | Khi gắn orchestrator (Airflow/Step Functions) chạy dbt ngoài `deploy_gold_dbt_dev.sh`: cấp `DbtSnapshotStagingViewInDefaultDb` cho execution role bằng cách nào, khi đã chốt không Terraform-hoá? Lựa chọn: orchestrator tự `put-role-policy` như script đang làm, hay tách một inline policy dùng chung do người vận hành cấp một lần. | No | NghiemCanCode | **Open** — kế thừa từ #5. |

### Decision Log
> `Rationale` = lý do đằng sau quyết định (ràng buộc, đánh đổi, yêu cầu business)

| Date       | Decision | Rationale | Decided by |
| ---------- | -------- | --------- | ---------- |
| 2026-07-24 | Ghi nhận thành known-issue riêng thay vì nhét vào Open Question của từng bảng | Lỗi cắt ngang snapshot, cả 2 dimension SCD2, cả 4 fact và bộ test — cần một chỗ mô tả đầy đủ chuỗi nhân quả; spec từng bảng chỉ trỏ tới đây. Cùng lý do đã tách `docs/metrics/metrics_layer.md`. | NghiemCanCode |
| 2026-07-24 | **Chưa sửa trong đợt này**, chấp nhận rủi ro có ý thức | Nguồn đang tĩnh nên chưa có lịch sử nào để mất (§4.2); các con số hiện tại vẫn đúng (§5). Ưu tiên hoàn thiện coverage của gold layer trước. Điều kiện bắt buộc phải sửa: **trước** khi silver bắt đầu thay đổi thuộc tính thật, hoặc **trước** khi dùng bộ test SCD2 làm bằng chứng cho bất cứ điều gì. | NghiemCanCode |
| 2026-07-24 | Không theo hướng tách catalog Iceberg riêng | `SparkRelation.__post_init__` raise `"Cannot set database in spark!"` — dbt-spark không có khái niệm catalog; và bỏ `SparkSessionCatalog` sẽ làm silver (Hive) không resolve được (§2.1). | NghiemCanCode |
| 2026-07-24 | **Đảo ngược quyết định "chưa sửa trong đợt này" ở dòng trên — sửa ngay.** | Không còn là lựa chọn: nâng lên dbt-spark 1.11.0 biến bug thành fail-loud, mọi `dbt build` đều chết (§2.2). Hai đường thoát là hạ version (khôi phục lại bug âm thầm xoá lịch sử — không chấp nhận được) hoặc sửa thật. Chọn sửa thật. | NghiemCanCode |
| 2026-07-24 | Không hạ `dbt-spark` về 1.10.3 để "hết lỗi" | Lỗi biến mất nhưng bug quay lại nguyên vẹn ở dạng nguy hiểm hơn — fail-open, xoá lịch sử SCD2 mà không có tín hiệu nào. Fail-loud là cải tiến của upstream, không phải hồi quy. | NghiemCanCode |
| 2026-07-24 | Hardcode `'Provider: iceberg'` thay vì `describe extended` từng bảng | Đã kiểm 2 điều kiện an toàn ở §8 và cả hai đều đúng: không model nào materialized là `view`/`ephemeral`, và `gold`/`gold_staging` toàn Iceberg. Phương án `describe` tốn N round-trip mỗi schema mà vẫn không lấy được schema cột cho catalog. Kèm điều kiện đảo ngược ghi rõ trong macro. | NghiemCanCode |
| 2026-07-24 | **Giữ lại** `spark_create_csv_table.sql` (Open Question #3) | Nó vô hại và idempotent, còn chi phí gỡ là một lần chạy dev nữa để chứng minh seed vẫn ổn. Quan trọng hơn: nó là lớp phòng vệ nếu macro override bị lệch sau một lần nâng dbt-spark — hai workaround độc lập, hỏng cái này không kéo theo cái kia. | NghiemCanCode |
| 2026-07-24 | Cấp quyền Glue trên `default` thay vì ép staging view của snapshot về `gold_staging` | Ép về schema khác sẽ phải override thêm cả `spark__snapshot_merge_sql` (nó tham chiếu source bằng identifier trần), tức là fork sâu hơn vào internals của adapter — càng dễ vỡ khi nâng version. `default` vốn đã là scratch namespace bắt buộc của Spark ở project này (STEP 0b của script tạo nó), nên cấp quyền ở đó là thuận theo thiết kế upstream. §8.1. | NghiemCanCode |
| 2026-07-24 | ~~Grant `default` vào Terraform, nhưng grant `finance_silver` thì **không**~~ — **đảo ngược 2026-07-25, xem dòng dưới.** Ghi lại để giữ lịch sử. | Hai vòng đời khác nhau: `default` là vĩnh viễn (chừng nào còn dbt snapshot Iceberg), còn `finance_silver` sẽ biến mất khi silver migrate sang Iceberg. Terraform-hoá thứ sắp bị xoá chỉ tạo thêm việc dọn. *(Phần vòng đời vẫn đúng; chỉ kết luận "vào Terraform" bị đảo. Trên thực tế grant cũng chưa từng được thêm vào `iam.tf`.)* | NghiemCanCode |
| 2026-07-25 | **Grant `default` KHÔNG đưa vào Terraform** — sống vĩnh viễn trong STEP 0c của `deploy_gold_dbt_dev.sh`. Đảo ngược quyết định 2026-07-24 ở dòng trên. | `default` không phải hạ tầng của project mà là scratch namespace bị Spark (Hive probe lúc boot) và dbt-spark (staging view của snapshot Iceberg) ép phải có. Bản thân database đó do owner **tạo tay bằng AWS Console** để chạy Hive table — nằm ngoài Terraform hoàn toàn (STEP 0b của script chỉ là lưới an toàn cho environment mới, trên dev nó luôn skip). Terraform-hoá riêng phần quyền trong khi resource thì không là xé một mối quan tâm ra hai chỗ, và mỗi lần workaround đổi lại phải sửa hai nơi. Terraform giữ đúng vai mô tả hạ tầng chủ đích. **Cái giá chấp nhận:** ai chạy dbt ngoài script sẽ `AccessDenied` — vĩnh viễn chứ không còn là tạm (Open Question #6). | NghiemCanCode |
| 2026-07-25 | Thêm `glue:GetDatabase` vào statement `DbtSnapshotStagingViewInDefaultDb` | Action đó trước giờ **chỉ** có trong `FinanceSilverRead`. Vì statement kia sẽ bị xoá khi silver lên Iceberg, mà grant `default` thì ở lại vĩnh viễn, để nguyên là tự đặt bẫy: đến ngày migrate, xoá `FinanceSilverRead` là Spark chết ngay lúc boot. Giờ mỗi statement tự đứng được, xoá cái tạm không kéo đổ cái vĩnh viễn. | NghiemCanCode |
