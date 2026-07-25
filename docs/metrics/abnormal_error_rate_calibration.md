# Calibration: tham số của Abnormal Error Rate (merchant)

---
## 0. Document Control

| Attribute     | Value         | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| Doc status    | Draft         | <Draft \| In Review \| Approved \| Deprecated> |
| Doc version   | v.0.0.5       |                                                |
| Owner (metric)| NghiemCanCode |                                                |
| Author (spec) | NghiemCanCode |                                                |
| Reviewers     | NghiemCanCode |                                                |
| Last updated  | 2026-07-25    |                                                |
| Status        | **Đã chốt**   | Chạy xong 2026-07-24, kết quả ở §5, tham số chốt ở §6; tham số + cảnh báo đã vào spec model 2026-07-25 và vào code cùng ngày (`dbt_project.yml` vars + cột `applied_*`) |

### Changelog

| Version | Date       | Author        | Change |
| ------- | ---------- | ------------- | ------ |
| v.0.0.1 | 2026-07-24 | NghiemCanCode | Tách phương pháp luận calibrate ra khỏi `metrics_layer.md` §2.1 và ra khỏi phần comment của `scripts/gold-dbt/deploy_gold_dbt_dev.sh`. Registry giữ vai tra nhanh, script giữ vai chạy lệnh, số liệu và diễn giải về đây. |
| v.0.0.5 | 2026-07-25 | NghiemCanCode | **Đối chiếu xong với dữ liệu model.** `rpt_merchant_error_daily` chạy trên dev cùng ngày cho `portfolio_error_rate_30d = 0.016090` và **12** merchant gắn cờ — trùng phép đo 1 và phép đo 4 của §5, dù hai bên tính bằng hai đường hoàn toàn khác nhau. §1 và §6 đổi từ "chưa đối chiếu" sang kết quả đối chiếu. **Không đo lại gì, số liệu §5 giữ nguyên** — lần này chỉ là xác nhận chéo. |
| v.0.0.4 | 2026-07-25 | NghiemCanCode | Đồng bộ trạng thái sau khi `rpt_merchant_error_daily` **được implement** (verify offline, chưa chạy dev): §1 và §6 bỏ câu "chưa được build". Hai tham số của §6 nay sống thật ở `dbt/dbt_project.yml` (`abnormal_error_rate_threshold` / `abnormal_error_min_transaction_count`) và được carry ra cột `applied_*`. Cả hai cảnh báo của tài liệu này đã vào `.yml` của model: cảnh báo §4 vào description model, cảnh báo §5.1 vào description của `excess_failed_transactions_30d`. Tiêu chí vận hành 10–50 của §3 thành một singular test severity `warn` (`rpt_merchant_error_daily_flagged_count_band.sql`), hardcode chứ không thành vars — nó là tiêu chí mà tham số phải thỏa, không phải tham số. **Không đo lại gì; số liệu §5 giữ nguyên** và vẫn chưa được đối chiếu với dữ liệu model sinh ra. |
| v.0.0.3 | 2026-07-25 | NghiemCanCode | Đồng bộ trạng thái sau khi `rpt_merchant_error_daily` được spec (`docs/metrics/merchant_error_daily_report.md` v.0.0.1): Open Question #1 → **Resolved** (chốt build model riêng, business spec Decision #25); §1 và §6 bỏ chữ "hoãn". Ghi nhận cả hai cảnh báo của tài liệu này đã vào spec — cảnh báo §4 thành ràng buộc `.yml`, còn cảnh báo "xếp hạng theo tỷ lệ thô" (§5.1) được xử lý mạnh hơn khuyến nghị gốc bằng cột `excess_failed_transactions_30d`. Không đo lại gì; số liệu §5 giữ nguyên. |
| v.0.0.2 | 2026-07-24 | NghiemCanCode | Chạy xong toàn bộ phép đo và **chốt tham số: sàn 50, ngưỡng 4.0%** (§6). Ba thay đổi phương pháp phát sinh trong lúc chạy, đều do dữ liệu ép: lưới sàn mở rộng thành 50/100/200, lưới ngưỡng dịch về vùng 3–5%, và thêm **phép đo 5 (dispersion)** để kiểm xem có hiệu ứng merchant thật hay không trước khi chọn ngưỡng — câu hỏi mà 4 phép đo gốc không trả lời được. Đo 5 cho dispersion 2.45–3.45, xác nhận có tín hiệu thật. |

---
## 1. Tài liệu này quyết định cái gì

Metric **Abnormal Error Rate (merchant)** đã chốt định nghĩa (business spec Decision #22/#23, đăng ký ở `docs/metrics/metrics_layer.md` §2): *merchant có error rate trên cửa sổ trailing 30 ngày vượt một ngưỡng, chỉ xét những merchant đạt một sàn số lượng giao dịch.*

Định nghĩa đã xong từ trước, còn **hai tham số** — ngưỡng `5%` và sàn `50` — ban đầu là giá trị tạm chọn theo cảm tính, không dựa trên phép đo nào. Tài liệu này là chỗ thay chúng bằng con số có căn cứ, và ghi lại đủ để người khác — hoặc chính mình sáu tháng sau — tái lập được kết luận.

**Đã calibrate xong 2026-07-24: ngưỡng 4,0%, sàn 50** (§6). Model `rpt_merchant_error_daily` đã **spec** và đã **implement** 2026-07-25 (`docs/metrics/merchant_error_daily_report.md`), nhận đủ hai tham số cùng cả hai cảnh báo của §4/§5.1 dưới đây. **Model đã chạy trên dev 2026-07-25 và ĐỐI CHIẾU KHỚP**: mặt bằng model tính ra `0.016090` — trùng đến chữ số cuối với 1,609% ở §5, và số merchant gắn cờ ra đúng **12**. Hai con số này được đo bằng hai đường tính không chung một dòng code (§5 dùng `group by` phẳng trên 30 `date_key` gần nhất; model dùng rolling window trên spine (merchant, mcc)), nên sự trùng khớp là bằng chứng cho cả hai phía: phép đo calibrate đúng, **và** phép gộp qua `mcc` của model không sót dòng. Metric vẫn chưa publish cho BI. Ngưỡng đặt sai làm metric hoặc im lặng hoàn toàn, hoặc gắn cờ nửa danh sách merchant; cả hai đều tệ hơn là chưa có metric — đó là lý do phải đo trước khi công bố.

Liên quan: `docs/metrics/metrics_layer.md` (registry, Open Question #4) · `docs/facts/daily_transaction_trend_fact.md` (bảng nguồn).

---
## 2. Phương pháp

### 2.1. Cửa sổ đo

30 `date_key` phân biệt **lớn nhất đang thực có** trong `fact_daily_transaction_trend` — suy ra từ dữ liệu, không hard-code ngày. Nguồn hiện tại là extract synthetic tĩnh kết thúc 2019-10-31, nên trên dev cửa sổ sẽ ra `20191002`–`20191031`; phép đo 1 in lại bounds đã resolve để mọi kết quả truy ngược được về đúng cửa sổ sinh ra nó.

### 2.2. Cách gộp lên mức merchant — chỗ dễ sai nhất

Grain của bảng là `(date_key, mcc, merchant_id)`, nên **một merchant có thể nằm trên nhiều dòng trong cùng một ngày**. Phép gộp ở đây group by `merchant_id` một mình:

- **không** group thêm theo `mcc`,
- **không** lọc bỏ bucket `mcc = '-1'` — đó vẫn là giao dịch của merchant đó, chỉ là chưa resolve được ngành hàng (business spec Decision #16).

Bỏ sót một trong hai làm **mẫu số thiếu ⇒ error rate bị thổi phồng**. Đây là quy tắc aggregate #6 của registry §3, và các query calibrate này là chỗ đầu tiên trong dự án thực thi nó.

Quy tắc #2 vẫn giữ nguyên hiệu lực: chỉ các cột count/amount mới cộng được kiểu này. `unique_cards` / `unique_customers` thì không, nên chúng không xuất hiện ở bất kỳ phép đo nào dưới đây.

### 2.3. Sáu phép đo

| # | Đo cái gì | Dùng để làm gì |
| - | ---------- | --------------- |
| 1 | Mặt bằng toàn portfolio: `sum(failed) / sum(transaction_count)` trên mọi merchant, kèm bounds của cửa sổ | Mốc so sánh. Ngưỡng phải cao hơn hẳn mặt bằng, nếu không "bất thường" chỉ là "bình thường". |
| 2 | Với mỗi sàn thử **50 / 100 / 200**: giữ lại bao nhiêu % merchant, % giao dịch, % **gross value** | Hai chi phí của sàn, đánh giá khác nhau — xem §3. |
| 3 | Phân bố error rate **trong nhóm đã qua sàn** (p50/p90/p95/p99), mỗi sàn một dòng | Ngưỡng nên rơi quanh p95–p99 của nhóm này. |
| 4 | Số merchant bị gắn cờ ở các ngưỡng thử **3.0 / 3.5 / 4.0 / 5.0%**, mỗi sàn một dòng | Danh sách phải đủ ngắn để Marketing xem tay được. |
| 4b | Top merchant theo error rate, kèm `failed_count` và số lỗi kỳ vọng theo mặt bằng (chẩn đoán) | Nhìn xem đầu danh sách có bị merchant sát sàn chiếm chỗ không. **Đọc kèm cảnh báo ở §5.1** — danh sách này sắp theo tỷ lệ nên thiên lệch về merchant n nhỏ, không được dùng làm mẫu đại diện. |
| 5 | **Dispersion**: chi-square goodness of fit của `failed_count` so với kỳ vọng `n × mặt bằng`, mỗi sàn một dòng | Có hiệu ứng merchant thật hay chỉ là nhiễu lấy mẫu? `dispersion ≈ 1` nghĩa là mọi shortlist đều là dương tính giả và **không ngưỡng nào calibrate được**; `> 1.5` nghĩa là merchant khác nhau thật và việc chọn ngưỡng mới có nghĩa. |

**"Gross value" = `total_spend_amount + total_inflow_amount`** (Q&A 2026-07-24), tức tổng độ lớn dòng tiền hai chiều qua merchant. Trend fact tách amount theo dấu, nên phải cộng lại mới ra được "quy mô giá trị" mà sàn có thể làm mất.

**Phép đo 5 không nằm trong 4 phép đo gốc của `metrics_layer.md` §2.1** — nó được thêm giữa chừng lần chạy 2026-07-24, khi số liệu của đo 4 nhìn qua có vẻ khớp với nhiễu ngẫu nhiên. Bốn phép đo gốc chỉ mô tả *phân bố*; không cái nào trả lời được câu hỏi đứng trước mọi câu hỏi khác: **có gì để phát hiện không?** Calibrate ngưỡng cho một danh sách toàn dương tính giả là công vô ích, nên câu hỏi đó phải được đóng trước. Giữ lại phép đo này cho mọi lần calibrate sau.

**Vì sao đo cả sàn 100 và 200 bên cạnh 50:** xem §4.

---
## 3. Tiêu chí chấp nhận

1. **Ngưỡng ≥ 2× mặt bằng** ở phép đo 1.
2. **Danh sách gắn cờ nằm trong khoảng ~10–50 merchant** ở phép đo 4 — tiêu chí vận hành, không phải thống kê: đủ ngắn để xem tay.
3. **Sàn volume giữ lại phần lớn gross value**, dù loại nhiều merchant. Hai chi phí ở phép đo 2 được đánh giá bất đối xứng: loại **nhiều merchant** là chuyện bình thường và đúng ý đồ (đuôi dài chính là thứ nhiễu mà sàn sinh ra để dọn), nhưng loại **nhiều giá trị giao dịch** thì không chấp nhận được — nghĩa là metric đang mù trước một phần thật của business.

---
## 4. Giới hạn thống kê mà sàn volume KHÔNG xoá được

Ở n = 50 và tỷ lệ đo được 5%, khoảng tin cậy 95% rộng khoảng **±6 điểm phần trăm** — tỷ lệ thật có thể nằm đâu đó từ ~0% tới ~11%. Phải nâng sàn lên khoảng n = 200 mới thu khoảng tin cậy về ±3 điểm. Đó là lý do phép đo 2 tính chi phí của cả ba mức sàn 50/100/200, chứ không chỉ mức 50 đang tạm dùng.

Với ngưỡng đã chốt 4,0%, con số cụ thể còn khó chịu hơn: một merchant đúng 50 giao dịch chỉ cần **3 lỗi** là vượt ngưỡng, trong khi kỳ vọng theo mặt bằng là 0,8. Đó là khoảng cách hoàn toàn nằm trong tầm với của ngẫu nhiên đối với một merchant đơn lẻ — **kể cả khi ở tầm portfolio hiệu ứng merchant là có thật** (đo 5, §5). Hai điều này không mâu thuẫn: dispersion nói rằng *tập hợp* merchant khác nhau thật, nó không bảo chứng cho *từng dòng* trong shortlist.

Hệ quả phải chấp nhận và phải nói ra: **một merchant nằm sát ngưỡng là bằng chứng yếu.** Metric này là bộ lọc để lập shortlist, không phải phán quyết.

> Khi nào implement `rpt_merchant_error_daily`, câu cảnh báo trên **bắt buộc** xuất hiện trong `.yml` của model — người đọc phải thấy nó ngay tại chỗ họ query, không chỉ ở tài liệu này.
>
> **Đã nhận vào spec 2026-07-25:** `merchant_error_daily_report.md` ghi ràng buộc này ở §1 và lặp lại trong §9 như một yêu cầu bắt buộc của `.yml`. Vẫn chờ lúc model được viết thật để đóng hoàn toàn.

---
## 5. Kết quả

Chạy trên dev **2026-07-24** (lần chạy cuối 17:12–17:15; hai lần trước 16:10 và 16:44 mất cột do giới hạn render của `dbt show`, xem `scripts/gold-dbt/README.md` §16.4).

**Đo 1 — mặt bằng portfolio.** Cửa sổ resolve đúng như thiết kế: `20191002`–`20191031`, 30 ngày.

| merchants | giao dịch | lỗi | **mặt bằng** |
| --------- | --------- | --- | ------------ |
| 10.433 | 114.011 | 1.834 | **1.609%** |

**Đo 2 — chi phí của từng sàn.** Phân bố volume rất lệch: p50 = 2, p75 = 4, p90 = 9, p95 = **14** giao dịch/merchant/30 ngày.

| sàn | merchant giữ lại | % merchant | % giao dịch | % gross value |
| --- | ---------------- | ---------- | ----------- | ------------- |
| 50 | 165 | 1,58% | **70,18%** | **68,14%** |
| 100 | 118 | 1,13% | 67,35% | 62,07% |
| 200 | 78 | 0,75% | 62,35% | 53,36% |

**Đo 3 — phân bố error rate trong nhóm đã qua sàn.**

| sàn | qualified | p50 | p90 | p95 | p99 |
| --- | --------- | --- | --- | --- | --- |
| 50 | 165 | 1,538% | 3,509% | 4,598% | 7,576% |
| 100 | 118 | 1,420% | 2,941% | 3,478% | 4,167% |
| 200 | 78 | 1,338% | 2,791% | 3,062% | 3,922% |

**Đo 4 — số merchant bị gắn cờ.**

| sàn | qualified | > 3,0% | > 3,5% | > 4,0% | > 5,0% |
| --- | --------- | ------ | ------ | ------ | ------ |
| 50 | 165 | 28 | 17 | **12** | 6 |
| 100 | 118 | 10 | 5 | 2 | 0 |
| 200 | 78 | 5 | 2 | 0 | 0 |

*(Lần chạy 16:44 còn đo mức 4,5%: sàn 50 → 9, sàn 100 → 1, sàn 200 → 0. Cột này bị bỏ khỏi query để lấy chỗ cho mức 5,0% — ngưỡng tạm đang xem xét — trong hạn mức 6 cột.)*

**Đo 4b — đầu danh sách ở sàn 50.** Bị merchant sát sàn chiếm chỗ: 20 merchant đứng đầu có `txn_count` 51–313, phần lớn 51–105, và error rate của họ quy về 2–5 lỗi tuyệt đối (ví dụ merchant 4802: **3 lỗi trên 51 giao dịch** = 5,882%, trong khi kỳ vọng theo mặt bằng là 0,82 lỗi). Đọc riêng bảng này sẽ kết luận nhầm rằng sàn 50 quá thấp — xem §5.1.

**Đo 5 — dispersion.**

| sàn | merchants | chi-square | df | **dispersion** |
| --- | --------- | ---------- | -- | -------------- |
| 50 | 165 | 401,8 | 164 | **2,450** |
| 100 | 118 | 312,2 | 117 | **2,668** |
| 200 | 78 | 265,9 | 77 | **3,453** |

### 5.1. Diễn giải

**Có hiệu ứng merchant thật.** Dispersion 2,45–3,45 ở cả ba sàn, xa ngưỡng 1,5 của §2.3 — phương sai của số lỗi giữa các merchant lớn gấp 2,5–3,5 lần mức mà nhiễu lấy mẫu thuần tuý sinh ra. Việc chọn ngưỡng vì thế có nghĩa. Dispersion còn **tăng theo sàn**, nghĩa là khác biệt đậm hơn ở nhóm volume cao — đúng dấu hiệu của hiệu ứng thật chứ không phải hiện tượng số nhỏ.

**Cảnh báo về đo 4b, ghi lại vì suýt dẫn đến kết luận sai.** Trong lần chạy 2026-07-24, bảng 4b (toàn merchant n = 51–105) đã được dùng để nhẩm tỷ lệ dương tính giả, ra kết quả "số merchant gắn cờ thấp hơn cả mức ngẫu nhiên ⇒ không có tín hiệu". Kết luận đó **sai**, và đo 5 bác bỏ nó. Sai vì 4b sắp xếp theo error rate, mà n nhỏ mới sinh được tỷ lệ cực đoan, nên bảng này **thiên lệch có hệ thống về phía n nhỏ**. Con số thật của nhóm qualified: 165 merchant nắm 70,18% của 114.011 giao dịch ⇒ **n trung bình ≈ 485**, chứ không phải ~64 như bảng gợi ý. Ở n = 485, ngưỡng 4% nghĩa là ≥ 20 lỗi trong khi kỳ vọng 7,8 — xác suất ngẫu nhiên cỡ phần nghìn, không phải vài phần trăm.

> **Quy tắc rút ra:** 4b là công cụ chẩn đoán *định tính* (đầu danh sách trông có hợp lý không), tuyệt đối không dùng làm mẫu để ước lượng bất cứ đại lượng nào của nhóm qualified. Đại lượng nào cần thì đo trực tiếp.

**Đối chiếu ba tiêu chí §3** (mặt bằng 1,609% ⇒ "≥ 2× mặt bằng" = ≥ 3,218%):

| sàn + ngưỡng | gắn cờ | 1. ≥ 2× mặt bằng | 2. 10–50 merchant | 3. giữ gross value |
| ------------ | ------ | ---------------- | ----------------- | ------------------ |
| 50 + 3,0% | 28 | ✗ 1,86× | ✓ | ✓ 68% |
| 50 + 3,5% | 17 | ✓ 2,18× | ✓ | ✓ 68% |
| **50 + 4,0%** | **12** | **✓ 2,49×** | **✓** | **✓ 68%** |
| 50 + 5,0% | 6 | ✓ 3,11× | ✗ quá ngắn | ✓ 68% |
| 100 + 3,0% | 10 | ✗ 1,86× | ✓ | ✓ 62% |
| 100 + 3,5% | 5 | ✓ | ✗ quá ngắn | ✓ 62% |
| 200 + bất kỳ | ≤ 5 | ✓ | ✗ quá ngắn | ✓ 53% |

**Sàn 50 là sàn duy nhất thoả được cả ba tiêu chí cùng lúc**, và nó thoả tiêu chí 3 một cách thuyết phục: loại 98,4% merchant mà vẫn giữ 70% giao dịch và 68% gross value — đúng khuôn mẫu bất đối xứng mà §3 mô tả. Nâng lên sàn 100 hay 200 làm danh sách ngắn tới mức metric gần như im lặng (5 rồi 2 merchant), đổi lại chỉ thêm ~6 điểm gross value bị loại.

**Ngưỡng tạm 5% quá cao**: chỉ bắt 6 merchant, dưới khoảng 10–50. Ngưỡng 3,0% thì ngược lại, không đạt tiêu chí "≥ 2× mặt bằng". Vùng khả thi là 3,5–4,0%, và **4,0% được chọn** vì nó cách mặt bằng xa hơn (2,49× so với 2,18×) và nằm giữa p90–p95 của nhóm qualified, tức gần gợi ý "p95–p99" của §2.3 nhất trong số các phương án còn giữ được ≥ 10 merchant. Đặt đúng p95 (4,598%) sẽ chỉ còn ~8 merchant, tụt dưới tiêu chí 2 — đây là chỗ hai hướng dẫn của §2.3 và §3 xung khắc, và tiêu chí vận hành được ưu tiên.

**Điều 4b vẫn đúng và vẫn phải nói ra:** ở sàn 50, một merchant 51 giao dịch chỉ cần 3 lỗi là vượt ngưỡng 4%. Bằng chứng cho những merchant sát sàn là yếu, kể cả khi hiệu ứng ở tầm portfolio là thật. Xem §4 — cảnh báo này bắt buộc vào `.yml` khi implement.

---
## 6. Tham số chốt

| Tham số | Giá trị tạm | **Giá trị chốt** | Căn cứ |
| ------- | ----------- | ---------------- | ------ |
| Ngưỡng error rate | 5% | **4,0%** | 2,49× mặt bằng 1,609% (tiêu chí 1 đạt rộng rãi); gắn cờ 12 merchant (tiêu chí 2 đạt); nằm giữa p90–p95 của nhóm qualified, gần gợi ý p95–p99 nhất trong các phương án còn ≥ 10 merchant. 5% cũ chỉ bắt 6 merchant. |
| Sàn số giao dịch | 50 | **50** (giữ nguyên) | Sàn duy nhất thoả cả ba tiêu chí. Giữ 70,18% giao dịch và 68,14% gross value dù loại 98,4% merchant. Nâng lên 100/200 chỉ thêm ~6 điểm gross value bị loại mà làm danh sách tụt xuống 5 rồi 2 merchant. |

Giá trị tạm ban đầu (5% / 50) đúng một nửa: **sàn 50 hoá ra là lựa chọn đúng**, còn ngưỡng 5% thì quá cao gấp rưỡi so với mức có căn cứ. Điều này chỉ biết được sau khi đo — trước đó cả hai đều là phỏng đoán ngang nhau.

Đồng bộ sau khi chốt — **đã làm hết 2026-07-24**: `metrics_layer.md` §2 + §2.1 (giá trị chốt, bỏ chữ "tạm") và Open Question #4 → Resolved, Decision Log, v.0.0.5 · business spec Decision #24 và bảng §4 · `docs/facts/daily_transaction_trend_fact.md` Open Question #2 · `scripts/gold-dbt/README.md` §15 và lịch sử chạy §17.

**Hai tham số nay sống trong code** — `rpt_merchant_error_daily` đã spec **và implement** 2026-07-25 (`docs/metrics/merchant_error_daily_report.md`, Open Question #1 dưới đây đã đóng). Chúng là dbt vars `abnormal_error_rate_threshold` / `abnormal_error_min_transaction_count` khai báo ở `dbt/dbt_project.yml`, đồng thời carry ra cột `applied_*` trên mỗi dòng — nên một lần re-calibrate sau này sẽ không âm thầm viết lại ý nghĩa của dữ liệu cũ. Muốn tham số mới áp cho toàn lịch sử thì phải `--full-refresh`; đó là chủ ý.

**Đã đối chiếu với dữ liệu model 2026-07-25** (`scripts/gold-dbt/README.md` §18): `portfolio_error_rate_30d = 0.016090` trùng mặt bằng 1,609% của phép đo 1, và 12 merchant gắn cờ trùng phép đo 4 — hai tham số chốt ở §6 do đó đứng vững trên dữ liệu mà model thật sự sinh ra, không chỉ trên truy vấn ad-hoc. **Vẫn chưa publish cho BI**: còn thiếu nhịp T+1 thật (nguồn synthetic dừng ở `20191031`) và một vòng review với người tiêu thụ.

---
## 7. Cách tái lập

`scripts/gold-dbt/deploy_gold_dbt_dev.sh` **STEP 9**. Bỏ comment khối đó rồi chạy script — STEP 0 tự dựng EMR Serverless session, trap cleanup tự tắt. SQL của cả 6 phép đo nằm trong `scripts/gold-dbt/sql/calibrate_abnormal_error_rate_*.sql` (`scripts/gold-dbt/README.md` §1.1, §16); script chỉ `cat` file vào `dbt show --inline`.

Toàn bộ là `dbt show`: **read-only**, không build model, không ghi partition nào. Vì thế không cần `--vars`, và bẫy `2036-01-01` (dim_dates kết thúc 2035-12-31, ghi 0 dòng trong im lặng) không áp dụng ở đây.

Các sàn ứng viên nằm ở **một chỗ duy nhất** — CTE `floors` (`values (50), (100), (200)`), giống hệt nhau ở phép đo 2, 3, 4 và 5. Sửa danh sách đó thì cả bốn đi theo, mỗi sàn một dòng kết quả. Riêng phép đo 4b cố định ở sàn 50 vì nhiệm vụ của nó là soi xem sàn đang dùng nhận vào những ai.

**Hai cái bẫy đã mất hai session vì chúng, đọc trước khi sửa query** (chi tiết ở `scripts/gold-dbt/README.md` §16.3–16.4):

1. File SQL **không được tự mang `limit`** ở câu ngoài cùng — `dbt show --limit N` nối chuỗi `limit N` vào cuối file chứ không bọc subquery, thành `limit 20 limit 25` và lỗi parse.
2. `dbt show` chỉ in được **6 cột đầu** và cắt tên cột dài quá **20 ký tự**, số bị mất luôn không moi lại được từ log lẫn `run_results.json`. `--printer-width` **không** sửa được (đã thử, log xác nhận flag vào đúng mà vẫn mất cột) — nguyên nhân là `show.py` gọi `print_table()` không truyền `max_columns` nên agate áp mặc định của nó. Vì thế cả 6 file đều được giữ ở ≤ 6 cột, và mỗi file ghi rõ cột nào đã bị bỏ để nhường chỗ.

---
## 8. Open Questions & Decision Log
### Open Questions

| # | Question | Blocking? | Owner | Status |
| - | -------- | --------- | ----- | ------ |
| 1 | ~~Sau khi chốt tham số, có build `rpt_merchant_error_daily` luôn không, hay để BI query thẳng trend fact?~~ — **đã chốt 2026-07-25: build model riêng**. Spec `docs/metrics/merchant_error_daily_report.md`; lý do đầy đủ ở business spec Decision #25 (công thức có ba cách sai im lặng, để BI tự viết là đặt cược vào việc ai cũng nhớ cả ba). Còn lại chỉ là việc implement. | No | NghiemCanCode | **Resolved** |
| 2 | Cửa sổ 30 ngày hiện lấy trên dữ liệu synthetic tĩnh kết thúc 2019-10-31. Khi nguồn có biến động thật, có cần calibrate lại không, và theo chu kỳ nào? | No | NghiemCanCode | Open |

### Decision Log

| Date | Decision | Rationale | Decided by |
| ---- | -------- | --------- | ---------- |
| 2026-07-24 | Tách phương pháp luận + kết quả calibrate ra file riêng, thay vì để trong `metrics_layer.md` §2.1 hoặc trong comment của deploy script | Registry tự đặt cho mình vai "tra nhanh" (§1 của nó), nên một run-log đầy đủ sẽ làm nó phình ra sai mục đích. Còn script thì mỗi khối comment chỉ nên trả lời "sao lệnh này lại nằm đây" — nhét cả số liệu vào làm file dài ra mà phần code chẳng thêm bao nhiêu. File nào việc đó. | NghiemCanCode |
| 2026-07-24 | "Giá trị giao dịch" ở tiêu chí sàn = gross = spend + inflow | Trend fact tách amount theo dấu; chỉ lấy spend sẽ bỏ qua merchant có dòng tiền vào lớn, làm chi phí của sàn trông nhẹ hơn thực tế. | NghiemCanCode |
| 2026-07-24 | Đo chi phí của cả sàn 200 bên cạnh sàn 50 | 50 là con số tạm chưa có căn cứ, mà §4 cho thấy ở n=50 khoảng tin cậy rộng tới ±6 điểm. Đo sẵn 200 để lúc chốt có hai lựa chọn kèm giá của từng cái, thay vì chỉ xác nhận lại phỏng đoán ban đầu. | NghiemCanCode |
| 2026-07-24 | Thêm **phép đo 5 (dispersion)** vào bộ đo chuẩn, ngoài 4 phép đo của `metrics_layer.md` §2.1 | Bốn phép đo gốc chỉ mô tả phân bố, không cái nào trả lời "có hiệu ứng merchant thật để phát hiện không". Thiếu câu trả lời đó thì việc chọn ngưỡng có thể chỉ là chọn độ dài của một danh sách dương tính giả — và trong chính lần chạy này, một ước lượng vội đã suýt kết luận nhầm theo hướng đó. Chi-square goodness of fit đóng câu hỏi bằng một query rẻ. | NghiemCanCode |
| 2026-07-24 | Giữ **sàn 50**, đổi ngưỡng **5% → 4,0%** | Sàn 50 là sàn duy nhất thoả cả ba tiêu chí §3, và giữ tới 70% giao dịch / 68% gross value dù loại 98,4% merchant. Ngưỡng 5% chỉ bắt 6 merchant, dưới khoảng 10–50; 3,0% thì không đạt "≥ 2× mặt bằng". Trong vùng khả thi 3,5–4,0%, chọn 4,0% vì cách mặt bằng xa hơn (2,49×) và gần gợi ý p95 hơn mà vẫn giữ được 12 merchant. | NghiemCanCode |
| 2026-07-24 | Ghi lại cảnh báo "không dùng phép đo 4b làm mẫu thống kê" ngay trong §5.1 thay vì chỉ sửa kết luận | Sai lầm này đã xảy ra thật trong lúc calibrate: 4b sắp theo error rate nên thiên lệch về merchant n nhỏ, dùng nó để nhẩm tỷ lệ dương tính giả cho ra kết luận ngược hẳn với đo 5. Một cái bẫy đã sập một lần thì sẽ sập lại ở lần calibrate sau nếu chỉ lặng lẽ sửa số. | NghiemCanCode |
