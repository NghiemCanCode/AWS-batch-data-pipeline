# Connection: core-banking-pg → bronze-cdc-s3

Bản ghi mọi giá trị đã nhập trên UI Airbyte cho source 01 Core Banking (deploy.md, Bước 9). UI không có lịch sử
thay đổi, nên mỗi lần sửa trên UI thì sửa file này và thêm một dòng vào "Lịch sử thay đổi". **Không ghi mật khẩu.**

| | |
|---|---|
| Ngày tạo | `<yyyy-mm-dd>` |
| Airbyte chart / Postgres source / S3 destination | `2.4.0` / `<3.8.x>` / `<1.9.x>` |
| Slot / publication | `airbyte_core_banking` / `airbyte_core_banking` |
| Peering | `<pcx-...>` |

## Source: `core-banking-pg`

| Trường | Giá trị |
|---|---|
| Host | `10.101.10.10` |
| Port | `5432` |
| Database Name | `core_banking` |
| Schemas | `core` |
| Username | `cdc_airbyte` |
| Password | source team đưa, không ghi ở đây |
| SSL Mode | `verify-full`, CA certificate = parameter `/fds/core-banking/tls/ca-crt`, không client cert |
| SSH Tunnel Method | `No Tunnel` |
| Update Method | `Read Changes using Change Data Capture (CDC)` |
| Replication Slot | `airbyte_core_banking` |
| Publication | `airbyte_core_banking` |
| LSN commit behavior | `After loading Data in the destination` |
| Các trường (Advanced) | mặc định (waiting 1200 s, initial load timeout 8 h, shutdown timeout 60 s, không heartbeat) |

## Destination: `bronze-cdc-s3`

| Trường | Giá trị |
|---|---|
| Access Key ID / Secret Access Key / Role ARN | để trống (instance profile) |
| S3 Bucket Name | `finance-transaction-datalake-dev` |
| S3 Bucket Path | `bronze/cdc` |
| S3 Bucket Region | `ap-southeast-1` |
| Output Format | `JSON Lines: Newline-delimited JSON`, Flattening `No flattening`, Compression `GZIP` |
| S3 Path Format | `${NAMESPACE}/${STREAM_NAME}/year=${YEAR}/month=${MONTH}/day=${DAY}/` |

## Connection

| Thiết lập | Giá trị |
|---|---|
| Streams | `customers`, `cards`, `mcc_codes`, `transactions`, `txn_fraud_labels`, `card_balances` (bỏ `transactions_p*`, `transactions_default`) |
| Sync mode | Append Historical Changes + Append New Rows and Updates Only (= Incremental Append) |
| Destination Namespace | `Custom`: `core_banking` |
| Stream prefix | trống |
| Schedule | `Scheduled`, `Every 1 hour` |
| Detect and propagate schema changes | `Propagate field changes only` |
| Backfill new or renamed columns | tắt |
| Thông báo schema change | Settings → Notifications (Bước 11.2) |

## Lịch sử thay đổi

| Ngày | Đổi gì | Vì sao |
|---|---|---|
| | | |
