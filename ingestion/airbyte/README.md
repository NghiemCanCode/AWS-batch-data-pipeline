# Airbyte (CDC ingestion)

Airbyte is the CDC consumer of the platform. It reads the WAL of each source database over VPC peering and
writes every change to `s3://<data-lake>/bronze/cdc/<namespace>/<stream>/`.

- Runs on one EC2 host in its own VPC (`10.200.0.0/16`, public subnet, SG with no ingress), installed with `abctl`.
- Infrastructure: [terraform/environments/dev/ingestion/](../../terraform/environments/dev/ingestion/)
- Deployment and operations, step by step: check my blog (in future) *(Vietnamese)*
- What is configured in the UI, per source: [connections/](connections/)

| Folder | Runs on | Purpose |
|---|---|---|
| `lib/` | Laptop | Shared shell helpers (`tf_out`, `run_remote`, ...) |
| `setup/` | Laptop | One-off steps after `terraform apply` |
| `host/` | Airbyte host | Bootstrap scripts, config files, cron jobs |
| `ops/` | Laptop | Push files, open the UI tunnel, start/stop for a demo |
| `tests/` | Airbyte host | Acceptance checks (path to a source) |
