variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "az" {
  type    = string
  default = "ap-southeast-1a" # same AZ as the sources: peering traffic inside one AZ is free
}

variable "name_prefix" { type = string }           # finance-transaction
variable "alert_email" { type = string }
variable "data_lake_bucket_name" { type = string } # platform stack output

variable "instance_type" {
  type    = string
  default = "t3.xlarge" # 4 vCPU, 16 GB: abctl asks for at least 4 CPU / 8 GB
}

# Log group name created by the account stack of the sources repo. Plain string; that state is never read.
variable "session_log_group" {
  type    = string
  default = "/fds/ssm-sessions"
}

# One entry per source. Values come from the source team: its CIDR and the peering ID its apply created.
# Empty map = Airbyte runs with no route to any source.
variable "cdc_sources" {
  type = map(object({
    peering_id = string
    cidr       = string
  }))
  default = {}
}

locals {
  name             = "${var.name_prefix}-airbyte"
  param_prefix     = "/${var.name_prefix}/airbyte"
  metric_namespace = "FDS/Airbyte"
  account_id       = data.aws_caller_identity.me.account_id
  region           = data.aws_region.this.region

  # One public subnet, no NAT: the host has its own public IP and an SG with no ingress rule
  vpc_cidr           = "10.200.0.0/16"
  public_subnet_cidr = "10.200.0.0/24"
  airbyte_private_ip = "10.200.0.10"

  root_volume_gb = 40 # Docker images: platform + one image per connector version
  data_volume_gb = 20 # /root/.airbyte: internal database (logs and state go to the storage bucket)

  cdc_prefix      = "bronze/cdc"
  auto_stop_after = "12h"
}
