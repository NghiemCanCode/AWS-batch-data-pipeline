output "instance_id" { value = aws_instance.airbyte.id }
output "private_ip" { value = aws_instance.airbyte.private_ip }
output "vpc_id" { value = aws_vpc.this.id }
output "vpc_cidr" { value = aws_vpc.this.cidr_block }
output "account_id" { value = local.account_id }
output "ops_bucket" { value = aws_s3_bucket.ops.bucket }
output "storage_bucket" { value = aws_s3_bucket.storage.bucket }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
output "admin_password_param" { value = aws_ssm_parameter.admin_password.name }
output "cdc_s3_uri" { value = "s3://${var.data_lake_bucket_name}/${local.cdc_prefix}" }
output "peering_ids" { value = { for k, v in aws_vpc_peering_connection_accepter.cdc : k => v.id } }

# NVMe SERIAL = volume ID without the dash (used in step 5.2)
output "data_volume_serial" { value = replace(aws_ebs_volume.data.id, "-", "") }
