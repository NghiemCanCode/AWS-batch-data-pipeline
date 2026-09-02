output "mwaa_webserver_url" {
  description = "Airflow web UI URL."
  value       = module.mwaa.mwaa_webserver_url
}

output "mwaa_environment_name" {
  description = "MWAA environment name."
  value       = module.mwaa.mwaa_environment_name
}

output "mwaa_execution_role_arn" {
  description = "IAM role the MWAA environment runs under."
  value       = aws_iam_role.mwaa_execution_role.arn
}

output "vpc_id" {
  description = "VPC hosting the MWAA environment."
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnets hosting the MWAA ENIs. Reusable by a future EKS cluster."
  value       = module.network.private_subnet_ids
}

output "nat_gateway_public_ips" {
  description = "NAT gateway public IPs, for third party IP allowlists."
  value       = module.network.nat_gateway_public_ips
}
