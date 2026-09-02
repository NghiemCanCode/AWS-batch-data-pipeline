output "vpc_id" {
  description = "VPC id"
  value       = aws_vpc.vpc.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.vpc.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnet ids"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet ids, feed these into aws_mwaa_environment.network_configuration"
  value       = aws_subnet.private[*].id
}

output "mwaa_security_group_id" {
  description = "Security group id for the MWAA environment"
  value       = aws_security_group.mwaa.id
}

output "public_route_table_id" {
  description = "Public route table id"
  value       = aws_route_table.public.id
}

output "private_route_table_ids" {
  description = "Private route table ids"
  value       = aws_route_table.private[*].id
}

output "nat_gateway_public_ips" {
  description = "Public IPs of the NAT gateways, useful for third party IP allowlists"
  value       = aws_eip.nat[*].public_ip
}
