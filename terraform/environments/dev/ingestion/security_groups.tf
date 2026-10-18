resource "aws_security_group" "airbyte" {
  name        = "${local.name}-host"
  description = "Airbyte host ${local.name}"
  vpc_id      = aws_vpc.this.id

  lifecycle { create_before_destroy = true }
}

# No ingress rule at all. The host has a public IP, so this SG is the only thing keeping the UI (port 8000) off
# the internet. The UI is reached through SSM port forwarding, an outbound connection from the SSM agent.

# Egress 80/443: apt, Docker Hub, the abctl Helm chart, SSM, CloudWatch
resource "aws_vpc_security_group_egress_rule" "airbyte_web" {
  for_each          = toset(["80", "443"])
  security_group_id = aws_security_group.airbyte.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = tonumber(each.key)
  to_port           = tonumber(each.key)
}

# Egress 5432 only to the CIDR of each source that has a peering
resource "aws_vpc_security_group_egress_rule" "airbyte_to_source" {
  for_each          = var.cdc_sources
  security_group_id = aws_security_group.airbyte.id
  cidr_ipv4         = each.value.cidr
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  description       = "CDC to ${each.key}"
}
