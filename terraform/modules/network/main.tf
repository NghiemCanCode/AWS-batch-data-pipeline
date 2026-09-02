locals {
  name      = "${var.name_prefix}-${var.environment}"
  nat_count = var.single_nat_gateway ? 1 : length(var.public_subnet_cidrs)
  tags      = merge({ environment = var.environment }, var.tags)
}

# MWAA bat buoc enable_dns_support + enable_dns_hostnames
resource "aws_vpc" "vpc" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.tags, { Name = "${local.name}-vpc" })
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.vpc.id

  tags = merge(local.tags, { Name = "${local.name}-igw" })
}

resource "aws_subnet" "public" {
  count = length(var.public_subnet_cidrs)

  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.tags, {
    Name = "${local.name}-public-${var.availability_zones[count.index]}"
    tier = "public"
  })
}

resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id            = aws_vpc.vpc.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  tags = merge(local.tags, {
    Name = "${local.name}-private-${var.availability_zones[count.index]}"
    tier = "private"
  })
}

resource "aws_eip" "nat" {
  count = local.nat_count

  domain = "vpc"

  tags = merge(local.tags, { Name = "${local.name}-nat-eip-${count.index}" })
}

resource "aws_nat_gateway" "nat" {
  count = local.nat_count

  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(local.tags, { Name = "${local.name}-nat-${count.index}" })

  depends_on = [aws_internet_gateway.igw]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.vpc.id

  tags = merge(local.tags, { Name = "${local.name}-rt-public" })
}

resource "aws_route" "public-internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# single_nat_gateway = true -> 1 route table dung chung cho ca 2 private subnet
resource "aws_route_table" "private" {
  count = local.nat_count

  vpc_id = aws_vpc.vpc.id

  tags = merge(local.tags, { Name = "${local.name}-rt-private-${count.index}" })
}

resource "aws_route" "private-nat" {
  count = local.nat_count

  route_table_id         = aws_route_table.private[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.nat[count.index].id
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[var.single_nat_gateway ? 0 : count.index].id
}

# Gateway endpoint mien phi: traffic S3 (data lake, dags, logs) khong di qua NAT
resource "aws_vpc_endpoint" "s3" {
  count = var.enable_s3_gateway_endpoint ? 1 : 0

  vpc_id            = aws_vpc.vpc.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = concat(aws_route_table.private[*].id, [aws_route_table.public.id])

  tags = merge(local.tags, { Name = "${local.name}-vpce-s3" })
}

resource "aws_security_group" "mwaa" {
  name        = "${local.name}-mwaa-sg"
  description = "Security group for MWAA environment ENIs"
  vpc_id      = aws_vpc.vpc.id

  tags = merge(local.tags, { Name = "${local.name}-mwaa-sg" })
}

# MWAA bat buoc co rule self-referencing cho toan bo traffic noi bo
resource "aws_vpc_security_group_ingress_rule" "mwaa-self" {
  security_group_id            = aws_security_group.mwaa.id
  referenced_security_group_id = aws_security_group.mwaa.id
  ip_protocol                  = "-1"
  description                  = "Self-referencing rule required by MWAA"
}

# Chi can khi MWAA chay PRIVATE_ONLY: vao UI tu VPN/bastion
resource "aws_vpc_security_group_ingress_rule" "mwaa-webserver" {
  for_each = toset(var.private_webserver_allowed_cidrs)

  security_group_id = aws_security_group.mwaa.id
  cidr_ipv4         = each.value
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
  description       = "HTTPS to private Airflow web server"
}

resource "aws_vpc_security_group_egress_rule" "mwaa-all" {
  security_group_id = aws_security_group.mwaa.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Allow all outbound"
}
