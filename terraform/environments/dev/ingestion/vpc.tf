resource "aws_vpc" "this" {
  cidr_block           = local.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = local.name }
}

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.public_subnet_cidr
  availability_zone = var.az
  tags              = { Name = "${local.name}-public" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

# No inline routes: routes are added with aws_route (Internet Gateway, one per source peering)
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-public" }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# S3 goes through the gateway endpoint: free, and keeps CDC data and Airbyte storage off the public internet path
resource "aws_vpc_endpoint" "s3" {
  vpc_id          = aws_vpc.this.id
  service_name    = "com.amazonaws.${var.region}.s3"
  route_table_ids = [aws_route_table.public.id]
}
