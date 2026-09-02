variable "name_prefix" {
  description = "Name prefix for network resources, e.g. finance-transaction"
  type        = string
}

variable "environment" {
  description = "Environment name used as name suffix and tag, e.g. dev / prod"
  type        = string
}

variable "region" {
  description = "AWS region, used to build VPC endpoint service names"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones, one per subnet pair. MWAA requires exactly 2"
  type        = list(string)

  validation {
    condition     = length(var.availability_zones) == 2
    error_message = "MWAA requires exactly 2 availability zones."
  }
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks of public subnets (NAT gateway lives here)"
  type        = list(string)
  default     = ["10.0.0.0/24", "10.0.1.0/24"]
}

# /20 chu khong phai /24: EKS dung VPC CNI, moi pod chiem mot IP that cua subnet.
# CIDR cua subnet khong mo rong duoc sau khi tao, nen cap du ngay tu dau.
# Khoang 10.0.2.0 - 10.0.15.255 con trong cho cac tang subnet ve sau.
variable "private_subnet_cidrs" {
  description = "CIDR blocks of private subnets (MWAA ENIs, later EKS pods, live here)"
  type        = list(string)
  default     = ["10.0.16.0/20", "10.0.32.0/20"]
}

variable "single_nat_gateway" {
  description = "true: one shared NAT gateway (cheaper, dev). false: one NAT gateway per AZ (HA, prod)"
  type        = bool
  default     = true
}

variable "enable_s3_gateway_endpoint" {
  description = "Create a free S3 gateway endpoint so data lake traffic bypasses the NAT gateway"
  type        = bool
  default     = true
}

# webserver_access_mode la khai niem cua MWAA nen no o module orchestration.
# O day chi can biet: mo 443 cho nhung CIDR nao. Rong = khong sinh rule.
variable "private_webserver_allowed_cidrs" {
  description = "CIDRs allowed to reach the Airflow web server on 443. Leave empty unless the MWAA environment runs with PRIVATE_ONLY"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Extra tags applied to every resource"
  type        = map(string)
  default     = {}
}
