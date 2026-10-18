terraform {
  required_version = ">= 1.10"
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.40.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.name_prefix, Stack = "ingestion", ManagedBy = "terraform" }
  }
}

data "aws_caller_identity" "me" {}
data "aws_region" "this" {}
