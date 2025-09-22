terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = "data_lake_bucket"
}