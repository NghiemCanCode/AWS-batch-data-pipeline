output "ecr_repository_url" {
  value = module.ecr.ecr_repository_url
}

output "data_lake_bucket_name" {
  value = module.data_lake.bucket_name
}

output "code_bucket_name" {
  value = module.code_bucket.bucket_name
}
