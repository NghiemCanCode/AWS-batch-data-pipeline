module "network" {
  source = "../../../modules/network"

  name_prefix                     = var.name_prefix
  environment                     = "dev"
  region                          = var.region
  vpc_cidr                        = var.vpc_cidr
  availability_zones              = var.availability_zones
  single_nat_gateway              = var.single_nat_gateway
  private_webserver_allowed_cidrs = var.private_webserver_allowed_cidrs
  tags                            = { stack = "orchestration" }
}

module "mwaa" {
  source = "../../../modules/orchestration"

  environment_name   = local.mwaa_name
  airflow_version    = var.airflow_version
  environment_class  = var.mwaa_environment_class
  execution_role_arn = aws_iam_role.mwaa_execution_role.arn

  source_bucket_arn = "arn:aws:s3:::${var.code_bucket_name}"
  dag_s3_path       = "dags/"

  requirements_s3_path           = var.requirements_s3_object_version == null ? null : "requirements.txt"
  requirements_s3_object_version = var.requirements_s3_object_version

  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [module.network.mwaa_security_group_id]

  webserver_access_mode = var.webserver_access_mode
  min_workers           = var.mwaa_min_workers
  max_workers           = var.mwaa_max_workers

  enable_secrets_manager_backend = var.enable_secrets_manager_backend
  secrets_prefix                 = var.secrets_prefix
  connections_lookup_pattern     = var.connections_lookup_pattern
  variables_lookup_pattern       = var.variables_lookup_pattern

  tags = { environment = "dev" }
}
