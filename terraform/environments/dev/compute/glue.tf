resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Gold Iceberg publish database for curated dimensional and fact tables."
}

resource "aws_glue_catalog_database" "gold_staging" {
  name        = "gold_staging"
  description = "Gold Iceberg staging database used by AWAP before publish."
}
