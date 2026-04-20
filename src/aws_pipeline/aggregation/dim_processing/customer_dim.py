from pyspark.sql import DataFrame

def _compute_address_key(column_name: str):
    city = 

def process_customer_dim(df) -> DataFrame:
     # Pure transformation
     df = df.withColumnRename("user_id", "customer_id") \
          .