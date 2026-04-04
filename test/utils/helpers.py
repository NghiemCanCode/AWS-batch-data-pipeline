from pyspark.sql.types import StructType, StructField, StringType


def make_df(spark, values: list, col_name: str = "value"):
    """Create a single-column DataFrame from a list of values (all StringType)."""
    return spark.createDataFrame(
        [(v,) for v in values],
        schema=StructType([StructField(col_name, StringType(), nullable=True)]),
    )


def collect_col(df, col_name: str = "result") -> list:
    """Collect all values of a single column into a Python list."""
    return [row[col_name] for row in df.collect()]
