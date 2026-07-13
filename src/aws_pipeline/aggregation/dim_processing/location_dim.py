from functools import reduce

from pyspark.sql import DataFrame, functions as F


LOCATION_COLUMN_PAIRS = (
    ("city", "state"),
    ("merchant_city", "merchant_state"),
)


def _normalized_string_col(column_name: str) -> F.col:
    trimmed_col = F.trim(F.col(column_name).cast("string"))
    return F.when(trimmed_col == F.lit(""), F.lit(None)).otherwise(trimmed_col)


def _nullable_string_col(df: DataFrame, column_name: str) -> F.col:
    if column_name in df.columns:
        return _normalized_string_col(column_name)
    return F.lit(None).cast("string")


def _select_location_pair(df: DataFrame, city_col: str, state_col: str) -> DataFrame | None:
    if city_col not in df.columns and state_col not in df.columns:
        return None

    return df.select(
        _nullable_string_col(df, city_col).alias("city"),
        _nullable_string_col(df, state_col).alias("state"),
    ).where(F.col("city").isNotNull() | F.col("state").isNotNull())


def _location_key_col(city_col: str, state_col: str) -> F.col:
    return F.md5(
        F.concat_ws(
            "||",
            F.coalesce(F.col(city_col), F.lit("")),
            F.coalesce(F.col(state_col), F.lit("")),
        )
    )


def process_location_dim(batch_logical_date, *dfs: DataFrame) -> DataFrame:
    """
    Business rule: Gold uses one conformed city/state dimension for customer and
    merchant locations. It is Type 1 CDC by location_key because source location
    parsing can be corrected without preserving a row-version history.
    """
    if not dfs:
        raise ValueError("At least one source DataFrame is required to build Location dimension.")

    location_dfs = []
    for df in dfs:
        for city_col, state_col in LOCATION_COLUMN_PAIRS:
            location_df = _select_location_pair(df, city_col, state_col)
            if location_df is not None:
                location_dfs.append(location_df)

    if not location_dfs:
        raise ValueError(
            "No supported location columns found. Expected city/state or "
            "merchant_city/merchant_state columns."
        )

    location_dim_df = reduce(
        lambda left, right: left.unionByName(right),
        location_dfs,
    )

    location_dim_df = (
        location_dim_df.withColumn("location_key", _location_key_col("city", "state"))
        .select("location_key", "city", "state")
        .distinct()
    )

    return location_dim_df
