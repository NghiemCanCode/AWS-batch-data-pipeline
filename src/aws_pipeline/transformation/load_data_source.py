def read_bronze_csv(
    spark: "SparkSession",
    input_base_path: str,
    relative_path: str,
    *,
    header: bool = True,
    infer_schema: bool = False,
    delimiter: str = ",",
) -> "DataFrame":

    if not input_base_path or not relative_path:
        raise ValueError("Both 'input_base_path' and 'relative_path' must be non-empty strings.")

    # Use forward-slash joining — compatible with local FS, S3, and HDFS.
    input_path = f"{input_base_path.rstrip('/')}/{relative_path.lstrip('/')}"

    logger.info("Reading bronze CSV from %s", input_path)

    df = (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("delimiter", delimiter)
        .csv(input_path)
    )

    logger.info("Successfully read %d columns from %s", len(df.columns), input_path)
    return df

def read_bronze_json(
    spark: "SparkSession",
    input_base_path: str,
    relative_path: str,
    **kwargs):

    if not input_base_path or not relative_path:
        raise ValueError("Both 'input_base_path' and 'relative_path' must be non-empty strings.")

    input_path = f"{input_base_path.rstrip('/')}/{relative_path.lstrip('/')}"

    logger.info("Reading bronze JSON from %s", input_path)

    if "multiLine" in kwargs:
        return spark.read.option("multiLine", "true").json(input_path)
    else:
        return spark.read.json(input_path)
