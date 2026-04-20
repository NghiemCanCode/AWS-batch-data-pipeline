import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../src"))
)

from aws_pipeline.aggregation.dim_processing.date_dim import generate_date_dim


def test_generate_date_dim(spark):

    START_DATE = "2026-01-01"
    END_DATE = "2026-01-31"

    df = generate_date_dim(START_DATE, END_DATE, spark, "2026-02-04")
    df.show(10)
