import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../src"))
)

from aws_pipeline.aggregation.dim_processing.time_dim import generate_time_dim


def test_generate_time_dim(spark):
    df = generate_time_dim(spark, "2026-04-04")
    df.show(10)
