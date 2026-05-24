import sys
import os

from pyspark.sql import functions as F

from test.utils.helpers import make_df, collect_col

from src.aws_pipeline.aggregation.dim_processing.date_dim import (
    generate_date_dim,
    _calculate_week_of_month_col,
    _calculate_is_weekend_col,
    _calculate_holiday_df,
)
from src.aws_pipeline.schemas.gold_schema import DateDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestCalculateWeekOfMonthCol:
    """_calculate_week_of_month_col: date to week bucket based on day-of-month / 7."""

    def test_boundary_values(self, spark):
        df = make_df(spark, ["2024-01-01", "2024-01-31"]).withColumn(
            "value", F.to_date(F.col("value"))
        )
        result = collect_col(
            df.withColumn("result", _calculate_week_of_month_col("value"))
        )
        assert result == [1, 5]

    def test_rollover_points(self, spark):
        df = make_df(
            spark,
            [
                "2024-01-07",
                "2024-01-08",
                "2024-01-14",
                "2024-01-15",
                "2024-01-21",
                "2024-01-22",
                "2024-01-28",
                "2024-01-29",
            ],
        ).withColumn("value", F.to_date(F.col("value")))
        result = collect_col(
            df.withColumn("result", _calculate_week_of_month_col("value"))
        )
        assert result == [1, 2, 2, 3, 3, 4, 4, 5]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, ["2024-02-16", "2024-02-23"]).withColumn(
            "value", F.to_date(F.col("value"))
        )
        result = collect_col(
            df.withColumn("result", _calculate_week_of_month_col("value"))
        )
        assert result == [3, 4]


class TestCalculateIsWeekendCol:
    """_calculate_is_weekend_col: true for Spark dayofweek Sunday=1 or Saturday=7."""

    def test_boundary_values(self, spark):
        df = make_df(spark, ["2024-01-01", "2024-12-31"]).withColumn(
            "value", F.to_date(F.col("value"))
        )
        result = collect_col(
            df.withColumn("result", _calculate_is_weekend_col("value"))
        )
        assert result == [False, False]

    def test_rollover_points(self, spark):
        df = make_df(
            spark, ["2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"]
        ).withColumn("value", F.to_date(F.col("value")))
        result = collect_col(
            df.withColumn("result", _calculate_is_weekend_col("value"))
        )
        assert result == [False, True, True, False]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, ["2024-07-03", "2024-07-07"]).withColumn(
            "value", F.to_date(F.col("value"))
        )
        result = collect_col(
            df.withColumn("result", _calculate_is_weekend_col("value"))
        )
        assert result == [False, True]


class TestCalculateHolidayDf:
    """_calculate_holiday_df: generates US holidays for an inclusive year range."""

    def test_boundary_values(self, spark):
        result = (
            _calculate_holiday_df(spark, 2024, 2024)
            .filter(F.col("full_date").isin("2024-01-01", "2024-12-25"))
            .orderBy("full_date")
            .select("full_date", "holiday_name")
            .collect()
        )
        assert [(row["full_date"].isoformat(), row["holiday_name"]) for row in result] == [
            ("2024-01-01", "New Year's Day"),
            ("2024-12-25", "Christmas Day"),
        ]

    def test_rollover_points(self, spark):
        result = (
            _calculate_holiday_df(spark, 2023, 2024)
            .filter(F.col("full_date").isin("2023-12-25", "2024-01-01"))
            .orderBy("full_date")
            .select("full_date", "holiday_name")
            .collect()
        )
        assert [(row["full_date"].isoformat(), row["holiday_name"]) for row in result] == [
            ("2023-12-25", "Christmas Day"),
            ("2024-01-01", "New Year's Day"),
        ]

    def test_representative_middle_values(self, spark):
        result = (
            _calculate_holiday_df(spark, 2024, 2024)
            .filter(F.col("full_date") == "2024-07-04")
            .select("full_date", "holiday_name")
            .collect()
        )
        assert [(row["full_date"].isoformat(), row["holiday_name"]) for row in result] == [
            ("2024-07-04", "Independence Day")
        ]


class TestGenerateDateDim:
    def test_generate_date_dim(self, spark):
        result_df = generate_date_dim(
            "2024-01-01", "2024-01-03", spark, "2024-01-04 00:00:00"
        )

        assert result_df.schema == DateDimensionSchema

        result = (
            result_df.orderBy("full_date")
            .select(
                "date_key",
                "full_date",
                "day_of_week",
                "day_of_month",
                "day_of_year",
                "week_of_month",
                "week_of_year",
                "month",
                "quarter",
                "year",
                "is_weekend",
                "is_holiday",
                "holiday_name",
            )
            .collect()
        )

        assert [
            (
                row["date_key"],
                row["full_date"].isoformat(),
                row["day_of_week"],
                row["day_of_month"],
                row["day_of_year"],
                row["week_of_month"],
                row["week_of_year"],
                row["month"],
                row["quarter"],
                row["year"],
                row["is_weekend"],
                row["is_holiday"],
                row["holiday_name"],
            )
            for row in result
        ] == [
            (
                20240101,
                "2024-01-01",
                2,
                1,
                1,
                1,
                1,
                1,
                1,
                2024,
                False,
                True,
                "New Year's Day",
            ),
            (
                20240102,
                "2024-01-02",
                3,
                2,
                2,
                1,
                1,
                1,
                1,
                2024,
                False,
                False,
                None,
            ),
            (
                20240103,
                "2024-01-03",
                4,
                3,
                3,
                1,
                1,
                1,
                1,
                2024,
                False,
                False,
                None,
            ),
        ]
