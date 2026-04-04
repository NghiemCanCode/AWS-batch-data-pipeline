import sys
import os

from test.utils.helpers import make_df, collect_col

from src.aws_pipeline.aggregation.dim_processing.time_dim import (
    generate_time_dim,
    _calculate_time_key_col,
    _calculate_time_24h_col,
    _calculate_hour_24_col,
    _calculate_hour_12_col,
    _calculate_am_pm_col,
    _calculate_minute_col,
    _calculate_second_col,
    _calculate_time_bucket_col,
    _display_bucket_time_col,
    _calculate_day_part_col,
)

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestCalculateTimeKeyCol:
    def test_boundary_values(self, spark):
        df = make_df(spark, [0, 86399])
        result = collect_col(df.withColumn("result", _calculate_time_key_col("value")))
        assert result == [0, 235959]

    def test_rollover_points(self, spark):
        df = make_df(spark, [59, 60, 3599, 3600])
        result = collect_col(df.withColumn("result", _calculate_time_key_col("value")))
        assert result == [59, 100, 5959, 10000]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, [3661])
        result = collect_col(df.withColumn("result", _calculate_time_key_col("value")))
        assert result == [10101]


class TestCalculateTime24hCol:
    """_calculate_time_24h_col: time_key (HHMMSS int) → 'HH:MM:SS' string with zero-padding."""

    def test_boundary_values(self, spark):
        # 0 → "00:00:00", 235959 → "23:59:59"
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_time_24h_col("value")))
        assert result == ["00:00:00", "23:59:59"]

    def test_rollover_points(self, spark):
        # Hour boundary: last second of hour 0 → first second of hour 1
        # Padding boundary: single-digit hour (09:xx:xx) → double-digit (10:xx:xx)
        df = make_df(spark, [5959, 10000, 90000, 100000])
        result = collect_col(df.withColumn("result", _calculate_time_24h_col("value")))
        assert result == ["00:59:59", "01:00:00", "09:00:00", "10:00:00"]

    def test_representative_middle_values(self, spark):
        # 13:15:30
        df = make_df(spark, [131530])
        result = collect_col(df.withColumn("result", _calculate_time_24h_col("value")))
        assert result == ["13:15:30"]


class TestCalculateHour24Col:
    """_calculate_hour_24_col: time_key → hour in 24h format (0–23)."""

    def test_boundary_values(self, spark):
        # Midnight (0) and last second of the day (23)
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_hour_24_col("value")))
        assert result == [0, 23]

    def test_rollover_points(self, spark):
        # Last key of hour 9 → first key of hour 10 (single→double digit transition)
        df = make_df(spark, [95959, 100000])
        result = collect_col(df.withColumn("result", _calculate_hour_24_col("value")))
        assert result == [9, 10]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, [130000])
        result = collect_col(df.withColumn("result", _calculate_hour_24_col("value")))
        assert result == [13]


class TestCalculateHour12Col:
    """_calculate_hour_12_col: time_key → hour in 12h format (1–12).
    Special case: hour24 % 12 == 0 → returns 12 (covers midnight and noon)."""

    def test_boundary_values(self, spark):
        # Midnight (hour24=0 → 0%12=0 → 12) and 23:59:59 (hour24=23 → 23%12=11)
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_hour_12_col("value")))
        assert result == [12, 11]

    def test_rollover_points(self, spark):
        # AM→PM transition at noon: 11:59:59 → 12:00:00 → 13:00:00
        # Noon (hour24=12 → 12%12=0 → 12), 1pm (hour24=13 → 13%12=1 → 1)
        df = make_df(spark, [115959, 120000, 125959, 130000])
        result = collect_col(df.withColumn("result", _calculate_hour_12_col("value")))
        assert result == [11, 12, 12, 1]

    def test_representative_middle_values(self, spark):
        # 15:00:00 → hour24=15 → 15%12=3
        df = make_df(spark, [150000])
        result = collect_col(df.withColumn("result", _calculate_hour_12_col("value")))
        assert result == [3]


class TestCalculateAmPmCol:
    """_calculate_am_pm_col: time_key → 'AM' if hour24 < 12, else 'PM'."""

    def test_boundary_values(self, spark):
        # Midnight → "AM", last second of day → "PM"
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_am_pm_col("value")))
        assert result == ["AM", "PM"]

    def test_noon_transition(self, spark):
        # 11:59:59 is still AM, 12:00:00 flips to PM
        df = make_df(spark, [115959, 120000])
        result = collect_col(df.withColumn("result", _calculate_am_pm_col("value")))
        assert result == ["AM", "PM"]

    def test_representative_middle_values(self, spark):
        # Typical morning and afternoon values
        df = make_df(spark, [90000, 150000])
        result = collect_col(df.withColumn("result", _calculate_am_pm_col("value")))
        assert result == ["AM", "PM"]


class TestCalculateMinuteCol:
    """_calculate_minute_col: time_key → minute (0–59)."""

    def test_boundary_values(self, spark):
        # Minute 0 and minute 59
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_minute_col("value")))
        assert result == [0, 59]

    def test_rollover_points(self, spark):
        # Minute 59 of hour 0 → minute 0 of hour 1
        df = make_df(spark, [5959, 10000])
        result = collect_col(df.withColumn("result", _calculate_minute_col("value")))
        assert result == [59, 0]

    def test_representative_middle_values(self, spark):
        # 13:30:00 → minute = 30
        df = make_df(spark, [133000])
        result = collect_col(df.withColumn("result", _calculate_minute_col("value")))
        assert result == [30]


class TestCalculateSecondCol:
    """_calculate_second_col: time_key → second (0–59)."""

    def test_boundary_values(self, spark):
        # Second 0 and second 59
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_second_col("value")))
        assert result == [0, 59]

    def test_rollover_points(self, spark):
        # Second 59 → second 0 at minute boundary
        df = make_df(spark, [59, 100])
        result = collect_col(df.withColumn("result", _calculate_second_col("value")))
        assert result == [59, 0]

    def test_representative_middle_values(self, spark):
        # 13:15:35 → second = 35
        df = make_df(spark, [131535])
        result = collect_col(df.withColumn("result", _calculate_second_col("value")))
        assert result == [35]


class TestCalculateTimeBucketCol:
    """_calculate_time_bucket_col: time_key + bucket_size → bucket start as HHMM int.
    Tested primarily with 15-min buckets; rollover test covers multiple bucket sizes."""

    def test_boundary_values(self, spark):
        # Start of day → bucket 0; 23:59:59 → last 15-min bucket starts at 23:45 (2345)
        df = make_df(spark, [0, 235959])
        result = collect_col(
            df.withColumn("result", _calculate_time_bucket_col("value", 15))
        )
        assert result == [0, 2345]

    def test_rollover_points(self, spark):
        # Within an hour: minute 14 stays in bucket 0, minute 15 enters bucket 15
        df = make_df(spark, [11400, 11500, 13000, 13059, 13100])
        result_15 = collect_col(
            df.withColumn("result", _calculate_time_bucket_col("value", 15))
        )
        assert result_15 == [100, 115, 130, 130, 130]

    def test_representative_middle_values(self, spark):
        # 13:22:00 → hour=13, min=22, bucket=floor(22/15)*15=15 → 1315
        df = make_df(spark, [132200])
        result = collect_col(
            df.withColumn("result", _calculate_time_bucket_col("value", 15))
        )
        assert result == [1315]


class TestDisplayBucketTimeCol:
    """_display_bucket_time_col: bucket int (HHMM) → 'HH:MM' string with zero-padding."""

    def test_boundary_values(self, spark):
        # Bucket 0 → "00:00", bucket 2345 → "23:45"
        df = make_df(spark, [0, 2345])
        result = collect_col(df.withColumn("result", _display_bucket_time_col("value")))
        assert result == ["00:00", "23:45"]

    def test_rollover_points(self, spark):
        # Hour boundary in bucket format: 59 → "00:59", 100 → "01:00"
        # Also lpad boundary: 900 → "09:00", 1000 → "10:00"
        df = make_df(spark, [59, 100, 900, 1000])
        result = collect_col(df.withColumn("result", _display_bucket_time_col("value")))
        assert result == ["00:59", "01:00", "09:00", "10:00"]

    def test_representative_middle_values(self, spark):
        # 13:15
        df = make_df(spark, [1315])
        result = collect_col(df.withColumn("result", _display_bucket_time_col("value")))
        assert result == ["13:15"]


class TestCalculateDayPartCol:
    """_calculate_day_part_col: time_key → day part name.
    Ranges: 0–59999 Early Night | 60000–119999 Morning | 120000–179999 Afternoon
            180000–219999 Evening | 220000–240000 Night.
    Suggestion: test_each_day_part_category is more informative than test_rollover_points
    because there are 5 distinct categories each needing verification."""

    def test_boundary_values(self, spark):
        # First and last valid time_key
        df = make_df(spark, [0, 235959])
        result = collect_col(df.withColumn("result", _calculate_day_part_col("value")))
        assert result == ["Early Night", "Night"]

    def test_each_day_part_boundary(self, spark):
        # One value just before and just at each category transition
        df = make_df(
            spark, [59999, 60000, 119999, 120000, 179999, 180000, 219999, 220000]
        )
        result = collect_col(df.withColumn("result", _calculate_day_part_col("value")))
        assert result == [
            "Early Night",
            "Morning",
            "Morning",
            "Afternoon",
            "Afternoon",
            "Evening",
            "Evening",
            "Night",
        ]

    def test_each_day_part_category(self, spark):
        # One representative value from the middle of each day part
        df = make_df(spark, [30000, 90000, 150000, 200000, 230000])
        result = collect_col(df.withColumn("result", _calculate_day_part_col("value")))
        assert result == ["Early Night", "Morning", "Afternoon", "Evening", "Night"]


class TestGenerateTimeDim:
    def test_generate_time_dim(self, spark):
        df = generate_time_dim(spark, "2026-04-04")
        df.show(2)
