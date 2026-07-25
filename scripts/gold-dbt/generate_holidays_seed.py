"""
Regenerates dbt/seeds/us_holidays.csv from the `holidays` package.

dbt SQL cannot replicate the US holiday rule tables (moving holidays,
observed-date adjustments, etc.), so the holiday calendar is materialized
once as a seed instead of computed at run time. Re-run this script whenever
the covered year range needs to be extended.

Holiday names are written with single quotes backslash-escaped (e.g.
"New Year\\'s Day") because dbt-spark's session connection method (Spark
Connect over EMR Serverless) builds seed inserts via unescaped string
substitution and would otherwise emit invalid SQL for names containing an
apostrophe. Spark SQL's string literal grammar parses "\\'" back into a
plain "'", so this round-trips correctly. If dbt-spark's binding
substitution is ever fixed upstream, this escaping should be removed and
the seed regenerated.

Usage:
    poetry run python scripts/gold-dbt/generate_holidays_seed.py --start-year 2015 --end-year 2035
"""

import argparse
import csv
from pathlib import Path

import holidays

SEED_PATH = Path(__file__).resolve().parent.parent.parent / "dbt" / "seeds" / "us_holidays.csv"


def main(start_year: int, end_year: int) -> None:
    us_holidays = holidays.US(years=range(start_year, end_year + 1))
    rows = sorted(us_holidays.items(), key=lambda item: item[0])

    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SEED_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["full_date", "holiday_name"])
        writer.writerows(
            (date.isoformat(), name.replace("'", "\\'")) for date, name in rows
        )

    print(f"Wrote {len(rows)} holiday rows to {SEED_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    args = parser.parse_args()
    main(args.start_year, args.end_year)
