"""Run SQL against the curated data product with Spark SQL.

The curated Parquet tables are registered as SQL views, so they can be queried
by name, like tables in a database:
    coin_daily_sentiment, sentiment_summary

Usage:
    python run_query.py queries/sentiment_overview.sql      # run a .sql file
    python run_query.py --sql "SELECT COUNT(*) FROM coin_daily_sentiment"
"""

import argparse
from pathlib import Path

import config
from run_pipeline import ensure_java

TABLES = ("coin_daily_sentiment", "sentiment_summary")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query the curated tables with Spark SQL")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("file", nargs="?", type=Path, help="a .sql file to run")
    group.add_argument("--sql", help="a SQL statement given directly")
    parser.add_argument("--rows", type=int, default=50, help="maximum number of rows to show")
    args = parser.parse_args()

    ensure_java()
    from transform import create_spark  # imported after Java is set up

    spark = create_spark()
    try:
        for name in TABLES:
            path = config.DATA_DIR / "curated" / name
            if not path.exists():
                raise SystemExit(f"Table '{name}' not found in {path}. Run run_pipeline.py first.")
            # A view is a name for the data: "FROM coin_daily_sentiment" now reads this Parquet folder.
            spark.read.parquet(str(path)).createOrReplaceTempView(name)

        sql = args.sql or args.file.read_text(encoding="utf-8")
        print(f"\n{sql.strip()}\n")
        spark.sql(sql).show(args.rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
