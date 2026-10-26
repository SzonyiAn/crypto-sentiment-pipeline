"""Step 3 of the pipeline: data quality checks.

Runs a list of checks on the staged and curated tables. Each check has a
severity:
  ERROR   the data is wrong → the pipeline must stop, nothing gets published
  WARNING something is unusual but explainable → logged, the pipeline continues

Every check counts the rows that VIOLATE a rule. 0 bad rows = PASS.

Results are also saved as a small JSON report (data/curated/_quality_report.json),
so every published dataset comes with a record of how it was checked.

Usage:
    python validate.py
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config
from transform import FNG_CLASS_ORDER, create_spark

log = logging.getLogger("validate")

# Rejected rows are allowed, but if more than this share of a source was
# rejected, something is badly wrong with the input (e.g. a format change).
MAX_REJECTED_SHARE = 0.01  # 1%

# A daily move above +/-100% is possible in crypto but extremely rare: worth a warning.
SUSPICIOUS_DAILY_RETURN = 1.0

# A stablecoin is supposed to be worth about $1.
STABLECOIN_PRICE_RANGE = (0.9, 1.1)


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str      # "ERROR" or "WARNING"
    bad_rows: int
    passed: bool


class DataQualityError(Exception):
    """Raised when at least one ERROR-level check fails."""


class Validator:
    def __init__(self):
        self.results: list[CheckResult] = []

    def check(self, table: str, name: str, bad_rows: int, severity: str = "ERROR") -> None:
        result = CheckResult(table, name, severity, int(bad_rows), bad_rows == 0)
        self.results.append(result)
        if result.passed:
            log.info("PASS     %-22s %s", table, name)
        else:
            level = logging.ERROR if severity == "ERROR" else logging.WARNING
            label = "FAIL" if severity == "ERROR" else "WARN"
            log.log(level, "%-8s %-22s %s  (%d bad rows)", label, table, name, bad_rows)

    # --- reusable rule types --------------------------------------------------

    def not_null(self, df: DataFrame, table: str, columns: list[str]) -> None:
        for col in columns:
            self.check(table, f"'{col}' is never null", df.filter(F.col(col).isNull()).count())

    def unique(self, df: DataFrame, table: str, key: list[str]) -> None:
        """A key is unique if no key value appears more than once."""
        duplicates = df.groupBy(*key).count().filter(F.col("count") > 1).count()
        self.check(table, f"{' + '.join(key)} is unique", duplicates)

    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "ERROR"]

    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "WARNING"]


def read(spark: SparkSession, layer: str, name: str) -> DataFrame:
    return spark.read.parquet(str(config.DATA_DIR / layer / name))


def missing_dates(df: DataFrame) -> int:
    """How many calendar days between the first and last date have no row?"""
    stats = df.agg(F.min("date").alias("first"), F.max("date").alias("last"),
                   F.countDistinct("date").alias("present")).first()
    expected = (stats["last"] - stats["first"]).days + 1
    return expected - stats["present"]


def check_staged(v: Validator, spark: SparkSession) -> None:
    prices = read(spark, "staged", "prices")
    fng = read(spark, "staged", "fear_greed")

    # --- staged/prices
    v.not_null(prices, "staged/prices", ["date", "symbol", "close"])
    v.unique(prices, "staged/prices", ["symbol", "date"])
    v.check("staged/prices", "close price is positive", prices.filter(F.col("close") <= 0).count())
    v.check("staged/prices", "high >= low", prices.filter(F.col("high") < F.col("low")).count())
    v.check("staged/prices", "all expected stablecoins present",
            len(config.STABLECOINS - {r.symbol for r in prices.select("symbol").distinct().collect()}))
    low, high = STABLECOIN_PRICE_RANGE
    v.check("staged/prices", f"stablecoins trade between ${low} and ${high}",
            prices.filter(F.col("is_stablecoin") & ~F.col("close").between(low, high)).count(),
            severity="WARNING")

    # --- staged/fear_greed
    v.not_null(fng, "staged/fear_greed", ["date", "fng_value", "fng_class"])
    v.unique(fng, "staged/fear_greed", ["date"])
    v.check("staged/fear_greed", "value between 0 and 100",
            fng.filter(~F.col("fng_value").between(0, 100)).count())
    v.check("staged/fear_greed", "no missing days in the index",
            missing_dates(fng), severity="WARNING")

    # --- rejected rows: a few are fine, many mean the input format changed
    for source, good in (("prices", prices), ("fear_greed", fng)):
        rejected = read(spark, "staged", f"rejected/{source}").count()
        total = rejected + good.count()
        v.check(f"staged/{source}", "rejected rows exist", rejected, severity="WARNING")
        too_many = rejected if rejected > MAX_REJECTED_SHARE * total else 0
        v.check(f"staged/{source}", f"rejected share <= {MAX_REJECTED_SHARE:.0%}", too_many)


def check_curated(v: Validator, spark: SparkSession) -> None:
    daily = read(spark, "curated", "coin_daily_sentiment")
    summary = read(spark, "curated", "sentiment_summary")
    prices = read(spark, "staged", "prices")
    fng = read(spark, "staged", "fear_greed")

    t = "coin_daily_sentiment"
    v.check(t, "table is not empty", 1 if daily.limit(1).count() == 0 else 0)
    v.not_null(daily, t, ["date", "symbol", "close", "fng_value", "fng_class", "is_stablecoin"])
    v.unique(daily, t, ["symbol", "date"])
    v.check(t, "volatility is not negative", daily.filter(F.col("volatility_7d") < 0).count())
    v.check(t, f"daily return within +/-{SUSPICIOUS_DAILY_RETURN:.0%}",
            daily.filter(F.abs("daily_return") > SUSPICIOUS_DAILY_RETURN).count(),
            severity="WARNING")

    # Join check: inside the overlap period, every price row should have found
    # a sentiment value. Rows that did not are lost in the inner join, so we
    # count and report them instead of losing them silently.
    first, last = fng.agg(F.min("date"), F.max("date")).first()
    prices_in_overlap = prices.filter(F.col("date").between(first, last))
    unmatched = prices_in_overlap.join(fng, on="date", how="left_anti").count()
    v.check(t, "every price row in the overlap has a sentiment value",
            unmatched, severity="WARNING")
    # ...and the curated table must contain exactly the matched rows, no more, no less.
    expected = prices_in_overlap.count() - unmatched
    v.check(t, "row count = matched price rows", abs(daily.count() - expected))

    # Reconciliation: the summary must account for every row of the detail table.
    t = "sentiment_summary"
    v.check(t, "all 5 sentiment classes present",
            len(set(FNG_CLASS_ORDER) - {r.fng_class for r in summary.select("fng_class").collect()}))
    summary_total = summary.agg(F.sum("coin_days")).first()[0] or 0
    v.check(t, "coin_days add up to the detail table", abs(summary_total - daily.count()))


def write_report(v: Validator) -> None:
    report = {
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "passed": not v.failed(),
        "errors": len(v.failed()),
        "warnings": len(v.warnings()),
        "checks": [asdict(r) for r in v.results],
    }
    path = config.DATA_DIR / "curated" / "_quality_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2))
    log.info("Quality report saved to %s", path.relative_to(config.PROJECT_DIR))


def run(spark: SparkSession | None = None) -> Validator:
    own_spark = spark is None
    spark = spark or create_spark()
    log.info("=== VALIDATE started ===")
    v = Validator()
    try:
        check_staged(v, spark)
        check_curated(v, spark)
    finally:
        if own_spark:
            spark.stop()

    write_report(v)
    log.info("=== VALIDATE finished: %d checks, %d errors, %d warnings ===",
             len(v.results), len(v.failed()), len(v.warnings()))
    if v.failed():
        names = "; ".join(f"{r.table}: {r.check}" for r in v.failed())
        raise DataQualityError(f"{len(v.failed())} check(s) failed: {names}")
    return v


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    try:
        run()
    except DataQualityError as exc:
        log.error("DATA QUALITY FAILED: %s", exc)
        raise SystemExit(1)  # exit code 1 = failure, so scripts/schedulers notice it


if __name__ == "__main__":
    main()
