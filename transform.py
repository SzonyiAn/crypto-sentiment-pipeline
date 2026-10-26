"""Step 2 of the pipeline: PySpark transformations (raw → staged → curated).

STAGED layer: clean and standardize each source separately.
  staged/prices            one row per coin per day, proper types, snake_case names
  staged/fear_greed        one row per day, proper types
  staged/rejected/...      rows we could not use, WITH the reason (nothing disappears silently)

CURATED layer: combine the sources and build the analytical tables.
  curated/coin_daily_sentiment   coin × day: returns, volatility + fear & greed of that day
  curated/sentiment_summary      one row per sentiment class: how did coins behave?

All tables are written as Parquet with mode "overwrite", so running the step
again replaces the results instead of adding duplicate rows.

Usage:
    python transform.py                 # local only
    python transform.py --target azure  # also publish staged + curated to Azure
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from storage import Storage

log = logging.getLogger("transform")

# The order of the five Fear & Greed classes, from most fearful to most greedy.
# Text sorts alphabetically ("Extreme Fear", "Extreme Greed", "Fear", ...), which
# is useless for reporting, so we store an explicit number next to the class.
FNG_CLASS_ORDER = {
    "Extreme Fear": 1, "Fear": 2, "Neutral": 3, "Greed": 4, "Extreme Greed": 5,
}

# We tell Spark the exact schema instead of letting it guess (inferSchema).
# Guessing needs an extra pass over the data and can guess wrong; an explicit
# schema is faster and makes the expected structure part of the code.
# Date is read as text on purpose: we convert it ourselves in a controlled way.
PRICE_SCHEMA = T.StructType([
    T.StructField("SNo", T.IntegerType()),
    T.StructField("Name", T.StringType()),
    T.StructField("Symbol", T.StringType()),
    T.StructField("Date", T.StringType()),
    T.StructField("High", T.DoubleType()),
    T.StructField("Low", T.DoubleType()),
    T.StructField("Open", T.DoubleType()),
    T.StructField("Close", T.DoubleType()),
    T.StructField("Volume", T.DoubleType()),
    T.StructField("Marketcap", T.DoubleType()),
])


def create_spark() -> SparkSession:
    """Start Spark in local mode, using all CPU cores of this machine (local[*])."""
    spark = (
        SparkSession.builder
        .appName("crypto-sentiment-pipeline")
        .master("local[*]")
        # The API timestamps are in UTC. Fixing the session time zone to UTC makes
        # timestamp → date conversion give the same result on every machine.
        .config("spark.sql.session.timeZone", "UTC")
        # Our data is small; the default of 200 partitions would just create
        # 200 tiny tasks and files. 8 is plenty.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # hide Spark's very chatty INFO logs
    return spark


def latest_raw_folder(domain: str) -> Path:
    """Find the newest ingestion folder of a source, e.g. raw/prices/2026/10/26.

    The raw layer can contain several ingestion days. We process the newest
    one, because each ingestion contains the full history of both sources.
    """
    base = config.DATA_DIR / "raw" / domain
    day_folders = sorted(p for p in base.glob("*/*/*") if p.is_dir())
    if not day_folders:
        raise FileNotFoundError(f"No raw data for '{domain}' in {base}. Run ingest.py first.")
    return day_folders[-1]


CURATED_TABLES = ("coin_daily_sentiment", "sentiment_summary")


def write_table(df: DataFrame, layer: str, name: str, storage: Storage) -> None:
    """Write a DataFrame as Parquet, replacing any earlier version (idempotent).

    Staged tables are uploaded to Azure right away. Curated tables are NOT:
    they are the final data product, so they are only published after
    validate.py has checked them (see publish_curated and run_pipeline.py).
    """
    path = storage.local_path(layer, name)
    df.write.mode("overwrite").parquet(str(path))
    log.info("Wrote %s/%s (%d rows)", layer, name, df.count())
    if layer != "curated":
        storage.publish_folder(layer, name)


def publish_curated(storage: Storage) -> None:
    """Upload the curated tables to Azure. Called only after validation passed."""
    for name in CURATED_TABLES:
        storage.publish_folder("curated", name)


# =============================================================================
# STAGED LAYER
# =============================================================================

def stage_prices(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Clean the coin price files. Returns (good rows, rejected rows)."""
    folder = latest_raw_folder("prices")
    log.info("Reading prices from %s", folder)

    # Spark reads all 23 CSV files of the folder at once and stacks them into one
    # table. A number that cannot be parsed (e.g. "n/a") becomes null; the
    # reject rules below then catch it.
    raw = spark.read.csv(f"{folder}/source=file", header=True, schema=PRICE_SCHEMA)

    df = raw.select(
        # "2013-04-29 23:59:59" (text) → 2013-04-29 (date). The time part carries
        # no information (every row has 23:59:59), so a date is what we need.
        # try_to_timestamp returns null for a malformed date instead of crashing
        # the whole job (Spark 4 is strict by default, "ANSI mode").
        F.to_date(F.try_to_timestamp(F.col("Date"), F.lit("yyyy-MM-dd HH:mm:ss"))).alias("date"),
        F.col("Symbol").alias("symbol"),
        F.col("Name").alias("name"),
        F.col("Open").alias("open"),
        F.col("High").alias("high"),
        F.col("Low").alias("low"),
        F.col("Close").alias("close"),
        # A volume or market cap of exactly 0 does not mean "nobody traded":
        # it means the value was not recorded (early days of a coin). We turn
        # these hidden missing values into real nulls, so averages are not
        # pulled down by fake zeros.
        F.when(F.col("Volume") > 0, F.col("Volume")).alias("volume"),
        F.when(F.col("Marketcap") > 0, F.col("Marketcap")).alias("market_cap"),
        F.col("Symbol").isin(*config.STABLECOINS).alias("is_stablecoin"),
        # Which raw file the row came from: useful when tracing a problem back.
        F.col("_metadata.file_name").alias("source_file"),
    )

    # Row-level checks: give every bad row a reason instead of dropping it silently.
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("date").isNull(), "unparseable date")
         .when(F.col("symbol").isNull(), "missing symbol")
         .when(F.col("close").isNull() | (F.col("close") <= 0), "missing or non-positive close")
         .when(F.col("high") < F.col("low"), "high lower than low"),
    )
    rejected = df.filter(F.col("reject_reason").isNotNull())
    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    # One row per coin per day. If the same (symbol, date) appears twice (e.g. a
    # file was delivered twice), we keep only one copy.
    good = good.dropDuplicates(["symbol", "date"])
    return good, rejected


def stage_fear_greed(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Flatten and clean the API's JSON response. Returns (good rows, rejected rows)."""
    folder = latest_raw_folder("fear_greed")
    log.info("Reading fear & greed from %s", folder)

    # The whole response is ONE JSON object spread over many lines, so multiLine
    # is needed. Its "data" field is a list with one element per day.
    raw = spark.read.json(f"{folder}/source=api/fear_greed.json", multiLine=True)

    # explode(): turns the list into rows, one row per list element.
    records = raw.select(F.explode("data").alias("r")).select("r.*")

    df = records.select(
        # The API sends everything as text. The timestamp is "seconds since
        # 1970-01-01 UTC" (Unix time), e.g. "1551157200" → 2019-02-26.
        # try_cast: a malformed value becomes null instead of crashing the job,
        # so we can reject that one row with a reason and keep the rest.
        F.to_date(F.from_unixtime(F.col("timestamp").try_cast("long"))).alias("date"),
        F.col("value").try_cast("int").alias("fng_value"),
        F.col("value_classification").alias("fng_class"),
    )

    df = df.withColumn(
        "reject_reason",
        F.when(F.col("date").isNull(), "unparseable timestamp")
         .when(F.col("fng_value").isNull(), "missing or non-numeric value")
         .when(~F.col("fng_value").between(0, 100), "value outside 0-100")
         .when(~F.col("fng_class").isin(*FNG_CLASS_ORDER), "unknown classification"),
    )
    rejected = df.filter(F.col("reject_reason").isNotNull())
    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    # Map the class text to its order number (1 = Extreme Fear ... 5 = Extreme Greed).
    order_map = F.create_map(*[F.lit(x) for pair in FNG_CLASS_ORDER.items() for x in pair])
    good = good.withColumn("fng_class_order", order_map[F.col("fng_class")])

    good = good.dropDuplicates(["date"])
    return good, rejected


# =============================================================================
# CURATED LAYER
# =============================================================================

def build_coin_daily_sentiment(prices: DataFrame, fear_greed: DataFrame) -> DataFrame:
    """Join prices with sentiment and compute returns and volatility per coin.

    The window calculations are done on the FULL price history before the join,
    so the first days of the overlap period can still look back at earlier prices.
    """
    # A window = "for each row, look at the other rows of the same coin, in date order".
    by_coin = Window.partitionBy("symbol").orderBy("date")

    df = (
        prices
        .withColumn("prev_date", F.lag("date").over(by_coin))
        .withColumn("prev_close", F.lag("close").over(by_coin))
        # Daily return: how much the price changed since yesterday, e.g. 0.05 = +5%.
        # Only computed if the previous row really is yesterday; across a gap in
        # the data it would be a multi-day change disguised as a daily one.
        .withColumn(
            "daily_return",
            F.when(F.datediff("date", "prev_date") == 1,
                   F.col("close") / F.col("prev_close") - 1),
        )
    )

    # Volatility = how much the returns jump around: the standard deviation of
    # the daily returns over the last 7 rows (today + 6 days before).
    last_7_days = by_coin.rowsBetween(-6, 0)
    df = (
        df.withColumn("volatility_7d", F.stddev("daily_return").over(last_7_days))
          .withColumn("return_count_7d", F.count("daily_return").over(last_7_days))
          # Fewer than 7 real returns (start of a coin's history, gaps) → not reliable.
          .withColumn("volatility_7d",
                      F.when(F.col("return_count_7d") == 7, F.col("volatility_7d")))
          # The key column for our question: what happens on the day AFTER a given
          # sentiment? lead() looks one row forward, lag() looked one row back.
          .withColumn("next_day_return", F.lead("daily_return").over(by_coin))
    )

    # Inner join on date: keeps only the days that exist in BOTH sources,
    # i.e. the overlap period (Feb 2018 – Jul 2021).
    joined = df.join(fear_greed, on="date", how="inner")

    return joined.select(
        "date", "symbol", "name", "is_stablecoin",
        "close", "volume", "market_cap",
        "daily_return", "next_day_return", "volatility_7d",
        "fng_value", "fng_class", "fng_class_order",
    )


def build_sentiment_summary(daily: DataFrame) -> DataFrame:
    """One row per (sentiment class, stablecoin or not): the answer to our question."""
    return (
        daily.groupBy("fng_class_order", "fng_class", "is_stablecoin")
        .agg(
            F.countDistinct("date").alias("days"),
            F.count("*").alias("coin_days"),
            F.avg("daily_return").alias("avg_daily_return"),
            F.avg("next_day_return").alias("avg_next_day_return"),
            # The median is the middle value: half of the days were better, half
            # worse. Unlike the average, a few extreme days cannot pull it up.
            F.median("next_day_return").alias("median_next_day_return"),
            # Share of days where the coin went up the next day.
            F.avg((F.col("next_day_return") > 0).cast("int")).alias("share_next_day_up"),
            F.avg("volatility_7d").alias("avg_volatility_7d"),
        )
        .orderBy("is_stablecoin", "fng_class_order")
    )


# =============================================================================
# ENTRY POINT
# =============================================================================

def run(target: str = config.DEFAULT_TARGET, spark: SparkSession | None = None) -> None:
    """Run all transformations. run_pipeline.py passes in its own Spark session
    (shared with validation); when run alone, we start and stop our own."""
    storage = Storage(target)
    own_spark = spark is None
    spark = spark or create_spark()
    log.info("=== TRANSFORM started (target=%s) ===", target)
    try:
        prices, prices_rejected = stage_prices(spark)
        fear_greed, fng_rejected = stage_fear_greed(spark)

        write_table(prices, "staged", "prices", storage)
        write_table(fear_greed, "staged", "fear_greed", storage)
        write_table(prices_rejected, "staged", "rejected/prices", storage)
        write_table(fng_rejected, "staged", "rejected/fear_greed", storage)

        # Read the staged tables back from disk: the curated layer is built from
        # the stored staged data, exactly as the layer diagram says.
        prices = spark.read.parquet(str(storage.local_path("staged", "prices")))
        fear_greed = spark.read.parquet(str(storage.local_path("staged", "fear_greed")))

        daily = build_coin_daily_sentiment(prices, fear_greed)
        write_table(daily, "curated", "coin_daily_sentiment", storage)

        daily = spark.read.parquet(str(storage.local_path("curated", "coin_daily_sentiment")))
        write_table(build_sentiment_summary(daily), "curated", "sentiment_summary", storage)

        # Print the answer table, rounded and in percent so it is readable.
        summary = spark.read.parquet(str(storage.local_path("curated", "sentiment_summary")))
        summary.select(
            "fng_class", "is_stablecoin", "days",
            F.round(F.col("avg_next_day_return") * 100, 3).alias("avg_next_day_return_%"),
            F.round(F.col("median_next_day_return") * 100, 3).alias("median_next_day_return_%"),
            F.round(F.col("share_next_day_up") * 100, 1).alias("next_day_up_%"),
            F.round(F.col("avg_volatility_7d") * 100, 2).alias("avg_volatility_7d_%"),
        ).orderBy("is_stablecoin", "fng_class_order").show(truncate=False)

        log.info("=== TRANSFORM finished (curated tables written locally, not yet published) ===")
    finally:
        if own_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Raw → staged → curated with PySpark")
    parser.add_argument("--target", choices=["local", "azure"], default=config.DEFAULT_TARGET)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    run(args.target)


if __name__ == "__main__":
    main()
