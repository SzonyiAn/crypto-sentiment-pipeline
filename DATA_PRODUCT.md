# Data Product: Crypto Market Sentiment

The curated layer contains two Parquet tables. Together they answer the question:
*how do cryptocurrencies behave on the day after fearful vs. greedy market sentiment?*

| | |
|---|---|
| **Location** | Azure Blob Storage, container `curated` (local copy: `data/curated/`) |
| **Format** | Parquet, queried with Spark SQL (`run_query.py`) |
| **Period covered** | 2018-02-01 – 2021-07-06 (overlap of both sources) |
| **Produced by** | `transform.py`, published by `run_pipeline.py` only after all checks passed |
| **Quality report** | `curated/_quality_report.json`, saved with every published version |
| **Update frequency** | On demand (`python run_pipeline.py`). The Fear & Greed source is updated daily, but the price source is a static Kaggle snapshot ending in July 2021, so re-running today gives the same result. With a live price source, a daily run would be appropriate. |
| **Refresh strategy** | Full refresh: each run rebuilds and **replaces** both tables, so re-running never creates duplicates |

---

## Table 1: `coin_daily_sentiment`

**Grain:** one row per **coin per day** (unique key: `symbol` + `date`). About 23,700 rows, 23 coins.

| Column | Type | Meaning |
|---|---|---|
| `date` | date | Calendar day (UTC) |
| `symbol` | string | Ticker, e.g. `BTC`, `ETH` |
| `name` | string | Coin name, e.g. `Bitcoin` |
| `is_stablecoin` | boolean | `true` for USDT and USDC (pegged to $1), the control group |
| `close` | double | Closing price in USD |
| `volume` | double | Traded volume in USD; `null` if not recorded in the source |
| `market_cap` | double | Market capitalization in USD; `null` if not recorded in the source |
| `daily_return` | double | Price change since the previous day: `close / previous close − 1` (0.05 = +5%). `null` if the previous day is missing |
| `next_day_return` | double | The **following** day's `daily_return`: what happened *after* this day's sentiment. `null` on a coin's last day |
| `volatility_7d` | double | Standard deviation of the last 7 daily returns (today included): how much the price jumped around. `null` if fewer than 7 returns are available |
| `fng_value` | int | Fear & Greed Index of the day, 0 (extreme fear) to 100 (extreme greed) |
| `fng_class` | string | `Extreme Fear`, `Fear`, `Neutral`, `Greed`, `Extreme Greed` |
| `fng_class_order` | int | 1 to 5 in the order above, for sorting (text would sort alphabetically) |

**Design notes**
- Returns and volatility are computed on each coin's **full price history before the join**, so the first
  days of the period can still look back at earlier prices.
- `next_day_return` is what makes the question answerable: sentiment of day *t* is compared with the price
  move of day *t + 1*, so the sentiment is known *before* the move.
- The join is an **inner join on `date`**: a coin-day appears only if both sources have that day.

## Table 2: `sentiment_summary`

**Grain:** one row per **sentiment class × stablecoin flag** (10 rows).

| Column | Type | Meaning |
|---|---|---|
| `fng_class_order`, `fng_class` | int, string | Sentiment class (see above) |
| `is_stablecoin` | boolean | Stablecoins and other coins are summarized separately |
| `days` | long | Number of distinct days in this class |
| `coin_days` | long | Number of rows of `coin_daily_sentiment` summarized here |
| `avg_daily_return` | double | Average return on the day itself |
| `avg_next_day_return` | double | Average return on the following day |
| `median_next_day_return` | double | Median return on the following day (robust to extreme days) |
| `share_next_day_up` | double | Share of coin-days where the price rose the next day (0.53 = 53%) |
| `avg_volatility_7d` | double | Average 7-day volatility |

---

## Quality checks

`validate.py` runs 32 checks after every transformation. **ERROR** checks stop the pipeline before publishing;
**WARNING** checks are logged and recorded in the quality report.

| Table | Check | Severity |
|---|---|---|
| staged/prices | `date`, `symbol`, `close` never null | ERROR |
| staged/prices | `symbol` + `date` unique | ERROR |
| staged/prices | close price > 0; high ≥ low | ERROR |
| staged/prices | all expected stablecoins present | ERROR |
| staged/prices | stablecoins trade between $0.90 and $1.10 | WARNING |
| staged/fear_greed | `date`, `fng_value`, `fng_class` never null; `date` unique | ERROR |
| staged/fear_greed | value between 0 and 100 | ERROR |
| staged/fear_greed | no missing days in the index | WARNING |
| staged (both) | rejected rows exist | WARNING |
| staged (both) | at most 1% of a source rejected | ERROR |
| coin_daily_sentiment | not empty; key columns never null; `symbol` + `date` unique | ERROR |
| coin_daily_sentiment | volatility not negative | ERROR |
| coin_daily_sentiment | daily return within ±100% | WARNING |
| coin_daily_sentiment | every price row in the overlap found a sentiment value | WARNING |
| coin_daily_sentiment | row count equals the number of matched price rows | ERROR |
| sentiment_summary | all 5 sentiment classes present | ERROR |
| sentiment_summary | `coin_days` add up to the detail table's row count | ERROR |

In addition, `transform.py` checks every row while staging. Rows with an unparseable date or timestamp, a
missing or non-positive price, high < low, a sentiment value outside 0–100 or an unknown class are not
silently dropped: they are written to `staged/rejected/` together with a `reject_reason`.

**Current state:** 0 errors, 4 explained warnings: 4 missing index days (3 inside the overlap, so 45 coin-days
are unmatched), 3 early Tether prices outside $0.90–1.10, and 3 real daily moves above ±100%.

---

## Example queries

Run with `python run_query.py <file>`. All three are in the `queries/` folder.

**Main question:** behaviour of non-stablecoins after each sentiment class (`queries/sentiment_overview.sql`):

```sql
SELECT
    fng_class                                 AS sentiment,
    days,
    ROUND(avg_next_day_return * 100, 3)       AS avg_next_day_return_pct,
    ROUND(median_next_day_return * 100, 3)    AS median_next_day_return_pct,
    ROUND(share_next_day_up * 100, 1)         AS next_day_up_pct,
    ROUND(avg_volatility_7d * 100, 2)         AS avg_volatility_7d_pct
FROM sentiment_summary
WHERE NOT is_stablecoin
ORDER BY fng_class_order;
```

**Other examples**
- `queries/coin_sensitivity.sql`: which coins react most? Median next-day return after Extreme Greed minus after
  Extreme Fear, per coin.
- `queries/btc_monthly.sql`: Bitcoin month by month, with average sentiment, month-end price and volatility.
