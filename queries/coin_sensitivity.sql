-- Which coins react most to sentiment? For each coin: the median next-day
-- return after Extreme Greed days minus the one after Extreme Fear days.
-- Only coins with at least 30 days in both classes, so the result is not
-- based on a handful of days.
SELECT
    symbol,
    name,
    is_stablecoin,
    COUNT_IF(fng_class = 'Extreme Fear')                                       AS fear_days,
    COUNT_IF(fng_class = 'Extreme Greed')                                      AS greed_days,
    ROUND(MEDIAN(CASE WHEN fng_class = 'Extreme Fear'  THEN next_day_return END) * 100, 3) AS median_after_fear_pct,
    ROUND(MEDIAN(CASE WHEN fng_class = 'Extreme Greed' THEN next_day_return END) * 100, 3) AS median_after_greed_pct,
    ROUND((MEDIAN(CASE WHEN fng_class = 'Extreme Greed' THEN next_day_return END)
         - MEDIAN(CASE WHEN fng_class = 'Extreme Fear'  THEN next_day_return END)) * 100, 3) AS greed_minus_fear_pct
FROM coin_daily_sentiment
GROUP BY symbol, name, is_stablecoin
HAVING fear_days >= 30 AND greed_days >= 30
ORDER BY greed_minus_fear_pct DESC;
