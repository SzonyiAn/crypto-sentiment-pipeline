-- Bitcoin month by month: average sentiment next to the price at month end.
-- Shows how sentiment followed the 2018 crash and the 2020-2021 bull market.
SELECT
    DATE_FORMAT(date, 'yyyy-MM')              AS month,
    ROUND(AVG(fng_value), 1)                  AS avg_fear_greed,
    ROUND(MAX_BY(close, date), 0)             AS month_end_close_usd,
    ROUND(AVG(volatility_7d) * 100, 2)        AS avg_volatility_7d_pct
FROM coin_daily_sentiment
WHERE symbol = 'BTC'
GROUP BY DATE_FORMAT(date, 'yyyy-MM')
ORDER BY month;
