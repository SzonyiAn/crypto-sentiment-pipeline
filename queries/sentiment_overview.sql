-- Main question: how do (non-stable) coins behave on the day after each
-- level of market sentiment?
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
