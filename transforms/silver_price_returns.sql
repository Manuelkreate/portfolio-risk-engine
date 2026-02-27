CREATE OR REPLACE TABLE `portfolio-risk-engine.silver.price_returns` AS

WITH deduplicated AS (
  SELECT
    date,
    ticker,
    close_price,
    ingestion_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY date, ticker 
      ORDER BY ingestion_timestamp DESC
    ) AS row_num
  FROM `portfolio-risk-engine.bronze.raw_prices`
),

cleaned AS (
  SELECT
    date,
    ticker,
    close_price,
    ingestion_timestamp
  FROM deduplicated
  WHERE row_num = 1
),

with_returns AS (
  SELECT
    date,
    ticker,
    close_price,
    ingestion_timestamp,
    LAG(close_price) OVER (
      PARTITION BY ticker
      ORDER BY date
    ) AS previous_close,
    ROUND (
      (close_price - LAG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY date
      )) / LAG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY date
      ) * 100,
    4) AS daily_return_pct
    FROM cleaned
),

final AS (
  SELECT
    *,
    ROUND(daily_return_pct / 100, 6) AS daily_return
  FROM with_returns
)

SELECT * FROM final
WHERE previous_close IS NOT NULL