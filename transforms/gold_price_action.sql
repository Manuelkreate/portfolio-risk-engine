DECLARE risk_free_rate FLOAT64 DEFAULT 0.05;
DECLARE target_risk FLOAT64 DEFAULT 0.02;
CREATE OR REPLACE TABLE `portfolio-risk-engine.gold.price_action` AS

WITH rolling_vol AS (
  SELECT
    date,
    ticker,
    close_price,
    previous_close,
    ingestion_timestamp,
    daily_return,
    daily_return_pct,
    ROUND (
      STDDEV(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 6) AS volatility_20d,
    ROUND (
      STDDEV(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ), 6) AS volatility_60d,    
    ROUND (
      STDDEV(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 6) AS volatility_90d
  FROM `portfolio-risk-engine.silver.price_returns`
),

rolling_avg AS (
  SELECT
    *,
    ROUND (
      AVG(rolling_vol.daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 6) AS avg_return_20d,
    ROUND (
      AVG(rolling_vol.daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ), 6) AS avg_return_60d,    
    ROUND (
      AVG(rolling_vol.daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 6) AS avg_return_90d
  FROM rolling_vol
),

sharpe_ratio AS (
  SELECT 
    *,
    ROUND (
      (avg_return_20d - (risk_free_rate/252)) / NULLIF(volatility_20d, 0), 4
    ) AS sharpe_20d,
    ROUND (
      (avg_return_60d - (risk_free_rate/252)) / NULLIF(volatility_60d, 0), 4
    ) AS sharpe_60d,
    ROUND (
      (avg_return_90d - (risk_free_rate/252)) / NULLIF(volatility_90d, 0), 4
    ) AS sharpe_90d
  FROM rolling_avg
),

position_size AS (
  SELECT 
    *,
    ROUND (
      target_risk / volatility_20d
    , 4) AS position_size_20d,
    ROUND (
      target_risk / volatility_60d
    , 4) AS position_size_60d,
    ROUND (
      target_risk / volatility_90d
    , 4) AS position_size_90d        
  FROM sharpe_ratio
)

SELECT * FROM position_size
WHERE previous_close IS NOT NULL