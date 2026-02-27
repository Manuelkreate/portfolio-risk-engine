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
    ), 6) AS rollvol20,
    ROUND (
      STDDEV(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ), 6) AS rollvol60,    
    ROUND (
      STDDEV(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 6) AS rollvol90,
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
    ), 6) AS rollavg20,
    ROUND (
      AVG(rolling_vol.daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ), 6) AS rollavg60,    
    ROUND (
      AVG(rolling_vol.daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 6) AS rollavg90,
  FROM rolling_vol
),

sharpe_ratio AS (
  SELECT 
    *,
    ROUND (
      (rollavg20 - (risk_free_rate/252)) / NULLIF(rollvol20, 0), 4
    ) AS shrp20,
    ROUND (
      (rollavg60 - (risk_free_rate/252)) / NULLIF(rollvol60, 0), 4
    ) AS shrp60,
    ROUND (
      (rollavg90 - (risk_free_rate/252)) / NULLIF(rollvol90, 0), 4
    ) AS shrp90,
  FROM rolling_avg
),

position_size AS (
  SELECT 
    *,
    ROUND (
      target_risk / rollvol20
    , 4) AS pos_size20,
    ROUND (
      target_risk / rollvol60
    , 4) AS pos_size60,
    ROUND (
      target_risk / rollvol90
    , 4) AS pos_size90        
  FROM sharpe_ratio
)

SELECT * FROM position_size
WHERE previous_close IS NOT NULL