{{ config(materialized='view') }}

WITH shared_date AS (
    SELECT MAX(date) as latest_date
    FROM {{ ref('price_action') }}
    WHERE date IN (
        SELECT date 
        FROM {{ ref('price_action') }}
        GROUP BY date
        HAVING COUNT(DISTINCT ticker) = 6
    )
),
raw_positions AS (
    SELECT 
        p.ticker,
        p.position_size_20d AS raw_20d,
        p.position_size_60d AS raw_60d,
        p.position_size_90d AS raw_90d
    FROM {{ ref('price_action') }} p
    INNER JOIN shared_date s ON p.date = s.latest_date
)
SELECT
    ticker,
    ROUND(raw_20d / SUM(raw_20d) OVER (), 4) AS position_size_20d,
    ROUND(raw_60d / SUM(raw_60d) OVER (), 4) AS position_size_60d,
    ROUND(raw_90d / SUM(raw_90d) OVER (), 4) AS position_size_90d
FROM raw_positions