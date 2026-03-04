{{ config(materialized='view') }}

SELECT
    date,
    ticker,
    volatility_20d,
    volatility_60d,
    volatility_90d
FROM {{ ref('price_action') }}
WHERE volatility_20d IS NOT NULL
AND volatility_60d IS NOT NULL
AND volatility_90d IS NOT NULL