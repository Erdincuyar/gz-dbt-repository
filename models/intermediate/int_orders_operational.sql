WITH orders_margin AS (
    SELECT *
    FROM {{ ref('int_orders_margin') }}
),

ship AS (
    SELECT
        orders_id,
        shipping_fee,
        log_cost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    o.orders_id,
    o.date_date,
    o.margin + s.shipping_fee - s.log_cost - s.ship_cost AS operational_margin
FROM orders_margin AS o
LEFT JOIN ship AS s
    ON o.orders_id = s.orders_id
