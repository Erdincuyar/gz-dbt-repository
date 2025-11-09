WITH sales AS (
    SELECT
        date_date,
        orders_id,
        pdt_id,
        revenue,
        quantity
    FROM {{ ref('stg_raw__sales') }}
),

product AS (
    SELECT
        product_id,
        purchase_price
    FROM {{ ref('stg_raw__product') }}
)

SELECT
    s.date_date,
    s.orders_id,
    s.pdt_id,
    s.revenue,
    s.quantity,
    p.purchase_price,

    -- satın alma maliyeti
    CAST(s.quantity * p.purchase_price AS FLOAT64) AS purchase_cost,

    -- marj
    CAST(s.revenue - (s.quantity * p.purchase_price) AS FLOAT64) AS margin

FROM sales AS s
LEFT JOIN product AS p
    ON s.pdt_id = p.product_id
