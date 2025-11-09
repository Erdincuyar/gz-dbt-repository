{{ config(materialized='table') }}

WITH ship AS (
    SELECT
        orders_id,
        CAST(shipping_fee AS FLOAT64) AS shipping_fee,
        CAST(log_cost AS FLOAT64) AS log_cost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM {{ ref('stg_raw__ship') }}
)

SELECT
    o.date_date AS date,

    -- Toplam işlem sayısı
    COUNT(DISTINCT o.orders_id) AS total_orders,

    -- Toplam gelir
    SUM(COALESCE(o.revenue, 0)) AS total_revenue,

    -- Ortalama sepet
    SAFE_DIVIDE(SUM(COALESCE(o.revenue, 0)), COUNT(DISTINCT o.orders_id)) AS avg_basket,

    -- Operasyonel marj
    SUM(COALESCE(o.operational_margin, 0)) AS operational_margin,

    -- Toplam satın alma maliyeti
    SUM(COALESCE(o.purchase_cost, 0)) AS total_purchase_cost,

    -- Toplam nakliye ücretleri
    SUM(COALESCE(s.shipping_fee, 0)) AS total_shipping_fee,

    -- Toplam lojistik maliyetleri
    SUM(COALESCE(s.log_cost, 0)) AS total_log_cost,

    -- Satılan toplam ürün miktarı
    SUM(COALESCE(o.quantity, 0)) AS total_quantity

FROM {{ ref('int_orders_operational') }} AS o
LEFT JOIN {{ ref('int_orders_margin') }} AS om
    ON o.orders_id = om.orders_id
LEFT JOIN ship AS s
    ON o.orders_id = s.orders_id

GROUP BY 1
ORDER BY 1
