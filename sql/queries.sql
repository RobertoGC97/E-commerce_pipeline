-- ============================================================================
-- OLIST DATA PIPELINE - QUERIES SQL DE ANÁLISIS
-- ============================================================================
-- Autor: [Tu Nombre]
-- Fecha: 2024
-- Descripción: Queries para análisis de datos de e-commerce Olist
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ANÁLISIS DE VENTAS POR CATEGORÍA
-- Demuestra: JOIN, GROUP BY, ORDER BY, funciones de agregación
-- ----------------------------------------------------------------------------
SELECT 
    p.product_category_name_english AS categoria,
    COUNT(DISTINCT oi.order_id) AS total_ordenes,
    COUNT(oi.order_item_id) AS total_items,
    ROUND(SUM(oi.price)::NUMERIC, 2) AS revenue_total,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS precio_promedio,
    ROUND(SUM(oi.freight_value)::NUMERIC, 2) AS costo_envio_total
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.product_id
WHERE p.product_category_name_english IS NOT NULL
GROUP BY p.product_category_name_english
HAVING SUM(oi.price) > 1000
ORDER BY revenue_total DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 2. DESEMPEÑO DE VENDEDORES (TOP 10)
-- Demuestra: JOIN múltiple, subquery, agregaciones
-- ----------------------------------------------------------------------------
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT oi.order_id) AS ordenes_completadas,
    ROUND(SUM(oi.price)::NUMERIC, 2) AS revenue_total,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS ticket_promedio,
    COUNT(oi.order_item_id) AS productos_vendidos,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS rating_promedio
FROM sellers s
INNER JOIN order_items oi ON s.seller_id = oi.seller_id
INNER JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city, s.seller_state
HAVING COUNT(DISTINCT oi.order_id) >= 10
ORDER BY revenue_total DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- 3. ANÁLISIS DE TIEMPOS DE ENTREGA
-- Demuestra: DATE functions, CASE WHEN, WHERE con condiciones complejas
-- ----------------------------------------------------------------------------
SELECT 
    o.order_status,
    COUNT(o.order_id) AS total_ordenes,
    ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400)::NUMERIC, 2) AS dias_entrega_promedio,
    ROUND(AVG(EXTRACT(EPOCH FROM (o.order_estimated_delivery_date - o.order_delivered_customer_date))/86400)::NUMERIC, 2) AS diferencia_estimado_real,
    COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) AS entregas_tarde,
    ROUND(100.0 * COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) / COUNT(o.order_id), 2) AS porcentaje_tarde
FROM orders o
WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND o.order_purchase_timestamp IS NOT NULL
GROUP BY o.order_status;


-- ----------------------------------------------------------------------------
-- 4. ANÁLISIS DE CLIENTES POR ESTADO
-- Demuestra: JOIN, GROUP BY, agregaciones múltiples
-- ----------------------------------------------------------------------------
SELECT 
    c.customer_state AS estado,
    COUNT(DISTINCT c.customer_id) AS total_clientes,
    COUNT(DISTINCT o.order_id) AS total_ordenes,
    ROUND(AVG(ss.total_payment)::NUMERIC, 2) AS ticket_promedio,
    ROUND(SUM(ss.total_payment)::NUMERIC, 2) AS revenue_total,
    ROUND(COUNT(DISTINCT o.order_id)::NUMERIC / COUNT(DISTINCT c.customer_id), 2) AS ordenes_por_cliente
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN sales_summary ss ON o.order_id = ss.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
HAVING COUNT(DISTINCT o.order_id) >= 100
ORDER BY revenue_total DESC;


-- ----------------------------------------------------------------------------
-- 5. PRODUCTOS MÁS VENDIDOS POR CATEGORÍA
-- Demuestra: Window functions, subquery, PARTITION BY
-- ----------------------------------------------------------------------------
WITH producto_ventas AS (
    SELECT 
        p.product_category_name_english AS categoria,
        p.product_id,
        COUNT(oi.order_id) AS veces_vendido,
        ROUND(SUM(oi.price)::NUMERIC, 2) AS revenue,
        ROW_NUMBER() OVER (PARTITION BY p.product_category_name_english ORDER BY COUNT(oi.order_id) DESC) AS ranking
    FROM products p
    INNER JOIN order_items oi ON p.product_id = oi.product_id
    WHERE p.product_category_name_english IS NOT NULL
    GROUP BY p.product_category_name_english, p.product_id
)
SELECT 
    categoria,
    product_id,
    veces_vendido,
    revenue
FROM producto_ventas
WHERE ranking <= 3
ORDER BY categoria, ranking;


-- ----------------------------------------------------------------------------
-- 6. MÉTODOS DE PAGO MÁS UTILIZADOS
-- Demuestra: GROUP BY, ORDER BY, agregaciones
-- ----------------------------------------------------------------------------
SELECT 
    p.payment_type AS metodo_pago,
    COUNT(DISTINCT p.order_id) AS ordenes_totales,
    COUNT(p.payment_sequential) AS numero_pagos,
    ROUND(SUM(p.payment_value)::NUMERIC, 2) AS monto_total,
    ROUND(AVG(p.payment_value)::NUMERIC, 2) AS monto_promedio,
    ROUND(AVG(p.payment_installments)::NUMERIC, 2) AS cuotas_promedio
FROM payments p
GROUP BY p.payment_type
ORDER BY ordenes_totales DESC;


-- ----------------------------------------------------------------------------
-- 7. ANÁLISIS TEMPORAL DE VENTAS (Por Mes)
-- Demuestra: DATE functions, GROUP BY temporal, ORDER BY
-- ----------------------------------------------------------------------------
SELECT 
    EXTRACT(YEAR FROM o.order_purchase_timestamp) AS año,
    EXTRACT(MONTH FROM o.order_purchase_timestamp) AS mes,
    COUNT(DISTINCT o.order_id) AS ordenes,
    COUNT(DISTINCT o.customer_id) AS clientes_unicos,
    ROUND(SUM(ss.total_payment)::NUMERIC, 2) AS revenue_total,
    ROUND(AVG(ss.total_payment)::NUMERIC, 2) AS ticket_promedio
FROM orders o
INNER JOIN sales_summary ss ON o.order_id = ss.order_id
WHERE o.order_status IN ('delivered', 'shipped')
    AND o.order_purchase_timestamp IS NOT NULL
GROUP BY año, mes
ORDER BY año DESC, mes DESC;


-- ----------------------------------------------------------------------------
-- 8. ANÁLISIS DE REVIEWS Y SATISFACCIÓN
-- Demuestra: JOIN, CASE WHEN, agregaciones condicionales
-- ----------------------------------------------------------------------------
SELECT 
    r.review_score,
    COUNT(r.review_id) AS total_reviews,
    ROUND(100.0 * COUNT(r.review_id) / SUM(COUNT(r.review_id)) OVER (), 2) AS porcentaje,
    COUNT(CASE WHEN LENGTH(r.review_comment_message) > 0 THEN 1 END) AS reviews_con_comentario,
    ROUND(AVG(EXTRACT(EPOCH FROM (r.review_answer_timestamp - r.review_creation_date))/86400)::NUMERIC, 2) AS dias_promedio_respuesta
FROM reviews r
WHERE r.review_score IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score DESC;


-- ----------------------------------------------------------------------------
-- 9. CLIENTES MÁS VALIOSOS (TOP 20)
-- Demuestra: Subquery, JOIN, ORDER BY, agregaciones
-- ----------------------------------------------------------------------------
SELECT 
    c.customer_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_ordenes,
    ROUND(SUM(ss.total_payment)::NUMERIC, 2) AS lifetime_value,
    ROUND(AVG(ss.total_payment)::NUMERIC, 2) AS ticket_promedio,
    MAX(o.order_purchase_timestamp) AS ultima_compra,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS satisfaccion_promedio
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN sales_summary ss ON o.order_id = ss.order_id
LEFT JOIN reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_id, c.customer_city, c.customer_state
HAVING COUNT(DISTINCT o.order_id) >= 2
ORDER BY lifetime_value DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- 10. ANÁLISIS DE COSTOS DE ENVÍO POR REGIÓN
-- Demuestra: JOIN, GROUP BY, agregaciones, HAVING
-- ----------------------------------------------------------------------------
SELECT 
    c.customer_state AS estado,
    COUNT(DISTINCT o.order_id) AS ordenes,
    ROUND(AVG(oi.freight_value)::NUMERIC, 2) AS costo_envio_promedio,
    ROUND(MIN(oi.freight_value)::NUMERIC, 2) AS costo_envio_minimo,
    ROUND(MAX(oi.freight_value)::NUMERIC, 2) AS costo_envio_maximo,
    ROUND(SUM(oi.freight_value)::NUMERIC, 2) AS costo_envio_total,
    ROUND(100.0 * AVG(oi.freight_value / NULLIF(oi.price, 0)), 2) AS porcentaje_envio_vs_precio
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
    AND oi.freight_value > 0
GROUP BY c.customer_state
HAVING COUNT(DISTINCT o.order_id) >= 50
ORDER BY costo_envio_promedio DESC;


-- ----------------------------------------------------------------------------
-- 11. COHORT ANALYSIS - Retención de Clientes
-- Demuestra: CTE, Window functions, DATE functions, subqueries complejas
-- ----------------------------------------------------------------------------
WITH primera_compra AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_purchase_timestamp)) AS cohort_mes
    FROM orders
    WHERE order_status = 'delivered'
    GROUP BY customer_id
),
compras_por_mes AS (
    SELECT 
        o.customer_id,
        pc.cohort_mes,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS compra_mes
    FROM orders o
    INNER JOIN primera_compra pc ON o.customer_id = pc.customer_id
    WHERE o.order_status = 'delivered'
)
SELECT 
    cohort_mes,
    COUNT(DISTINCT CASE WHEN compra_mes = cohort_mes THEN customer_id END) AS mes_0,
    COUNT(DISTINCT CASE WHEN compra_mes = cohort_mes + INTERVAL '1 month' THEN customer_id END) AS mes_1,
    COUNT(DISTINCT CASE WHEN compra_mes = cohort_mes + INTERVAL '2 months' THEN customer_id END) AS mes_2,
    COUNT(DISTINCT CASE WHEN compra_mes = cohort_mes + INTERVAL '3 months' THEN customer_id END) AS mes_3
FROM compras_por_mes
GROUP BY cohort_mes
ORDER BY cohort_mes;


-- ----------------------------------------------------------------------------
-- 12. PERFORMANCE SUMMARY - Dashboard Principal
-- Demuestra: Múltiples CTEs, agregaciones complejas
-- ----------------------------------------------------------------------------
WITH metricas_generales AS (
    SELECT 
        COUNT(DISTINCT o.order_id) AS total_ordenes,
        COUNT(DISTINCT o.customer_id) AS total_clientes,
        COUNT(DISTINCT oi.product_id) AS total_productos,
        ROUND(SUM(ss.total_payment)::NUMERIC, 2) AS revenue_total
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN sales_summary ss ON o.order_id = ss.order_id
    WHERE o.order_status = 'delivered'
),
metricas_tiempo AS (
    SELECT 
        ROUND(AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp))/86400)::NUMERIC, 2) AS avg_delivery_days
    FROM orders
    WHERE order_status = 'delivered'
        AND order_delivered_customer_date IS NOT NULL
),
metricas_satisfaccion AS (
    SELECT 
        ROUND(AVG(review_score)::NUMERIC, 2) AS avg_rating
    FROM reviews
    WHERE review_score IS NOT NULL
)
SELECT 
    mg.total_ordenes,
    mg.total_clientes,
    mg.total_productos,
    mg.revenue_total,
    ROUND(mg.revenue_total / mg.total_ordenes, 2) AS ticket_promedio,
    mt.avg_delivery_days,
    ms.avg_rating
FROM metricas_generales mg, metricas_tiempo mt, metricas_satisfaccion ms;