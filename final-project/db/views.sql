-- ============================================================
-- views.sql — аналитические представления для Metabase
-- Запускать после load_history.py
-- ============================================================

-- ── 1. Ежедневная выручка и заказы ───────────────────────────────────────────
CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT
    order_date,
    COUNT(DISTINCT order_id)    AS orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(revenue)                AS revenue,
    SUM(profit)                 AS profit,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_value,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)::FLOAT /
        NULLIF(COUNT(*), 0)     AS return_rate
FROM raw_orders
GROUP BY order_date
ORDER BY order_date;


-- ── 2. Выручка и прибыль по категориям ───────────────────────────────────────
CREATE OR REPLACE VIEW v_category_metrics AS
SELECT
    category,
    subcategory,
    COUNT(DISTINCT order_id)                         AS orders,
    SUM(quantity)                                    AS units_sold,
    ROUND(SUM(revenue)::NUMERIC, 2)                  AS revenue,
    ROUND(SUM(profit)::NUMERIC,  2)                  AS profit,
    ROUND(SUM(profit) / NULLIF(SUM(revenue), 0) * 100, 2) AS margin_pct,
    ROUND(AVG(price)::NUMERIC, 2)                    AS avg_price,
    ROUND(AVG(discount_pct)::NUMERIC, 2)             AS avg_discount,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)::FLOAT /
        NULLIF(COUNT(*), 0) * 100                    AS return_rate_pct
FROM raw_orders
GROUP BY category, subcategory
ORDER BY revenue DESC;


-- ── 3. ABC-анализ товаров по выручке ─────────────────────────────────────────
CREATE OR REPLACE VIEW v_product_abc AS
WITH product_revenue AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        SUM(revenue)   AS revenue,
        SUM(profit)    AS profit,
        SUM(quantity)  AS units_sold,
        COUNT(DISTINCT order_id) AS orders
    FROM raw_orders
    WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY product_id, product_name, category, brand
),
ranked AS (
    SELECT *,
        SUM(revenue) OVER ()                          AS total_revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
    FROM product_revenue
)
SELECT
    product_id, product_name, category, brand,
    ROUND(revenue::NUMERIC, 2)       AS revenue,
    ROUND(profit::NUMERIC, 2)        AS profit,
    units_sold, orders,
    ROUND(revenue / total_revenue * 100, 2) AS revenue_share_pct,
    ROUND(cumulative_revenue / total_revenue * 100, 2) AS cumulative_share_pct,
    CASE
        WHEN cumulative_revenue / total_revenue <= 0.80 THEN 'A'
        WHEN cumulative_revenue / total_revenue <= 0.95 THEN 'B'
        ELSE 'C'
    END AS abc_class
FROM ranked
ORDER BY revenue DESC;


-- ── 4. RFM-сегментация клиентов ──────────────────────────────────────────────
CREATE OR REPLACE VIEW v_customer_rfm AS
WITH rfm_raw AS (
    SELECT
        customer_id,
        customer_name,
        customer_city,
        customer_gender,
        MAX(order_date)                   AS last_order_date,
        COUNT(DISTINCT order_id)          AS frequency,
        ROUND(SUM(revenue)::NUMERIC, 2)   AS monetary,
        ('2024-01-01'::DATE - MAX(order_date)) AS recency_days
    FROM raw_orders
    WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
      AND is_returned = FALSE
    GROUP BY customer_id, customer_name, customer_city, customer_gender
),
scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)     AS m_score
    FROM rfm_raw
)
SELECT *,
    ROUND((r_score + f_score + m_score) / 3.0, 2) AS rfm_avg,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 4 AND f_score < 3  THEN 'Recent'
        WHEN r_score < 3  AND f_score >= 4 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Potential'
    END AS rfm_segment
FROM scored
ORDER BY monetary DESC;


-- ── 5. Когортный анализ (удержание по месяцам) ───────────────────────────────
CREATE OR REPLACE VIEW v_cohort_retention AS
WITH first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date))::DATE AS cohort_month
    FROM raw_orders
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        o.customer_id,
        f.cohort_month,
        DATE_TRUNC('month', o.order_date)::DATE AS activity_month
    FROM raw_orders o
    JOIN first_order f ON o.customer_id = f.customer_id
    GROUP BY o.customer_id, f.cohort_month, DATE_TRUNC('month', o.order_date)::DATE
)
SELECT
    cohort_month,
    (DATE_PART('year', activity_month) - DATE_PART('year', cohort_month)) * 12 +
     DATE_PART('month', activity_month) - DATE_PART('month', cohort_month) AS month_number,
    COUNT(DISTINCT customer_id) AS customers
FROM monthly_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;


-- ── 6. Метрики по городам ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_city_metrics AS
SELECT
    customer_city                                AS city,
    COUNT(DISTINCT customer_id)                  AS unique_customers,
    COUNT(DISTINCT order_id)                     AS orders,
    ROUND(SUM(revenue)::NUMERIC, 2)              AS revenue,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS ltv_avg,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2)    AS aov,
    ROUND(AVG(rating)::NUMERIC, 2)               AS avg_rating,
    ROUND(SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)::NUMERIC /
          NULLIF(COUNT(*), 0) * 100, 2)          AS return_rate_pct
FROM raw_orders
GROUP BY customer_city
ORDER BY revenue DESC;


-- ── 7. Ежемесячная динамика выручки ──────────────────────────────────────────
CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    DATE_TRUNC('month', order_date)::DATE        AS month,
    EXTRACT(YEAR FROM order_date)::INT           AS year,
    EXTRACT(MONTH FROM order_date)::INT          AS month_num,
    COUNT(DISTINCT order_id)                     AS orders,
    COUNT(DISTINCT customer_id)                  AS customers,
    ROUND(SUM(revenue)::NUMERIC, 2)              AS revenue,
    ROUND(SUM(profit)::NUMERIC, 2)               AS profit,
    ROUND(AVG(revenue / NULLIF(quantity, 0))::NUMERIC, 2) AS avg_item_price,
    ROUND(SUM(CASE WHEN is_returned THEN revenue ELSE 0 END)::NUMERIC /
          NULLIF(SUM(revenue), 0) * 100, 2)      AS return_revenue_pct
FROM raw_orders
GROUP BY DATE_TRUNC('month', order_date), year, month_num
ORDER BY month;
