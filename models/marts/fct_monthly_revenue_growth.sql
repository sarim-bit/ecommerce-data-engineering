with monthly_revenue as (
    select
        date_trunc('month', order_purchase_timestamp) as order_month,
        sum(total_order_value) as revenue
    from {{ ref('int_order_summaries') }}
    where order_status = 'delivered'
    group by 1
),

growth_calc as (
    select
        order_month,
        revenue,
        lag(revenue) over (order by order_month) as previous_month_revenue
    from monthly_revenue
)

select
    order_month,
    revenue,
    previous_month_revenue,
    round(((revenue - previous_month_revenue) / previous_month_revenue) * 100, 2) as mom_growth_pct
from growth_calc