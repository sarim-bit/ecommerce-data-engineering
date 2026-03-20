with orders as (
    select * from {{ ref('int_order_summaries') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_metrics as (
    select
        c.customer_unique_id,
        -- Recency: Days since last order
        datediff('day', max(o.order_purchase_timestamp), current_timestamp()) as recency,
        -- Frequency: Total unique orders
        count(distinct o.order_id) as frequency,
        -- Monetary: Total spend
        sum(o.total_order_value) as monetary
    from orders o
    join customers c on o.customer_id = c.customer_id
    where o.order_status = 'delivered'
    group by 1
),

rfm_scores as (
    select
        *,
        ntile(5) over (order by recency desc) as r_score,
        ntile(5) over (order by frequency asc) as f_score,
        ntile(5) over (order by monetary asc) as m_score
    from customer_metrics
)

select 
    *,
    (r_score + f_score + m_score) as total_rfm_score,
    case 
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 4 and f_score >= 2 then 'Loyal Customers'
        when r_score <= 2 then 'At Risk'
        when r_score = 1 then 'Lost'
        else 'Promising/Recent'
    end as customer_segment
from rfm_scores