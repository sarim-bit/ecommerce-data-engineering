with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_state,
        c.customer_city,
        'Brazil' as customer_country,  -- Hardcoded as Olist is a Brazilian marketplace
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        -- Calculate actual delivery time
        datediff('day', o.order_purchase_timestamp, o.order_delivered_customer_date) as actual_delivery_time,
        -- Calculate how much earlier/later than estimate
        datediff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date) as delivery_delay,
        case 
            when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1 
            else 0 
        end as is_late_delivery
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final