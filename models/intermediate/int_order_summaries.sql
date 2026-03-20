with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select 
        order_id,
        sum(payment_value) as total_order_value
    from {{ ref('stg_order_payments') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_status,
        p.total_order_value
    from orders o
    left join payments p on o.order_id = p.order_id
)

select * from final