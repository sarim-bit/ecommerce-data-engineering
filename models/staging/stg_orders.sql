with raw_orders as (
    select * from {{ source('olist_raw', 'orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    -- Handle the "approval before purchase" anomaly you identified
    case 
        when order_approved_at < order_purchase_timestamp then null 
        else order_approved_at 
    end as order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
from raw_orders