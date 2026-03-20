with raw_items as (
    select * from {{ source('olist_raw', 'order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price::numeric(10,2) as price,
    freight_value::numeric(10,2) as freight_value
from raw_items