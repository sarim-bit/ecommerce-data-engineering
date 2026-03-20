with raw_products as (
    select * from {{ source('olist_raw', 'products') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by product_id order by product_category_name) as row_num
    from raw_products
)

select
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from deduplicated
where row_num = 1