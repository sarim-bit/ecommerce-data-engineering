with raw_sellers as (
    select * from {{ source('olist_raw', 'sellers') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by seller_id order by seller_city) as row_num
    from raw_sellers
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from deduplicated
where row_num = 1