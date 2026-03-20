with raw_customers as (
    select * from {{ source('olist_raw', 'customers') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by customer_id order by customer_unique_id) as row_num
    from raw_customers
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from deduplicated
where row_num = 1