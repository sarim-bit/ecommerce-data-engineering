with raw_reviews as (
    select * from {{ source('olist_raw', 'order_reviews') }}
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
from raw_reviews