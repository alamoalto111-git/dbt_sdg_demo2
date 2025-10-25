{{ config(materialized='table') }}

select
    order_status_code,
    order_status_desc
from {{ ref('buv_order_status') }}