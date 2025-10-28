{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_part') }}
)

select

---    {{ generate_surrogate_key_int(['p_part_key']) }} AS part_sk,

     p_partkey,

     p_name,
     p_mfgr,
     p_brand,
     p_type,
     p_size,
     p_container,
     p_retailprice,
     p_comment,

    ---orders---------------------------
    total_orders,
    total_sales,
  --  total_quantity,
    first_order_date,
    last_order_date,
    days_since_last_order,
    years_since_last_order,

     hub_part_hk,
    -- sat_customer_pk,
     flag_current,
     load_date_hub,
     load_date_sat,
     load_date_bv,
     current_timestamp as load_date_dim
from 
  buv b
