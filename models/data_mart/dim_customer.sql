{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_customer') }}
)

select
    c_custid,
    c_name,           
    c_acctbal,
    c_mktsegment,
    --c_address,
    c_nationkey,
    c_phone,
    c_comment,

    ---orders---------------------------
    total_orders,
    total_sales,
    first_order_date,
    last_order_date,
    days_since_last_order,
    years_since_last_order,

    ---accout balance--------------------
     id_dim_balance,
     desc_dim_balance,

     hub_customer_hk,
     sat_customer_pk,
     flag_current,
     load_date_hub,
     load_date_sat,
     load_date_bv,
    current_timestamp as load_date_dim
from 
  buv b
