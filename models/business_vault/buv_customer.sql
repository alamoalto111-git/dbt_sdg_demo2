{{ config(materialized='table') }}

with hub as (
    select
        hub_customer_hk,
        business_key,
        load_date
    from {{ ref('hub_customer') }}
),

sat as (
    select
      s.hub_customer_hk,
      sat_customer_pk,
      c_name,
      c_acctbal,
      c_mktsegment,
      c_address,
      c_nationkey,
      c_phone,
      c_comment,
      true as flag_current,
      s.load_date
    from 
      {{ ref('sat_customer') }} s--,
      --hub h
    --where h.hub_customer_hk = s.hub_customer_hk
    qualify row_number() over (partition by s.hub_customer_hk order by s.load_date desc) = 1
),

orders as (
    select
        l.hub_customer_hk,
        count(distinct o.hub_order_hk) as total_orders,
        sum(o.o_totalprice)            as total_sales,
        min(o.o_orderdate)             as first_order_date,
        max(o.o_orderdate)             as last_order_date,
        datediff(day,  max(o.o_orderdate), current_date) as days_since_last_order,
        datediff(year, max(o.o_orderdate), current_date) as years_since_last_order
    from 
       {{ ref('link_order_customer') }} l,
       (select * 
        from 
          {{ ref('sat_order') }} o
        qualify row_number() over (partition by hub_order_hk order by load_date desc) = 1
        ) o
    where l.hub_order_hk = o.hub_order_hk   
    group by
       l.hub_customer_hk
)

select
    h.business_key as c_custid,
    s.c_name,           
    s.c_acctbal,
    s.c_mktsegment,
    s.c_address,
    s.c_nationkey,
    s.c_phone,
    s.c_comment,

    ---orders---------------------------
    o.total_orders,
    o.total_sales,
    o.first_order_date,
    o.last_order_date,
    o.days_since_last_order,
    o.years_since_last_order,

    ---accout balance--------------------
    CASE
        WHEN s.C_ACCTBAL < 0 THEN 'NEGATIVE'
        WHEN s.C_ACCTBAL = 0 THEN 'ZERO'
        WHEN s.C_ACCTBAL BETWEEN 1 AND 1000 THEN 'LOW'
        WHEN s.C_ACCTBAL BETWEEN 1001 AND 5000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS id_dim_balance,

    CASE
        WHEN C_ACCTBAL < 0 THEN 'Negative Balance'
        WHEN C_ACCTBAL = 0 THEN 'Zero Balance'
        WHEN C_ACCTBAL BETWEEN 1 AND 1000  THEN 'Balance below 1000'
        WHEN C_ACCTBAL BETWEEN 1001 AND 5000 THEN 'Balance between 1000 and 5000'
        ELSE 'Balance over 5000'
    END AS desc_dim_balance,

    h.hub_customer_hk,
    s.sat_customer_pk,
    s.flag_current,
    h.load_date       as load_date_hub,
    s.load_date       as load_date_sat,
    current_timestamp as load_date_bv
from 
  hub h
left join sat s
  on h.hub_customer_hk = s.hub_customer_hk
left join orders o
  on h.hub_customer_hk = o.hub_customer_hk  