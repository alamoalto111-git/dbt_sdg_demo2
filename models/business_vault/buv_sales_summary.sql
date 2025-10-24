{{ config(materialized='table') }}

with joined as (
    select
        o.hub_order_hk,
        c.hub_customer_hk,
        s.hub_supplier_hk,
        li.l_extendedprice * (1 - li.l_discount) as net_sales,
        li.l_SHIPDATE
    from {{ ref('sat_order') }} o
    join {{ ref('link_order_customer') }} oc on o.hub_order_hk = oc.hub_order_hk
    join {{ ref('sat_customer') }} c on c.hub_customer_hk = oc.hub_customer_hk
    join {{ ref('sat_lineitem') }} li on li.hub_order_hk = o.hub_order_hk
)

---models/business_vault/bv_sales_summary.sql

select
    customer_hk,
    sum(net_sales) as total_sales,
    min(L_SHIPDATE) as first_ship_date,
    max(L_SHIPDATE) as last_ship_date
from joined
group by customer_hk