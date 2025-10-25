{{ config(materialized='table') }}

with base as (
    select
        -- Hash keys
        lnk_ol.link_order_lineitem_hk,
        lnk_ol.hub_order_hk,
        --lnk_ol.hub_lineitem_hk,
        lnk_oc.hub_customer_hk,

--Claves origen
hub_o.business_key as CLAVE_ORDER,
sat_li.l_linenumber as LINENUMBER,
hub_p.business_key as CLAVE_PART,

        --sat_li.l_partkey,
        --sat_li.l_suppkey,
        
        -- Fechas y status
        sat_o.o_orderdate,
        sat_o.o_orderstatus,

        sat_li.l_shipdate,
        sat_li.l_commitdate,
        sat_li.l_receiptdate,
        sat_li.l_returnflag,
        sat_li.l_linestatus,

        -- Métricas económicas
        sat_p.p_retailprice ALE_p_retailprice,

        sat_li.l_quantity,
        sat_li.l_extendedprice,
        sat_li.l_discount,
        sat_li.l_tax,
        (sat_li.l_extendedprice * (1 - sat_li.l_discount)) as net_sales,
        (sat_li.l_extendedprice * (1 - sat_li.l_discount) * (1 + sat_li.l_tax)) as gross_sales,

        -- Comentarios y descripción
        sat_li.l_comment,
        sat_c.c_name as customer_name,
        sat_s.s_name as supplier_name,
        sat_p.p_name as part_name,
        sat_p.p_brand,
        sat_p.p_type,
        sat_p.p_container
    from 
         {{ ref('link_order_lineitem') }} lnk_ol

    join {{ ref('sat_lineitem') }} sat_li
        on lnk_ol.link_order_lineitem_hk = sat_li.link_order_lineitem_hk
    
    join {{ ref('sat_order') }} sat_o
        on lnk_ol.hub_order_hk = sat_o.hub_order_hk
    
    join {{ ref('link_order_customer') }} lnk_oc
        on lnk_oc.hub_order_hk = lnk_ol.hub_order_hk
    
    join {{ ref('sat_customer') }} sat_c
        on sat_c.hub_customer_hk = lnk_oc.hub_customer_hk
    
    left join {{ ref('sat_supplier') }} sat_s
        on sat_s.hub_supplier_hk = lnk_ol.hub_supplier_hk   --sat_li.hub_supplier_hk
    
    left join {{ ref('sat_part') }} sat_p
        on sat_p.hub_part_hk = lnk_ol.hub_part_hk   --sat_li.hub_part_hk

   left join {{ ref('hub_part') }} hub_p
     on lnk_ol.hub_part_hk = hub_p.hub_part_hk

   left join {{ ref('hub_order') }} hub_o
     on lnk_ol.hub_order_hk = hub_o.hub_order_hk

)

select * from base