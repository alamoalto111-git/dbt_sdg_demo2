{{ config(materialized='incremental', unique_key='hub_order_hk' ) }}

with src as (  
  select * from {{ ref('stg_orders') }}
),
hub as (  
  select    
     {{ dv_hash(['o_orderkey']) }} as hub_order_hk,
     --abs(hash(o_orderkey)) as hub_order_sk,
     --sha2(concat(o_orderkey),256) as hub_order_sk,
     --md5(o_orderkey) as hub_order_skmd,     
     o_orderkey                         as business_key,    
     'ORDER'                            as entity_name,
     '{{ ref('stg_orders') }}'          as record_source,
     '{{ ref('stg_orders').name }}'     as record_source_md,
     '{{ ref('stg_orders').schema }}'   as record_source_sc,
     '{{ ref('stg_orders').database }}' as record_source_db,
     current_timestamp() as load_date    
  from src 
  group by o_orderkey)

select * from hub
{% if is_incremental() %}
where hub_order_hk not in (select hub_order_hk from {{ this }})
{% endif %}



