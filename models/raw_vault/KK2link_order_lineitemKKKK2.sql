{{ config(materialized='incremental', unique_key='link_order_lineitem_hk') }}

with src as (  
    select l_orderkey, l_linenumber from {{ ref('stg_lineitem') }}
),

link as (  
  select   
  
    {{ dv_hash(['l_orderkey','l_linenumber']) }} as link_order_lineitem_hk,    
    {{ dv_hash(['l_orderkey']) }}   as hub_order_hk,
    {{ dv_hash(['l_linenumber']) }} as hub_lineitem_hk,

    '{{ ref('stg_lineitem') }}'          as record_source,
    current_timestamp() as load_date    
    
  from src
    group by l_orderkey, l_linenumber)
  
 
select
  * 
from 
  link{% if is_incremental() %}
where 
  link_order_lineitem_hk not in (select link_order_lineitem_hk from {{ this }}){% endif %}
