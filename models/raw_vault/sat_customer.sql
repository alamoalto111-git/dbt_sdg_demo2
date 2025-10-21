{{ config(materialized='incremental', unique_key='sat_customer_pk') }}

with src as (
  select * from {{ ref('stg_customer') }}
),
sat as (
  select 
     --sha2(concat(c_custkey, coalesce(c_name,''), 
     --coalesce(c_address,'')),256) as sat_customer_pk,    
     --sha2(concat(c_custkey),256) as hub_customer_sk,
      abs(hash(c_custkey)) as hub_customer_sk,
      --coalesce(c_address,'')),256) as sat_customer_pk,

      sha2(
            concat_ws(
                '||',
                coalesce(c_name,''),
                coalesce(c_address,'')
               -- coalesce(c_nationkey::string,'')
               -- coalesce(c_phone,''),
                --coalesce(c_acctbal::string,''),
                --coalesce(c_mktsegment,''),
                --coalesce(c_comment,'')
            ),256
        ) as sat_customer_pk, --hashdiff,
        
      c_name,    
      c_address,
      c_nationkey,    
      c_phone,    
      c_acctbal,    
      c_mktsegment,    
      c_comment, 
      --'stg_customer' as record_source, 
     '{{ ref('stg_customer') }}'          as record_source,
     '{{ ref('stg_customer').name }}'     as record_source_md,
     '{{ ref('stg_customer').schema }}'   as record_source_sc,
     '{{ ref('stg_customer').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_customer_pk not in (select sat_customer_pk from {{ this }})
{% endif %} 



