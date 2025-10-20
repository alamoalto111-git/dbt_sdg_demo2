{{ config(materialized='incremental', unique_key='hub_customer_sk' ) }}

with src as (  
  select * from {{ ref('stg_customer') }}
--alejandra si funciona ---with src as (  select * from TPCH_AG.RAW_VAULT.stg_customer
---antes errorTPCH_AG.tpch_sf01_RAW_VAULT.stg_customer
),
hub as (  
  select    
     -- clave hash del negocio (business key hash)    
     sha2(concat(c_custkey),256) as hub_customer_sk,    
     c_custkey           as business_key,    
     'CUSTOMER'          as entity_name,    
     current_timestamp() as load_date,    
     'stg_customer'      as record_source  
  from src 
  group by c_custkey)

select * from hub
{% if is_incremental() %}
where hub_customer_sk not in (select hub_customer_sk from {{ this }})
{% endif %}



