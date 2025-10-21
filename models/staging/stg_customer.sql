{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'customer') }}
    ---SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.CUSTOMER
    where c_custkey BETWEEN 99998 AND 100100
)
select
 
  c_custkey,--::varchar as c_custkey,  
  c_name,             ---  as c_name,  
  c_address, --as c_address,  
  c_nationkey::varchar as c_nationkey,  
  c_phone as c_phone,  
  c_acctbal::numeric as c_acctbal,  
  c_mktsegment,  
  c_comment
from raw