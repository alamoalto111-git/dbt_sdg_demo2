{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'supplier') }}
    where s_suppkey in 
         (select l_suppkey from SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.lineitem
          where l_orderkey between 4200001 and 4200034) 
)
select
  s_suppkey,  
  s_name,             
  s_address,
  s_nationkey, 
  s_phone,  
  s_acctbal,
  s_comment
from raw