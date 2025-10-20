{{ config(materialized='view') }}
with raw as (  select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER where c_custkey BETWEEN 99998 AND 100100)
select
sha2(concat(c_custkey),256) as hub_customer_sk,
md5(c_custkey) as hub_customer_skmd,  
  c_custkey::varchar as c_custkey,  
  c_name as c_name,  
  c_address as c_address,  
  c_nationkey::varchar as c_nationkey,  
  c_phone as c_phone,  
  c_acctbal::numeric as c_acctbal,  
  c_mktsegment as c_mktsegment,  
  c_comment as c_comment
from raw