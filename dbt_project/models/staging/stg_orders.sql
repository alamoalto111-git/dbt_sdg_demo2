{{ config(materialized='view') }}
with raw as (  select * from SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.ORDERS)
select  
    o_orderkey::varchar as o_orderkey,
    o_custkey::varchar as o_custkey,
    o_orderstatus as o_orderstatus,  
    o_totalprice::numeric as o_totalprice,
    o_orderdate::date as o_orderdate,
    o_orderpriority as o_orderpriority,  
    o_clerk as o_clerk,
    o_shippriority as o_shippriority, 
    o_comment as o_comment
from raw