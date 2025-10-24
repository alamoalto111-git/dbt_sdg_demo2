{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'partsupp') }}
    where ps_partkey in 
         (select l_partkey from SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.lineitem
          where l_orderkey between 4200001 and 4200034) 
)
select
  ps_partkey,
  ps_suppkey,
  ps_availqty,
  ps_supplycost,
  ps_comment
from raw