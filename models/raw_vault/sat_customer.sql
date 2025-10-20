{{ config(materialized='incremental', unique_key='sat_customer_pk') }}

with src as (
  select * from {{ ref('stg_customer') }}
),
sat as (  select    sha2(concat(c_custkey, coalesce(c_name,''), coalesce(c_address,'')),256) as sat_customer_pk,    sha2(concat(c_custkey),256) as hub_customer_sk,    c_name,    c_address,    c_phone,    c_acctbal,    c_mktsegment,    c_comment,    current_timestamp() as load_date,    'stg_customer' as record_source  from src)select * from sat{% if is_incremental() %}where sat_customer_pk not in (select sat_customer_pk from {{ this }}){% endif %}



