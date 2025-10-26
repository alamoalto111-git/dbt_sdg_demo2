{{ config(materialized='table') }}

select
  *
from {{ ref('buv_calendar') }}