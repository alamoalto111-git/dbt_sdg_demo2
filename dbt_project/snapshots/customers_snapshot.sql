{% snapshot customers_snapshot %}
{{
  config(
    target_schema='raw_vault',
    target_database='tpch_ag',
    unique_key='id',
    strategy='check',
    check_cols=['name','email']
  )
}}
SELECT id, name, email, updated_at
FROM 
{{ source('esquema_snapshot2','scd2_customers') }}
{% endsnapshot %}
