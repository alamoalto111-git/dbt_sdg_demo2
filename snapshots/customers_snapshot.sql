{% snapshot customers_snapshot %}
{{
  config(
    target_schema='sc_mytest',
    target_database='db_mytest',
    unique_key='id',
    strategy='check',
    check_cols=['name','email']
  )
}}
SELECT id, name, email, updated_at
FROM 
{{ source('esquema_snapshot2','scd2_customers') }}
{% endsnapshot %}
