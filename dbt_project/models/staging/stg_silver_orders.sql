{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'orders_cleaned') }}
),

renamed as (
    select
        order_id,
        customer_id,
        vehicle_id,
        driver_id,
        origin_hub_id,
        destination_hub_id,
        created_at as order_created_at,
        dispatched_at,
        promised_sla_at,
        actual_delivery_at,
        order_status,
        package_weight_kg,
        freight_value_brl,
        sla_breach_flag,
        order_date,
        _cleaned_at
    from source
)

select * from renamed
