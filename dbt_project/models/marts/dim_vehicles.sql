{{ config(materialized='table', file_format='delta') }}

with stg_telemetry as (
    select distinct
        vehicle_id
    from {{ ref('stg_silver_telemetry') }}
)

select
    vehicle_id,
    case
        when vehicle_id like 'VH-1%' then 'TRUCK_HEAVY'
        when vehicle_id like 'VH-2%' then 'VAN_LIGHT'
        when vehicle_id like 'VH-3%' then 'TRUCK_MEDIUM'
        else 'FLEET_STANDARD'
    end as vehicle_type,
    case
        when vehicle_id like 'VH-1%' then 2.5 -- Litros consumidos por hora em marcha lenta
        when vehicle_id like 'VH-2%' then 1.4
        when vehicle_id like 'VH-3%' then 1.9
        else 2.0
    end as idle_burn_rate_liters_per_hour,
    'DIESEL S10' as fuel_type,
    current_timestamp() as dbt_updated_at
from stg_telemetry
