{{ config(materialized='table', file_format='delta') }}

with drivers as (
    select distinct
        driver_id
    from {{ ref('stg_silver_telemetry') }}
    where driver_id is not null
)

select
    driver_id,
    concat('Motorista Operacional ', substr(driver_id, 5)) as driver_name,
    'TURNO_DIURNO' as operational_shift,
    current_timestamp() as dbt_updated_at
from drivers
