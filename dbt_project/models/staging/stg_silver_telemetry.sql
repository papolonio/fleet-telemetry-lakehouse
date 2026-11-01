{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'telemetry_cleaned') }}
),

renamed as (
    select
        vehicle_id,
        driver_id,
        event_timestamp,
        event_date,
        latitude,
        longitude,
        speed_kmh,
        engine_rpm,
        fuel_level_pct,
        engine_temp_c,
        odometer_km,
        status as telemetry_status,
        ignition_on,
        is_idling,
        is_overspeeding,
        _cleaned_at
    from source
)

select * from renamed
