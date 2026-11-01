{{ config(
    materialized='table',
    file_format='delta',
    liquid_clustered_by=['vehicle_id', 'trip_date']
) }}

/*
  fct_telemetry_trips
  -------------------
  Fato granular que consolida as viagens e sessões de condução dos veículos.
  Calcula diretamente as métricas de negócio definidas no dicionario_de_metricas.md:
  1. Tempo e custo de combustível desperdiçado em marcha lenta (Idling Cost).
  2. Total de infrações de condução (Harsh Events & Overspeeding).
  3. Score de risco do motorista por 100 km rodados.
*/

with telemetry as (
    select * from {{ ref('stg_silver_telemetry') }}
),

vehicles as (
    select * from {{ ref('dim_vehicles') }}
),

aggregated_sessions as (
    select
        t.vehicle_id,
        t.driver_id,
        t.event_date as trip_date,
        min(t.event_timestamp) as trip_start_at,
        max(t.event_timestamp) as trip_end_at,
        
        -- Distância percorrida baseada no odômetro
        round(max(t.odometer_km) - min(t.odometer_km), 2) as distance_traveled_km,
        
        -- Estatísticas de telemetria
        round(avg(t.speed_kmh), 2) as avg_speed_kmh,
        max(t.speed_kmh) as max_speed_kmh,
        round(avg(t.engine_rpm), 0) as avg_engine_rpm,
        round(avg(t.engine_temp_c), 1) as avg_engine_temp_c,
        
        -- Contagem de eventos de ociosidade (Idling)
        sum(case when t.is_idling then 1 else 0 end) as idling_event_ticks,
        
        -- Contagem de eventos de condução agressiva
        sum(case when t.telemetry_status = 'HARSH_BRAKING' then 1 else 0 end) as harsh_braking_events,
        sum(case when t.is_overspeeding then 1 else 0 end) as overspeeding_events
        
    from telemetry t
    group by t.vehicle_id, t.driver_id, t.event_date
),

metrics_calculated as (
    select
        -- Geração de chave substituta única para a Fato
        md5(concat(a.vehicle_id, '_', a.driver_id, '_', cast(a.trip_date as string))) as trip_surrogate_key,
        a.vehicle_id,
        a.driver_id,
        a.trip_date,
        a.trip_start_at,
        a.trip_end_at,
        a.distance_traveled_km,
        a.avg_speed_kmh,
        a.max_speed_kmh,
        a.avg_engine_rpm,
        a.avg_engine_temp_c,
        
        -- Duração estimada de Idling em minutos (cada tick simula ~15 segundos)
        round((a.idling_event_ticks * 15.0) / 60.0, 2) as idle_duration_minutes,
        
        -- Cálculo de Combustível Desperdiçado (Litros)
        -- Fórmula: Horas ociosas * Taxa de queima do veículo (L/h)
        round(
            ((a.idling_event_ticks * 15.0) / 3600.0) * coalesce(v.idle_burn_rate_liters_per_hour, 2.0),
            2
        ) as estimated_fuel_wasted_liters,
        
        -- Custo Financeiro do Desperdício (Preço médio Diesel: R$ 6.10/L)
        round(
            (((a.idling_event_ticks * 15.0) / 3600.0) * coalesce(v.idle_burn_rate_liters_per_hour, 2.0)) * 6.10,
            2
        ) as estimated_idling_cost_brl,
        
        a.harsh_braking_events,
        a.overspeeding_events,
        (a.harsh_braking_events + a.overspeeding_events) as total_harsh_events,
        
        -- Driver Harsh Events Score (por 100 km rodados)
        case
            when a.distance_traveled_km > 0 then
                round(((a.harsh_braking_events + a.overspeeding_events) / a.distance_traveled_km) * 100.0, 2)
            else 0.0
        end as driver_risk_score_per_100km,
        
        current_timestamp() as dbt_created_at
        
    from aggregated_sessions a
    left join vehicles v on a.vehicle_id = v.vehicle_id
)

select * from metrics_calculated
