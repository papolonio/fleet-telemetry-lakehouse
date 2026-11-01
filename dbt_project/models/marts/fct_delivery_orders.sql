{{ config(
    materialized='table',
    file_format='delta',
    liquid_clustered_by=['order_id', 'order_date']
) }}

/*
  fct_delivery_orders
  -------------------
  Fato granular que consolida as entregas e cruzamento com a telemetria veicular.
  Responde à Business Question 2:
  - Taxa de Aderência ao SLA de Entrega (On-Time Delivery).
  - Identificação de atrasos e correlação com motoristas e veículos.
*/

with orders as (
    select * from {{ ref('stg_silver_orders') }}
)

select
    order_id,
    customer_id,
    vehicle_id,
    driver_id,
    origin_hub_id,
    destination_hub_id,
    order_date,
    order_created_at,
    dispatched_at,
    promised_sla_at,
    actual_delivery_at,
    order_status,
    package_weight_kg,
    freight_value_brl,
    
    -- Duração real da viagem em horas
    case
        when actual_delivery_at is not null and dispatched_at is not null then
            round((unix_timestamp(actual_delivery_at) - unix_timestamp(dispatched_at)) / 3600.0, 2)
        else null
    end as transit_duration_hours,
    
    -- Atraso em minutos em relação ao SLA prometido
    case
        when actual_delivery_at is not null then
            round((unix_timestamp(actual_delivery_at) - unix_timestamp(promised_sla_at)) / 60.0, 1)
        else null
    end as sla_delay_minutes,
    
    -- Flag de violação de SLA (1 se entregou atrasado, 0 se no prazo)
    case
        when actual_delivery_at is not null and actual_delivery_at > promised_sla_at then 1
        when actual_delivery_at is not null and actual_delivery_at <= promised_sla_at then 0
        else null
    end as is_sla_breached,
    
    current_timestamp() as dbt_created_at

from orders
