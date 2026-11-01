-- ==============================================================================
-- DDL Databricks: Criação de Metric Views no Unity Catalog
-- Camada Semântica nativa para BI, Dashboards e Databricks AI/BI (Genie)
-- ==============================================================================

USE CATALOG fleet_logistics;
USE SCHEMA gold;

-- 1. Metric View: Desperdício com Ociosidade (Idling Waste) por Frota e Veículo
CREATE OR REPLACE VIEW mv_fleet_idling_cost AS
SELECT
    v.vehicle_type,
    f.vehicle_id,
    f.trip_date,
    COUNT(f.trip_surrogate_key) AS total_trips,
    ROUND(SUM(f.distance_traveled_km), 2) AS total_distance_km,
    ROUND(SUM(f.idle_duration_minutes), 1) AS total_idle_minutes,
    ROUND(SUM(f.estimated_fuel_wasted_liters), 2) AS total_fuel_wasted_liters,
    ROUND(SUM(f.estimated_idling_cost_brl), 2) AS total_idling_cost_brl,
    ROUND(
        SUM(f.estimated_idling_cost_brl) / NULLIF(SUM(f.distance_traveled_km), 0),
        4
    ) AS idling_cost_per_km_brl
FROM fleet_logistics.gold.fct_telemetry_trips f
INNER JOIN fleet_logistics.gold.dim_vehicles v ON f.vehicle_id = v.vehicle_id
GROUP BY v.vehicle_type, f.vehicle_id, f.trip_date;

-- 2. Metric View: Aderência ao SLA de Entregas (On-Time Delivery Rate)
CREATE OR REPLACE VIEW mv_delivery_sla_compliance AS
SELECT
    origin_hub_id,
    destination_hub_id,
    order_date,
    COUNT(order_id) AS total_orders,
    SUM(CASE WHEN is_sla_breached = 0 THEN 1 ELSE 0 END) AS on_time_orders,
    SUM(CASE WHEN is_sla_breached = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
        (SUM(CASE WHEN is_sla_breached = 0 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(order_id), 0)) * 100.0,
        2
    ) AS sla_compliance_rate_pct,
    ROUND(AVG(sla_delay_minutes), 1) AS avg_delay_minutes
FROM fleet_logistics.gold.fct_delivery_orders
WHERE order_status IN ('DELIVERED', 'DELAYED')
GROUP BY origin_hub_id, destination_hub_id, order_date;

-- 3. Metric View: Ranking de Segurança do Motorista (Driver Safety Index)
CREATE OR REPLACE VIEW mv_driver_safety_leaderboard AS
SELECT
    d.driver_name,
    f.driver_id,
    ROUND(SUM(f.distance_traveled_km), 1) AS total_distance_km,
    SUM(f.harsh_braking_events) AS total_harsh_braking,
    SUM(f.overspeeding_events) AS total_overspeeding,
    SUM(f.total_harsh_events) AS total_incidents,
    ROUND(
        (SUM(f.total_harsh_events) / NULLIF(SUM(f.distance_traveled_km), 0)) * 100.0,
        2
    ) AS aggregated_risk_score_per_100km,
    CASE
        WHEN (SUM(f.total_harsh_events) / NULLIF(SUM(f.distance_traveled_km), 0)) * 100.0 < 5.0 THEN 'VERDE (Seguro)'
        WHEN (SUM(f.total_harsh_events) / NULLIF(SUM(f.distance_traveled_km), 0)) * 100.0 <= 15.0 THEN 'AMARELO (Atenção)'
        ELSE 'VERMELHO (Alto Risco)'
    end AS driver_risk_tier
FROM fleet_logistics.gold.fct_telemetry_trips f
INNER JOIN fleet_logistics.gold.dim_drivers d ON f.driver_id = d.driver_id
GROUP BY d.driver_name, f.driver_id;
