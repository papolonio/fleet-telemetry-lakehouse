"""
generate_telemetry.py
---------------------
Gerador de dados sintéticos de telemetria veicular (IoT) em alta frequência.
Simula o envio de micro-lotes JSON para a Landing Zone do Lakehouse.

Inclui anomalias intencionais para testar a resiliência do pipeline:
1. Eventos de Idling (RPM elevado sem velocidade) para cálculo de desperdício.
2. Harsh Braking e Overspeeding para cálculo do Driver Safety Score.
3. Eventos corrompidos / schema drift para testar a _rescued_data_column do Auto Loader.
"""

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Configurações de Frota e Rotas (Região Metropolitana de São Paulo / Hubs)
VEHICLES = [
    {"vehicle_id": "VH-101", "type": "TRUCK_HEAVY", "fuel_type": "DIESEL", "idle_burn_rate_lph": 2.5},
    {"vehicle_id": "VH-102", "type": "TRUCK_HEAVY", "fuel_type": "DIESEL", "idle_burn_rate_lph": 2.5},
    {"vehicle_id": "VH-201", "type": "VAN_LIGHT",   "fuel_type": "DIESEL", "idle_burn_rate_lph": 1.4},
    {"vehicle_id": "VH-202", "type": "VAN_LIGHT",   "fuel_type": "DIESEL", "idle_burn_rate_lph": 1.4},
    {"vehicle_id": "VH-301", "type": "TRUCK_MEDIUM", "fuel_type": "DIESEL", "idle_burn_rate_lph": 1.9},
]

DRIVERS = ["DRV-501", "DRV-502", "DRV-503", "DRV-504", "DRV-505"]

# Coordenadas base (Hub Central - SP: -23.5505, -46.6333)
BASE_LAT = -23.5505
BASE_LON = -46.6333


def generate_telemetry_batch(
    num_records: int = 1000,
    start_time: datetime = None,
    corrupt_ratio: float = 0.02
) -> list[dict]:
    """Gera um lote de eventos de telemetria com séries temporais e anomalias controladas."""
    if start_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(hours=2)

    records = []
    current_time = start_time

    # Estado de odômetro inicial por veículo
    odometers = {v["vehicle_id"]: random.uniform(50000.0, 120000.0) for v in VEHICLES}

    for i in range(num_records):
        vehicle = random.choice(VEHICLES)
        v_id = vehicle["vehicle_id"]
        driver_id = random.choice(DRIVERS)
        current_time += timedelta(seconds=random.randint(5, 30))

        # Determinar perfil de comportamento (Normal, Idling, Harsh Driving, Corrompido)
        behavior_roll = random.random()

        if behavior_roll < corrupt_ratio:
            # Anomalia de Schema Drift / Payload corrompido para testar Auto Loader
            record = {
                "vehicle_id": v_id,
                "driver_id": driver_id,
                "event_timestamp": "TIMESTAMP_INVALIDO_CORRUPTED",
                "speed_kmh": "cem_por_hora", # Tipo incompatível (string em campo numérico)
                "unannounced_new_field_v2": "telemetry_firmware_v2.4", # Schema evolution
                "engine_rpm": None,
                "notes": "Corrupted record injected for pipeline resilience testing"
            }
        elif behavior_roll < 0.20:
            # Cenário de IDLING (Motor ligado, parado por mais de 5 minutos)
            record = {
                "vehicle_id": v_id,
                "driver_id": driver_id,
                "event_timestamp": current_time.isoformat(),
                "latitude": round(BASE_LAT + random.uniform(-0.01, 0.01), 6),
                "longitude": round(BASE_LON + random.uniform(-0.01, 0.01), 6),
                "speed_kmh": 0.0,
                "engine_rpm": random.randint(650, 850), # RPM de marcha lenta
                "fuel_level_pct": round(max(5.0, random.uniform(40.0, 80.0)), 2),
                "engine_temp_c": random.randint(85, 95),
                "odometer_km": round(odometers[v_id], 2),
                "status": "IDLING",
                "ignition_on": True
            }
        elif behavior_roll < 0.35:
            # Cenário de CONDUÇÃO AGRESSIVA (Excesso de velocidade ou frenagem brusca)
            is_overspeed = random.choice([True, False])
            speed = random.uniform(105.0, 130.0) if is_overspeed else random.uniform(40.0, 70.0)
            rpm = random.randint(2800, 3600)
            odometers[v_id] += (speed / 3600.0) * 15 # Incremento de odômetro

            record = {
                "vehicle_id": v_id,
                "driver_id": driver_id,
                "event_timestamp": current_time.isoformat(),
                "latitude": round(BASE_LAT + random.uniform(-0.15, 0.15), 6),
                "longitude": round(BASE_LON + random.uniform(-0.15, 0.15), 6),
                "speed_kmh": round(speed, 2),
                "engine_rpm": rpm,
                "fuel_level_pct": round(max(5.0, random.uniform(20.0, 75.0)), 2),
                "engine_temp_c": random.randint(95, 108), # Motor mais quente
                "odometer_km": round(odometers[v_id], 2),
                "status": "HARSH_EVENT" if is_overspeed else "HARSH_BRAKING",
                "ignition_on": True
            }
        else:
            # Condução Normal em trânsito
            speed = random.uniform(30.0, 80.0)
            odometers[v_id] += (speed / 3600.0) * 20
            record = {
                "vehicle_id": v_id,
                "driver_id": driver_id,
                "event_timestamp": current_time.isoformat(),
                "latitude": round(BASE_LAT + random.uniform(-0.10, 0.10), 6),
                "longitude": round(BASE_LON + random.uniform(-0.10, 0.10), 6),
                "speed_kmh": round(speed, 2),
                "engine_rpm": random.randint(1200, 2200),
                "fuel_level_pct": round(max(5.0, random.uniform(30.0, 90.0)), 2),
                "engine_temp_c": random.randint(80, 92),
                "odometer_km": round(odometers[v_id], 2),
                "status": "DRIVING",
                "ignition_on": True
            }

        records.append(record)

    return records


def save_telemetry_files(records: list[dict], output_dir: str, records_per_file: int = 250):
    """Grava os registros em múltiplos arquivos JSON (simulando micro-batches de streaming)."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    batch_id = int(time.time())
    for chunk_idx, i in enumerate(range(0, len(records), records_per_file)):
        chunk = records[i:i + records_per_file]
        filename = f"telemetry_batch_{batch_id}_part_{chunk_idx:03d}.json"
        file_path = out_path / filename

        # Formato NDJSON (Newline Delimited JSON) - padrão ouro para Auto Loader
        with open(file_path, "w", encoding="utf-8") as f:
            for rec in chunk:
                f.write(json.dumps(rec) + "\n")

        print(f"[Telemetria] Gerado: {file_path} ({len(chunk)} registros)")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    default_dir = project_root / "data" / "landing" / "telemetry"
    output_directory = os.getenv("OUTPUT_DIR", str(default_dir))
    print(f"Iniciando gerador de telemetria em: {output_directory}")
    events = generate_telemetry_batch(num_records=1000)
    save_telemetry_files(events, output_directory)
    print("Concluído!")
