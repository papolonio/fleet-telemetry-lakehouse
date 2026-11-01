"""
generate_orders.py
------------------
Gerador de dados relacionais transacionais de Entregas e Pedidos (ERP Logístico).
Simula eventos de despacho, SLA de entrega e status operacional.

Permite cruzar a telemetria com métricas de negócio:
- SLA Breach Rate (Entregas atrasadas vs no prazo).
- Correlação de atrasos com incidentes de telemetria (paradas prolongadas/quebras).
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


HUBS = [
    {"hub_id": "HUB-SP-CENTRO", "city": "São Paulo", "lat": -23.5505, "lon": -46.6333},
    {"hub_id": "HUB-SP-CAMPINAS", "city": "Campinas", "lat": -22.9056, "lon": -47.0608},
    {"hub_id": "HUB-SP-SANTOS", "city": "Santos", "lat": -23.9608, "lon": -46.3336},
    {"hub_id": "HUB-SP-SJC", "city": "São José dos Campos", "lat": -23.2237, "lon": -45.9009},
]

VEHICLE_IDS = ["VH-101", "VH-102", "VH-201", "VH-202", "VH-301"]
DRIVER_IDS = ["DRV-501", "DRV-502", "DRV-503", "DRV-504", "DRV-505"]
ORDER_STATUSES = ["DELIVERED", "IN_TRANSIT", "DELAYED", "CANCELLED"]


def generate_orders_batch(num_orders: int = 500) -> list[dict]:
    """Gera dados transacionais de ordens de entrega com SLA e timestamps correlacionados."""
    orders = []
    base_time = datetime.now(timezone.utc) - timedelta(days=2)

    for _ in range(num_orders):
        order_id = str(uuid.uuid4())
        customer_id = f"CUST-{random.randint(1000, 9999)}"
        vehicle_id = random.choice(VEHICLE_IDS)
        driver_id = random.choice(DRIVER_IDS)

        origin = random.choice(HUBS)
        dest = random.choice([h for h in HUBS if h["hub_id"] != origin["hub_id"]])

        # Timestamps de ciclo de vida do pedido
        created_at = base_time + timedelta(hours=random.randint(1, 48), minutes=random.randint(0, 59))
        dispatched_at = created_at + timedelta(minutes=random.randint(30, 180))
        sla_hours = random.choice([4, 6, 8, 12])
        promised_sla_at = dispatched_at + timedelta(hours=sla_hours)

        # 25% de probabilidade de atraso (SLA Breach) para testar a métrica de pontualidade
        is_delayed = random.random() < 0.25
        if is_delayed:
            actual_delivery_at = promised_sla_at + timedelta(minutes=random.randint(20, 240))
            status = "DELAYED"
        else:
            actual_delivery_at = promised_sla_at - timedelta(minutes=random.randint(15, 120))
            status = "DELIVERED"

        # Alguns pedidos ainda em trânsito
        if random.random() < 0.10:
            actual_delivery_at = None
            status = "IN_TRANSIT"

        package_weight = round(random.uniform(5.0, 450.0), 2)
        freight_value = round(random.uniform(45.0, 1200.0), 2)

        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "driver_id": driver_id,
            "origin_hub_id": origin["hub_id"],
            "destination_hub_id": dest["hub_id"],
            "created_at": created_at.isoformat(),
            "dispatched_at": dispatched_at.isoformat(),
            "promised_sla_at": promised_sla_at.isoformat(),
            "actual_delivery_at": actual_delivery_at.isoformat() if actual_delivery_at else None,
            "order_status": status,
            "package_weight_kg": package_weight,
            "freight_value_brl": freight_value,
            "sla_breach_flag": 1 if (actual_delivery_at and actual_delivery_at > promised_sla_at) else 0
        }
        orders.append(order)

    return orders


def save_orders_files(orders: list[dict], output_dir: str):
    """Grava as ordens em formato NDJSON na pasta de landing de pedidos."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    batch_id = int(time.time())
    file_path = out_path / f"orders_batch_{batch_id}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        for ord_rec in orders:
            f.write(json.dumps(ord_rec) + "\n")

    print(f"[Pedidos/ERP] Gerado: {file_path} ({len(orders)} ordens)")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    default_dir = project_root / "data" / "landing" / "orders"
    output_directory = os.getenv("OUTPUT_DIR", str(default_dir))
    print(f"Iniciando gerador de pedidos em: {output_directory}")
    batch = generate_orders_batch(num_orders=500)
    save_orders_files(batch, output_directory)
    print("Concluído!")
