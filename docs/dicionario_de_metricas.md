# Dicionário de Métricas & Camada Semântica

Este documento formaliza as regras de negócio e fórmulas analíticas que serão materializadas na camada Gold e expostas via **Databricks Metric Views**.

---

## 1. Métrica: Custo Estimado de Ociosidade (Idling Fuel Cost)
* **Objetivo:** Mensurar o prejuízo financeiro causado por veículos ligados sem deslocamento.
* **Definição de Ociosidade (Idling Event):**
  - Condição: `engine_rpm > 550` AND `speed_kmh == 0` com duração consecutiva > 5 minutos.
* **Fórmula de Cálculo:**
  - $\text{Consumo em Marcha Lenta (litros)} = \text{Duração (horas)} \times \text{Taxa Média de Consumo (ex: 2.2 L/h)}$.
  - $\text{Custo Estimado (R\$)} = \text{Litros Desperdiçados} \times \text{Preço Médio do Diesel (R\$/L)}$.
* **Dimensões de Análise:** `vehicle_id`, `driver_id`, `fleet_type`, `base_operational_hub`, `event_date`.

---

## 2. Métrica: Taxa de Aderência ao SLA de Entrega (On-Time Delivery Rate)
* **Objetivo:** Avaliar a pontualidade operacional das entregas.
* **Definição de Entrega Pontual:**
  - `actual_delivery_timestamp <= promised_delivery_sla_timestamp`.
* **Fórmula de Cálculo:**
  $$\text{SLA Compliance Rate (\%)} = \frac{\text{Total de Pedidos Entregues no Prazo}}{\text{Total de Pedidos Realizados}} \times 100$$
* **Dimensões de Análise:** `route_id`, `delivery_region`, `driver_id`, `time_window`.

---

## 3. Métrica: Índice de Risco de Condução (Driver Harsh Events Score)
* **Objetivo:** Identificar condutas de risco que geram sinistros e aceleram o desgaste de pneus, freios e suspensão.
* **Eventos Rastreados:**
  - **Frenagem Brusca:** Desaceleração $> 12\text{ km/h por segundo}$.
  - **Aceleração Brusca:** Aceleração $> 10\text{ km/h por segundo}$.
  - **Excesso de Velocidade:** `speed_kmh > speed_limit_kmh + 10\%`.
* **Fórmula de Cálculo:**
  $$\text{Score de Condução (por 100 km)} = \frac{\sum (\text{Harsh Braking} + \text{Harsh Acceleration} + \text{Speeding Events})}{\text{Total de Quilômetros Rodados}} \times 100$$
* **Classificação de Risco:**
  - **Verde (Baixo Risco):** Score $< 5$
  - **Amarelo (Atenção):** Score entre $5$ e $15$
  - **Vermelho (Alto Risco):** Score $> 15$

---

## 4. Métrica: Eficiência de Utilização dos Veículos (Fleet Utilization Rate)
* **Objetivo:** Otimizar o dimensionamento da frota (*Fleet Sizing*), identificando ativos subutilizados.
* **Fórmula de Cálculo:**
  $$\text{Taxa de Utilização (\%)} = \frac{\text{Tempo Total em Viagem Operacional (horas)}}{\text{Tempo Total Disponível em Turno (ex: 16h/dia)}} \times 100$$
* **Dimensões de Análise:** `vehicle_type`, `vehicle_id`, `hub_location`.
