# 🚛 Fleet Telemetry Lakehouse: High-Scale IoT & Logistics Analytics

[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![dbt Core](https://img.shields.io/badge/dbt-Core%20v1.8-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![Azure](https://img.shields.io/badge/Azure-ADLS%20Gen2%20%26%20Databricks-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![DataOps](https://img.shields.io/badge/DataOps-Databricks%20Asset%20Bundles-00A4EF)](https://docs.databricks.com/dev-tools/bundles/index.html)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Liquid%20Clustering-00ADD8?logo=apachespark&logoColor=white)](https://delta.io)

Plataforma de Engenharia de Dados de ponta a ponta desenvolvida para processar e analisar telemetria de frotas veiculares (IoT em tempo quase-real) correlacionada com dados transacionais de entregas e SLAs (ERP Logístico).

Projeto estruturado seguindo as melhores práticas de **Engenharia de Software aplicadas a Dados (DataOps)**, **Arquitetura Medalhão**, **Governança Unificada no Unity Catalog**, **Liquid Clustering** e **Analytics Engineering com dbt Core**.

---

## 🎯 Problema de Negócio & Perguntas Respondidas

Frotas logísticas modernas perdem milhões anualmente com ineficiências invisíveis. Este Lakehouse responde diretamente a 4 dores de negócio:

1. **Desperdício Financeiro com Marcha Lenta (Idling Fuel Waste):**
   * Mensura litros de diesel e valor em R$ queimados por caminhões parados com o motor ligado (`rpm > 650` e `speed == 0`), estratificado por rota e modelo de veículo.
2. **Aderência ao SLA de Entrega (On-Time Delivery Rate):**
   * Identifica gargalos na "última milha" (*last-mile*), correlacionando anomalias de telemetria com atrasos contratuais de pedidos.
3. **Índice de Condução Agressiva (Driver Safety & Wear Index):**
   * Normaliza eventos de frenagem brusca, aceleração repentina e excesso de velocidade por 100 km rodados, gerando um score de risco para manutenção preventiva.
4. **Eficiência de Ativos Móveis (Fleet Utilization Rate):**
   * Compara tempo útil em trânsito versus ociosidade em pátios.

---

## 🏗️ Desenho da Arquitetura

```
[IoT Telemetry NDJSON]       [ERP Orders NDJSON]
          │                           │
          ▼                           ▼
┌─────────────────────────────────────────────────────────┐
│     Azure ADLS Gen2: Unity Catalog Landing Volumes      │
└────────────────────────────┬────────────────────────────┘
                             │
            (Databricks Auto Loader: cloudFiles)
                             ▼
┌─────────────────────────────────────────────────────────┐
│  BRONZE LAYER: delta.fleet_logistics.bronze.*           │
│  - Append-only com preservação de schema original       │
│  - Rescued Data Column (_rescued_data) contra corrupção │
└────────────────────────────┬────────────────────────────┘
                             │
              (PySpark Cleaning & Validation)
                             ▼
┌─────────────────────────────────────────────────────────┐
│  SILVER LAYER: delta.fleet_logistics.silver.*           │
│  - Deduplicação e cast tipado UTC                       │
│  - Feature flags (is_idling, is_overspeeding)           │
│  - Liquid Clustering: CLUSTER BY (vehicle_id, timestamp)│
└────────────────────────────┬────────────────────────────┘
                             │
     (dbt Core via Databricks Serverless SQL Warehouse)
                             ▼
┌─────────────────────────────────────────────────────────┐
│  GOLD LAYER: Modelo Estrela (Kimball)                   │
│  - Dimensões: dim_vehicles, dim_drivers, dim_routes     │
│  - Fatos: fct_telemetry_trips, fct_delivery_orders      │
│  - Liquid Clustering nativo no dbt-databricks           │
│  - Testes de integridade (unique, not_null, ref)        │
└────────────────────────────┬────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│  CAMADA SEMÂNTICA: Databricks Metric Views              │
│  - mv_fleet_idling_cost                                 │
│  - mv_delivery_sla_compliance                           │
│  - mv_driver_safety_leaderboard                         │
│  -> Consumo unificado via BI, Dashboards e AI/BI Genie  │
└─────────────────────────────────────────────────────────┘
```

---

## ⚡ Diferenciais Técnicos e "Os Porquês"

### 1. Liquid Clustering vs. Particionamento Tradicional
* **O problema do particionamento clássico:** Particionar por `date` e `vehicle_id` gera o problema de *small files* (milhares de pastas quase vazias). Particionar apenas por `date` inviabiliza consultas rápidas por veículo.
* **A solução:** Usamos `CLUSTER BY (vehicle_id, event_timestamp)`. O Delta Lake organiza os dados usando curvas de Hilbert multidimensionais, garantindo *Data Skipping* instantâneo tanto para filtros temporais quanto por veículo, com reclusterização incremental via `OPTIMIZE`.

### 2. Auto Loader (`cloudFiles`) com Proteção de Dados
* Ingestão incremental sem necessidade de varredura completa de diretórios.
* Payload com tipos corrompidos ou novas colunas não derrubam o pipeline: dados anômalos são desviados para `_rescued_data`.
* `trigger(availableNow=True)` processa todos os micro-lotes acumulados e desliga a computação, respeitando limites de custos e quotas (Azure for Students / Free Tier).

### 3. Analytics Engineering com dbt Core
* Transformação da Gold executada no **Databricks SQL Serverless (2X-Small)**.
* Modelos dimensionais organizados em `staging`, `intermediate` e `marts`.
* Suporte nativo a Liquid Clustering via dbt model config:
  ```sql
  {{ config(
      materialized='table',
      file_format='delta',
      liquid_clustered_by=['vehicle_id', 'trip_date']
  ) }}
  ```
* Testes automatizados de qualidade de dados (`dbt test`) integrados ao CI/CD.

### 4. DataOps via Databricks Asset Bundles (DABs)
* Infraestrutura e orquestração de Workflows declarados em YAML (`databricks.yml`).
* Pipeline multi-tarefa encadeado: Ingestão Bronze ➔ Limpeza Silver ➔ Transformação dbt ➔ Publicação de Metric Views.
* Esteira de CI/CD via **GitHub Actions** com linting (`ruff`), validação do bundle e deploy automático.

---

## 📁 Estrutura do Repositório

```text
├── .github/
│   └── workflows/
│       └── ci_cd.yml                 # Pipeline GitHub Actions (Lint, Bundle Validate, Deploy)
├── databricks.yml                    # Databricks Asset Bundle (DAB) manifest
├── dbt_project/                      # Projeto dbt Core
│   ├── dbt_project.yml               # Configuração do projeto dbt
│   ├── packages.yml                  # Dependências (dbt_utils)
│   ├── profiles.yml.example          # Template de conexão Databricks SQL Warehouse
│   └── models/
│       ├── staging/                  # Views sobre as tabelas Silver
│       │   ├── _staging__sources.yml
│       │   ├── stg_silver_telemetry.sql
│       │   └── stg_silver_orders.sql
│       └── marts/                    # Modelo Dimensional Gold
│           ├── _marts__models.yml    # Documentação e Testes (unique, not_null, relationships)
│           ├── dim_vehicles.sql      # Dimensão de veículos e taxas de consumo
│           ├── dim_drivers.sql       # Dimensão de motoristas
│           ├── fct_telemetry_trips.sql   # Fato de viagens, ociosidade e infrações
│           └── fct_delivery_orders.sql  # Fato de entregas e SLA
├── docker/
│   ├── Dockerfile                    # Container com Databricks CLI v0.200+, dbt, Python 3.11
│   ├── docker-compose.yml
│   └── requirements.txt
├── docs/                             # Documentos de Contexto e Arquitetura
│   ├── arquitetura_e_decisoes.md     # Racional detalhado da solução
│   ├── dicionario_de_metricas.md     # Definição e fórmulas de negócio
│   └── status_do_projeto.md          # Roadmap e status das fases
├── infra/
│   └── azure/
│       └── setup_azure.sh            # Provisionador Azure CLI (ADLS Gen2, Access Connector, Workspace)
└── src/
    ├── generator/                    # Geradores sintéticos multisserviço
    │   ├── generate_telemetry.py     # Micro-lotes IoT com anomalias de idling e schema drift
    │   └── generate_orders.py        # ERP transacional com SLAs de entrega
    └── pipelines/
        ├── bronze_ingestion.py       # Auto Loader com _rescued_data
        ├── silver_cleaning.py        # Limpeza, Deduplicação e Liquid Clustering
        └── create_metric_views.sql   # DDL das Metric Views do Unity Catalog
```

---

## 🚀 Como Executar o Projeto

### 1. Provisionar Infraestrutura na Azure
Execute o script no Azure Cloud Shell ou Azure CLI:
```bash
chmod +x infra/azure/setup_azure.sh
./infra/azure/setup_azure.sh
```

### 2. Gerar Dados Sintéticos Locais
```bash
python3 src/generator/generate_telemetry.py
python3 src/generator/generate_orders.py
```

### 3. Validar e Fazer Deploy do Bundle (DABs)
```bash
# Validar sintaxe e integridade do bundle
databricks bundle validate -t dev

# Fazer deploy do pipeline completo no Databricks
databricks bundle deploy -t dev

# Executar o Workflow ponta a ponta
databricks bundle run fleet_end_to_end_pipeline -t dev
```

---

## 👤 Autor & Propósito
Desenvolvido por **Pedro Apolonio** como vitrine técnica de engenharia de dados moderna no ecossistema Databricks, provando domínio em DataOps, Analytics Engineering e resolução de problemas analíticos de alta escala.
