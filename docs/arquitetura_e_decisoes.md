# Arquitetura & Decisões Técnicas: Fleet Telemetry Lakehouse

## 1. Visão Geral do Negócio
O objetivo desta plataforma de dados é processar telemetria veicular em tempo quase real cruzada com dados operacionais de ERP (pedidos e entregas), fornecendo inteligência sobre:
- Custo de combustível por ociosidade (*idling cost*).
- Aderência de SLA na última milha (*last-mile delivery*).
- Desgaste mecânico e segurança na condução (*harsh braking, overspeeding*).
- Eficiência global da frota móvel (OEE).

---

## 2. Decisões Arquiteturais e Justificativas ("Os Porquês")

### 2.1 Nuvem e Infraestrutura Base (Azure)
- **Azure Databricks Workspace (Tier Premium):**
  - *Decisão:* Provisionar workspace Premium integrado ao Unity Catalog.
  - *Por quê:* O Unity Catalog exige o tier Premium no Azure. Garante segurança corporativa, governança unificada de dados e IA, e linhagem automática de ponta a ponta.
- **Armazenamento Seguro (ADLS Gen2 + Managed Identity):**
  - *Decisão:* Conexão via *Access Connector for Azure Databricks* sem chaves de acesso no código.
  - *Por quê:* Padrão zero-trust. Elimina o risco de vazamento de credenciais no Git e dispensa `dbutils.fs.mount` (método legado e depreciado).

### 2.2 Camada de Governança (Unity Catalog)
Estrutura de 3 níveis: `catalog.schema.table`
- **Catálogo:** `fleet_logistics`
- **Schemas:**
  - `bronze`: Dados brutos append-only (`cloudFiles`), preservando schema original e dados malformados em `_rescued_data`.
  - `silver`: Dados higienizados, tipados, enriquecidos espacialmente e deduplicados.
  - `gold`: Modelos dimensionais (Kimball: fatos e dimensões) e Metric Views prontas para consumo de BI e IA.
  - `landing_zone` (Volumes): Pastas gerenciadas no Unity Catalog para ingestão dos JSONs/Parquets simulados.

### 2.3 Performance: Liquid Clustering
- **Substituição de particionamento tradicional:**
  - *Decisão:* Empregar `CLUSTER BY (vehicle_id, event_timestamp)` na Silver e nas tabelas Fato da Gold.
  - *Por quê:* 
    1. O particionamento convencional (`date` + `vehicle_id`) causa o problema crônico de *small files* (milhares de diretórios quase vazios) ou *skew* de dados.
    2. O Liquid Clustering reorganiza dados incrementalmente sem reescrever partições inteiras e adapta-se automaticamente a padrões de consulta com múltiplos filtros sem necessidade de tuning manual de Z-Order.

### 2.4 Ingestão: Databricks Auto Loader (`cloudFiles`)
- *Decisão:* Ingestão de arquivos brutos via Auto Loader na Bronze.
- *Por quê:* Otimizado para alto volume de pequenos arquivos com evolução automática de esquema (`schemaEvolutionMode: addNewColumns`) e baixo custo computacional (evita chamadas excessivas de listagem de diretórios na nuvem).

### 2.5 Transformação e Modelagem: dbt Core (`dbt-databricks`)
- *Decisão:* dbt Core orquestrando as transformações da camada Gold contra o Databricks SQL Warehouse Serverless (2X-Small).
- *Por quê:* Separa a responsabilidade de engenharia pesada de ingestão (PySpark) da engenharia analítica (Analytics Engineering). Garante testes automatizados (`not null`, `unique`, testes de integridade referencial), documentação como código e linhagem reprodutível.

### 2.6 DataOps & CI/CD: Databricks Asset Bundles (DABs)
- *Decisão:* Todo o deploy de jobs, pipelines e configurações versionado em YAML via DABs.
- *Por quê:* Padrão moderno da Databricks para Infraestrutura como Código (IaC), permitindo alternar de forma transparente entre targets `dev` e `prod`.

### 2.7 Padrão de Versionamento Git (Conventional Commits em Português)
- *Padrão adotado:* Todas as mensagens de commit devem seguir o padrão Conventional Commits redigidas estritamente em **português**:
  - `feat: <nova funcionalidade ou pipeline>`
  - `fix: <correção de bug ou ajuste de regra>`
  - `docs: <documentação ou dicionários de métricas>`
  - `refactor: <refatoração de código sem alteração de regra de negócio>`
  - `ci: <ajustes em pipelines de CI/CD ou DABs>`

---

## 3. Topologia do Fluxo de Dados
```
[Gerador Sintético Docker]
        │ (JSON telemetria & Parquet ERP)
        ▼
[Unity Catalog Volume: /landing_zone]
        │
   (Auto Loader)
        ▼
[Bronze Tables (Delta)]
        │
   (PySpark Cleansing & Deduplication)
        ▼
[Silver Tables (Delta + Liquid Clustering)]
        │
   (dbt Core via Databricks SQL Serverless)
        ▼
[Gold Star Schema (Dims & Facts)]
        │
   (Databricks Metric Views)
        ▼
[BI & Analytics (Genie / Power BI)]
```
