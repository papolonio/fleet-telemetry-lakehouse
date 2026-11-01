# Status do Projeto & Próximos Passos (Checkpoints)

## 📌 Status Atual: FASE 1 - Setup de Ambiente & Estrutura Base

| Fase | Descrição | Status | Detalhes |
|---|---|---|---|
| **Fase 1** | Arquitetura, Discovery e Blueprint | **Concluída** | Decisões técnicas, métricas e estrutura do repositório definidas. |
| **Fase 2** | Setup do Databricks na Azure & Unity Catalog | **Script Pronto** | Script `setup_azure.sh` criado. Pronto para execução no Azure Cloud Shell. |
| **Fase 3** | Ambiente Docker Local & Gerador de Dados | **Concluída** | Dockerfile, docker-compose e geradores de dados testados gerando micro-batches. |
| **Fase 4** | Ingestão Bronze (Auto Loader) & Limpeza Silver (PySpark) | **Concluída** | `bronze_ingestion.py` (Auto Loader + `_rescued_data`) e `silver_cleaning.py` (Liquid Clustering + Upsert) implementados e compilados. |
| **Fase 5** | Modelagem Dimensional Gold (dbt Core) & Testes | **Concluída** | Projeto dbt configurado com modelo Kimball (`dim_vehicles`, `dim_drivers`, `fct_telemetry_trips`, `fct_delivery_orders`), Liquid Clustering no dbt e testes de integridade referencial. |
| **Fase 6** | Databricks Metric Views & Camada Semântica | **Concluída** | Script DDL `create_metric_views.sql` com as 3 Metric Views analíticas para BI/Genie. |
| **Fase 7** | DataOps com Databricks Asset Bundles (DABs) & CI/CD | **Concluída** | `databricks.yml` com pipeline multi-tarefa (Bronze -> Silver -> dbt -> Metric Views) e workflow de CI/CD via GitHub Actions `.github/workflows/ci_cd.yml`. |

---

## 🎯 Próximo Checkpoint / Ações Práticas:
1. [ ] Criar repositório no seu GitHub pessoal e fazer o `git push origin main`.
2. [ ] Executar o script `setup_azure.sh` no Azure Cloud Shell para subir o workspace e storage.
3. [ ] Conectar o Databricks CLI e executar o deploy do bundle: `databricks bundle deploy -t dev`.
| **Fase 4** | Ingestão Bronze (Auto Loader) & Limpeza Silver (PySpark) | Não iniciado | Notebooks/scripts com Liquid Clustering e tratamento de schema. |
| **Fase 5** | Modelagem Dimensional Gold (dbt Core) & Testes | Não iniciado | Projeto dbt, modelos dimensionais (Kimball), testes de qualidade. |
| **Fase 6** | Databricks Metric Views & Camada Semântica | Não iniciado | DDL de Metric Views para consumo unificado de BI/Genie. |
| **Fase 7** | DataOps com Databricks Asset Bundles (DABs) & CI/CD | Não iniciado | `databricks.yml`, pipeline de deploy automatizado via GitHub Actions. |

---

## 🎯 To-Do Imediato (Próximos Passos):
1. [ ] Estruturar o repositório Git local com os padrões de projeto (`src/`, `dbt/`, `bundles/`, `docker/`, `docs/`).
2. [ ] Configuração do Ambiente Azure & Unity Catalog (passo a passo para o usuário).
3. [ ] Criar o `Dockerfile` e `docker-compose.yml` para o ambiente de desenvolvimento isolado (Databricks CLI + dbt + Python).
4. [ ] Desenvolver o gerador sintético de dados multisserviço com anomalias propositais.
