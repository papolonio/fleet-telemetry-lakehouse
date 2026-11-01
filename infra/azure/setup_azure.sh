#!/usr/bin/env bash
# ==============================================================================
# Script de Provisionamento de Infraestrutura: Azure Databricks + Unity Catalog
# Otimizado para: Conta de Estudante (Azure for Students - Quotas & Custos Reduzidos)
# ==============================================================================

set -euo pipefail

# Garantir que o binário do az CLI local esteja no PATH
export PATH="$HOME/.local/bin:$PATH"

# 1. Variáveis de Configuração
# O Azure for Students possui a política 'sys.regionrestriction' limitando regiões.
# 'northcentralus' é a região permitida com suporte completo a Databricks, Storage e Access Connector.
LOCATION="northcentralus"
RANDOM_SUFFIX=$((RANDOM % 9000 + 1000))
RESOURCE_GROUP="rg-fleet-lakehouse-dev"
STORAGE_ACCOUNT="stfleetdata${RANDOM_SUFFIX}" # Precisa ser globalmente único (letras minúsculas e números)
CONTAINER_METASTORE="unity-catalog-metastore"
CONTAINER_LANDING="landing-zone"
ACCESS_CONNECTOR="id-databricks-access-connector"
WORKSPACE_NAME="dbw-fleet-lakehouse-dev"

echo "======================================================================"
echo "Iniciando provisionamento para o projeto Fleet Telemetry Lakehouse..."
echo "Região: ${LOCATION}"
echo "Resource Group: ${RESOURCE_GROUP}"
echo "Storage Account (ADLS Gen2): ${STORAGE_ACCOUNT}"
echo "Workspace Databricks: ${WORKSPACE_NAME}"
echo "======================================================================"

# 2. Criar Resource Group
echo "--> 1/6 Criando Resource Group..."
az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --tags Environment=Dev Project=FleetTelemetry CostCenter=StudentTrial

# 3. Criar Storage Account ADLS Gen2 (Hierarchical Namespace OBRIGATÓRIO para Databricks/Unity Catalog)
# Usando Standard_LRS para consumo mínimo de créditos
echo "--> 2/6 Criando Storage Account ADLS Gen2 (HNS Habilitado)..."
az storage account create \
  --name "${STORAGE_ACCOUNT}" \
  --resource-group "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --allow-blob-public-access false \
  --min-tls-version TLS1_2

# 4. Criar Containers (Metastore do Unity Catalog e Landing Zone para os dados brutos)
echo "--> 3/6 Criando Containers no ADLS Gen2..."
STORAGE_KEY=$(az storage account keys list --resource-group "${RESOURCE_GROUP}" --account-name "${STORAGE_ACCOUNT}" --query '[0].value' -o tsv)

az storage container create \
  --name "${CONTAINER_METASTORE}" \
  --account-name "${STORAGE_ACCOUNT}" \
  --account-key "${STORAGE_KEY}"

az storage container create \
  --name "${CONTAINER_LANDING}" \
  --account-name "${STORAGE_ACCOUNT}" \
  --account-key "${STORAGE_KEY}"

# 5. Criar Access Connector for Azure Databricks (Managed Identity para acesso Zero-Trust)
echo "--> 4/6 Criando Access Connector for Azure Databricks..."
az databricks access-connector create \
  --name "${ACCESS_CONNECTOR}" \
  --resource-group "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --identity-type SystemAssigned

CONNECTOR_ID=$(az databricks access-connector show \
  --name "${ACCESS_CONNECTOR}" \
  --resource-group "${RESOURCE_GROUP}" \
  --query 'identity.principalId' -o tsv)

CONNECTOR_RES_ID=$(az databricks access-connector show \
  --name "${ACCESS_CONNECTOR}" \
  --resource-group "${RESOURCE_GROUP}" \
  --query 'id' -o tsv)

# 6. Atribuir Permissão de 'Storage Blob Data Contributor' ao Access Connector
# Isso dá acesso ao Unity Catalog sem precisar expor senhas/chaves
echo "--> 5/6 Atribuindo permissão RBAC ao Access Connector no Storage Account..."
STORAGE_RES_ID=$(az storage account show --name "${STORAGE_ACCOUNT}" --resource-group "${RESOURCE_GROUP}" --query 'id' -o tsv)

az role assignment create \
  --assignee-object-id "${CONNECTOR_ID}" \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "${STORAGE_RES_ID}"

# 7. Criar Workspace do Azure Databricks (Tier Premium é obrigatório para Unity Catalog)
# Nota: Databricks concede 14 dias de Trial gratuito dos DBUs de computação.
echo "--> 6/6 Provisionando Azure Databricks Workspace (Tier: Premium)..."
az databricks workspace create \
  --name "${WORKSPACE_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --location "${LOCATION}" \
  --sku premium

WORKSPACE_URL=$(az databricks workspace show \
  --name "${WORKSPACE_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --query 'workspaceUrl' -o tsv)

echo ""
echo "======================================================================"
echo "✅ Provisionamento Concluído com Sucesso!"
echo "======================================================================"
echo "Guarde os dados abaixo para configurarmos o Unity Catalog:"
echo "- Databricks Workspace URL: https://${WORKSPACE_URL}"
echo "- Storage Account Name:     ${STORAGE_ACCOUNT}"
echo "- Metastore ADLS Path:      abfss://${CONTAINER_METASTORE}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
echo "- Landing Zone ADLS Path:   abfss://${CONTAINER_LANDING}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
echo "- Access Connector Res ID:  ${CONNECTOR_RES_ID}"
echo "======================================================================"
