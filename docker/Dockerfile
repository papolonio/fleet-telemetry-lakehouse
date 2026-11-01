# Dockerfile: Ambiente de Desenvolvimento & DataOps
# Inclui: Python 3.11, Databricks CLI v0.200+, dbt Core com dbt-databricks e Azure CLI

FROM python:3.11-slim

# Evitar prompts interativos
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Instalar dependências de sistema essenciais e Azure CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    unzip \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalar Databricks CLI oficial mais recente (v0.200+)
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configurar diretório de trabalho
WORKDIR /workspace

# Instalar bibliotecas Python para Engenharia de Dados & Analytics Engineering
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Entrada padrão
CMD ["/bin/bash"]
