"""
bronze_ingestion.py
-------------------
Pipeline de Ingestão Bruta (Bronze Layer) utilizando Databricks Auto Loader (cloudFiles).

Por que Auto Loader (cloudFiles)?
1. Ingestão Incremental: Processa apenas arquivos novos que chegam à Landing Zone sem
   necessidade de varreduras dispendiosas (Directory Listing) no storage da nuvem.
2. Schema Evolution: Suporta evolução automática de schema (`addNewColumns`) sem quebrar pipelines.
3. Resilient Parsing: Captura dados malformados ou divergentes na coluna `_rescued_data`,
   garantindo zero perda de dados brutos (Data Loss Prevention).
4. Custo Controlado: Emprega `trigger(availableNow=True)` para processar em micro-lotes
   e desligar o cluster após a conclusão, minimizando custos de DBUs e VM na Azure.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


def get_spark_session(app_name: str = "Bronze_AutoLoader_Ingestion") -> SparkSession:
    """Inicializa ou obtém a SparkSession ativa com suporte a Delta e Unity Catalog."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def ingest_telemetry_to_bronze(
    spark: SparkSession,
    catalog: str = "fleet_logistics",
    landing_volume_path: str = "/Volumes/fleet_logistics/landing_zone/raw_telemetry",
    checkpoint_base_path: str = "/Volumes/fleet_logistics/landing_zone/checkpoints/bronze_telemetry"
):
    """
    Ingere dados brutos de telemetria IoT via Auto Loader para a tabela Delta Bronze.
    """
    target_table = f"{catalog}.bronze.telemetry_raw"
    schema_checkpoint = f"{checkpoint_base_path}/schema"
    stream_checkpoint = f"{checkpoint_base_path}/stream"

    print(f"[Bronze] Iniciando ingestão de Telemetria:")
    print(f"  Origem (Volume): {landing_volume_path}")
    print(f"  Destino:         {target_table}")

    # Leitura em Streaming com Auto Loader
    telemetry_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_checkpoint)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Captura colunas inesperadas ou com tipos corrompidos automaticamente
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(landing_volume_path)
    )

    # Metadados de Governança e Auditoria
    bronze_telemetry = (
        telemetry_stream
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    # Gravação Append-Only na Bronze
    # availableNow=True processa todos os micro-lotes pendentes e finaliza o stream
    query = (
        bronze_telemetry.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", stream_checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    print(f"[Bronze] Ingestão de Telemetria concluída com sucesso em {target_table}!")


def ingest_orders_to_bronze(
    spark: SparkSession,
    catalog: str = "fleet_logistics",
    landing_volume_path: str = "/Volumes/fleet_logistics/landing_zone/raw_orders",
    checkpoint_base_path: str = "/Volumes/fleet_logistics/landing_zone/checkpoints/bronze_orders"
):
    """
    Ingere dados transacionais de entregas e ordens (ERP) via Auto Loader para a Bronze.
    """
    target_table = f"{catalog}.bronze.orders_raw"
    schema_checkpoint = f"{checkpoint_base_path}/schema"
    stream_checkpoint = f"{checkpoint_base_path}/stream"

    print(f"[Bronze] Iniciando ingestão de Pedidos/ERP:")
    print(f"  Origem (Volume): {landing_volume_path}")
    print(f"  Destino:         {target_table}")

    orders_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_checkpoint)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(landing_volume_path)
    )

    bronze_orders = (
        orders_stream
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    query = (
        bronze_orders.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", stream_checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    print(f"[Bronze] Ingestão de Pedidos concluída com sucesso em {target_table}!")


if __name__ == "__main__":
    spark = get_spark_session()
    catalog_name = sys.argv[1] if len(sys.argv) > 1 else "fleet_logistics"

    ingest_telemetry_to_bronze(spark, catalog=catalog_name)
    ingest_orders_to_bronze(spark, catalog=catalog_name)
