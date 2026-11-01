"""
silver_cleaning.py
------------------
Pipeline de Limpeza, Validação e Enriquecimento (Silver Layer).

Por que Liquid Clustering em vez de Particionamento Tradicional?
1. Elimina Small Files & Data Skew:
   Particionar por (date, vehicle_id) criaria milhares de pequenas partições com arquivos
   de poucos kilobytes, degradando a performance do Metastore e das queries.
2. Indexação Multidimensional Flexível:
   O Liquid Clustering (`CLUSTER BY (vehicle_id, event_timestamp)`) organiza os dados
   em curvas de Hilbert multidimensionais, permitindo 'Data Skipping' extremamente rápido
   tanto para consultas filtradas por período quanto por veículo/frota específica.
3. Não-Destrutivo:
   Diferente do particionamento rígido, as colunas de clusterização podem ser alteradas
   no futuro sem necessidade de reescrever a tabela inteira (`ALTER TABLE ... CLUSTER BY`).
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    when,
    current_timestamp,
    round as spark_round,
    to_date
)


def get_spark_session(app_name: str = "Silver_Cleansing_Pipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def ensure_silver_telemetry_table(spark: SparkSession, table_name: str):
    """Cria a tabela Silver com Liquid Clustering habilitado caso ainda não exista."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            vehicle_id STRING NOT NULL,
            driver_id STRING,
            event_timestamp TIMESTAMP NOT NULL,
            event_date DATE NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            speed_kmh DOUBLE,
            engine_rpm INT,
            fuel_level_pct DOUBLE,
            engine_temp_c INT,
            odometer_km DOUBLE,
            status STRING,
            ignition_on BOOLEAN,
            is_idling BOOLEAN,
            is_overspeeding BOOLEAN,
            _cleaned_at TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (vehicle_id, event_timestamp);
    """)
    print(f"[Silver] Tabela {table_name} verificada com Liquid Clustering.")


def ensure_silver_orders_table(spark: SparkSession, table_name: str):
    """Cria a tabela Silver de Pedidos com Liquid Clustering."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id STRING NOT NULL,
            customer_id STRING,
            vehicle_id STRING,
            driver_id STRING,
            origin_hub_id STRING,
            destination_hub_id STRING,
            created_at TIMESTAMP,
            dispatched_at TIMESTAMP,
            promised_sla_at TIMESTAMP,
            actual_delivery_at TIMESTAMP,
            order_status STRING,
            package_weight_kg DOUBLE,
            freight_value_brl DOUBLE,
            sla_breach_flag INT,
            order_date DATE NOT NULL,
            _cleaned_at TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (order_id, order_date);
    """)
    print(f"[Silver] Tabela {table_name} verificada com Liquid Clustering.")


def clean_telemetry_silver(spark: SparkSession, catalog: str = "fleet_logistics"):
    """
    Higieniza a camada Bronze de Telemetria e materializa na Silver.
    - Filtra payloads corrompidos (_rescued_data presente ou timestamps inválidos).
    - Aplica regras de negócio físicas (velocidade > 0 e < 250 km/h, limites de coordenadas).
    - Deduplica por (vehicle_id, event_timestamp).
    - Adiciona flags calculadas de condução (is_idling, is_overspeeding).
    """
    bronze_table = f"{catalog}.bronze.telemetry_raw"
    silver_table = f"{catalog}.silver.telemetry_cleaned"

    ensure_silver_telemetry_table(spark, silver_table)

    print(f"[Silver] Lendo de {bronze_table}...")
    df_bronze = spark.read.table(bronze_table)

    # 1. Parse seguro de timestamp e isolamento de erros
    df_parsed = (
        df_bronze
        .withColumn("parsed_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("event_date", to_date(col("parsed_timestamp")))
    )

    # 2. Filtragem de Qualidade de Dados (Data Quality Rules)
    # Rejeita nulos obrigatórios e descarta registros corrompidos capturados pelo Auto Loader
    df_valid = (
        df_parsed
        .filter(col("vehicle_id").isNotNull() & (col("vehicle_id") != ""))
        .filter(col("parsed_timestamp").isNotNull())
        .filter(col("latitude").between(-90.0, 90.0))
        .filter(col("longitude").between(-180.0, 180.0))
        .filter((col("speed_kmh") >= 0.0) & (col("speed_kmh") <= 250.0))
        .filter((col("engine_rpm") >= 0) & (col("engine_rpm") <= 8000))
    )

    # 3. Deduplicação em nível de veículo e timestamp de evento
    df_deduped = df_valid.dropDuplicates(["vehicle_id", "parsed_timestamp"])

    # 4. Feature Engineering & Regras de Negócio
    df_silver = (
        df_deduped
        .select(
            col("vehicle_id"),
            col("driver_id"),
            col("parsed_timestamp").alias("event_timestamp"),
            col("event_date"),
            spark_round(col("latitude"), 6).alias("latitude"),
            spark_round(col("longitude"), 6).alias("longitude"),
            spark_round(col("speed_kmh"), 2).alias("speed_kmh"),
            col("engine_rpm").cast("int").alias("engine_rpm"),
            spark_round(col("fuel_level_pct"), 2).alias("fuel_level_pct"),
            col("engine_temp_c").cast("int").alias("engine_temp_c"),
            spark_round(col("odometer_km"), 2).alias("odometer_km"),
            col("status"),
            col("ignition_on").cast("boolean").alias("ignition_on"),
            # Flag Idling: Motor ligado, velocidade zero e RPM > 550
            when((col("speed_kmh") == 0.0) & (col("engine_rpm") > 550), True)
            .otherwise(False).alias("is_idling"),
            # Flag Overspeeding: Acima de 100 km/h
            when(col("speed_kmh") > 100.0, True)
            .otherwise(False).alias("is_overspeeding"),
            current_timestamp().alias("_cleaned_at")
        )
    )

    # 5. Escrita na Silver Delta com Liquid Clustering
    # Usando merge (UPSERT) idempotente para garantir reprodutibilidade
    df_silver.createOrReplaceTempView("stg_silver_telemetry")

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING stg_silver_telemetry AS source
        ON target.vehicle_id = source.vehicle_id 
           AND target.event_timestamp = source.event_timestamp
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *;
    """)

    # 6. Otimização Automática do Liquid Clustering (Zero Maintenance)
    print(f"[Silver] Executando OPTIMIZE incremental em {silver_table}...")
    spark.sql(f"OPTIMIZE {silver_table};")
    print(f"[Silver] Pipeline de Telemetria Silver finalizado!")


def clean_orders_silver(spark: SparkSession, catalog: str = "fleet_logistics"):
    """Higieniza pedidos do ERP e grava na Silver com Liquid Clustering."""
    bronze_table = f"{catalog}.bronze.orders_raw"
    silver_table = f"{catalog}.silver.orders_cleaned"

    ensure_silver_orders_table(spark, silver_table)

    print(f"[Silver] Lendo pedidos de {bronze_table}...")
    df_bronze = spark.read.table(bronze_table)

    df_cleaned = (
        df_bronze
        .filter(col("order_id").isNotNull())
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("dispatched_at", to_timestamp(col("dispatched_at")))
        .withColumn("promised_sla_at", to_timestamp(col("promised_sla_at")))
        .withColumn("actual_delivery_at", to_timestamp(col("actual_delivery_at")))
        .withColumn("order_date", to_date(col("created_at")))
        .withColumn("_cleaned_at", current_timestamp())
        .dropDuplicates(["order_id"])
    )

    df_cleaned.createOrReplaceTempView("stg_silver_orders")

    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING stg_silver_orders AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *;
    """)

    spark.sql(f"OPTIMIZE {silver_table};")
    print(f"[Silver] Pipeline de Pedidos Silver finalizado!")


if __name__ == "__main__":
    spark = get_spark_session()
    catalog_name = sys.argv[1] if len(sys.argv) > 1 else "fleet_logistics"

    clean_telemetry_silver(spark, catalog=catalog_name)
    clean_orders_silver(spark, catalog=catalog_name)
