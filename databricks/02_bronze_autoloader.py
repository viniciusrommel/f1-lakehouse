# Databricks notebook source
# Usa o `spark` global do notebook: SparkSession propria nao funciona em
# compute Serverless.

from pyspark.sql.functions import current_timestamp

S3_BUCKET   = "f1-lakehouse-galgo"
LANDING     = f"s3://{S3_BUCKET}/cdc"
CHECKPOINT  = f"s3://{S3_BUCKET}/_checkpoints/autoloader"
CATALOG     = "f1"

# topico -> (nome da tabela bronze, colunas de particao)
# ingestion_jobs nao tem `round`, entao particiona so por season.
TOPICS = {
    "f1cdc.public.telemetry_events":      ("telemetry", ["season", "round", "session_type"]),
    "f1cdc.public.lap_events":            ("laps",       ["season", "round", "session_type"]),
    "f1cdc.public.weather_events":        ("weather",             ["season", "round", "session_type"]),
    "f1cdc.public.race_control_messages": ("race_control",        ["season", "round", "session_type"]),
    "f1cdc.public.track_status_events":   ("track_status",        ["season", "round", "session_type"]),
    "f1cdc.public.session_status_events": ("session_status",      ["season", "round", "session_type"]),
    "f1cdc.public.session_results":       ("results",             ["season", "round", "session_type"]),
    "f1cdc.public.ingestion_jobs":        ("ingestion_jobs", ["season"]),
}


spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")


for topic, (table, partition_cols) in TOPICS.items():
    print(f"Processando pendências: {topic} -> {CATALOG}.bronze.{table}")

    query = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "avro")
            .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/{table}/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{LANDING}/{topic}")
            # Bronze imutavel: nao filtra deletes, a Silver cuida disso
            .withColumn("_bronze_loaded_at", current_timestamp())
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"{CHECKPOINT}/{table}/chk")
            .option("mergeSchema", "true")
            .partitionBy(*partition_cols)
            .trigger(availableNow=True)
            .toTable(f"{CATALOG}.bronze.{table}")
    )
    query.awaitTermination()
    print(f"  ok: f1.bronze.{table} atualizada.")


for table, _ in TOPICS.values():
    n = spark.table(f"{CATALOG}.bronze.{table}").count()
    print(f"f1.bronze.{table}: {n} linhas")
