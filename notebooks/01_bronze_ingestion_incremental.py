# Databricks notebook source
# Project: AI Training Data Quality & Annotation Analytics Platform
# Notebook: 01_bronze_ingestion_incremental.py
# Purpose: Watermark-based incremental load of annotation outputs into Bronze Delta
#          Only processes new/changed records since last successful pipeline run

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, max as spark_max
)
import pyodbc

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ADF parameters
dbutils.widgets.text("pipeline_name", "annotation_ingestion")
dbutils.widgets.text("source_path",   "")
dbutils.widgets.text("source_type",   "annotation")
dbutils.widgets.text("file_format",   "json")

PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
SOURCE_PATH   = dbutils.widgets.get("source_path")
SOURCE_TYPE   = dbutils.widgets.get("source_type")
FILE_FORMAT   = dbutils.widgets.get("file_format")

BRONZE_PATH   = f"abfss://bronze@<storage>.dfs.core.windows.net/{SOURCE_TYPE}"
JDBC_URL      = dbutils.secrets.get("kv-scope", "AZURE-SQL-JDBC-URL")

# COMMAND ----------
# ── Step 1: Read Watermark ──────────────────────────────────────────────────
# Watermark stored in Azure SQL control table
# Returns last successfully processed timestamp for this pipeline

watermark_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f"watermark.pipeline_watermark")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
    .filter(col("pipeline_name") == PIPELINE_NAME)
    .select("last_processed_ts")
)

last_watermark = watermark_df.first()["last_processed_ts"] if watermark_df.count() > 0 else "1900-01-01"
print(f"Last watermark: {last_watermark}")

# COMMAND ----------
# ── Step 2: Read Only New Records ──────────────────────────────────────────
# Source system writes a `updated_at` column on every insert/update

raw_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .json(SOURCE_PATH) if FILE_FORMAT == "json"
    else spark.read.option("header","true").option("inferSchema","true").csv(SOURCE_PATH)
)

# Filter: only records newer than watermark
incremental_df = (
    raw_df
    .filter(col("updated_at") > lit(last_watermark))
    .withColumn("_ingested_at",  current_timestamp())
    .withColumn("_source_file",  input_file_name())
    .withColumn("_source_type",  lit(SOURCE_TYPE))
    .withColumn("_pipeline_name", lit(PIPELINE_NAME))
)

new_record_count = incremental_df.count()
print(f"New records since watermark: {new_record_count}")

if new_record_count == 0:
    print("No new records. Exiting.")
    dbutils.notebook.exit("no_new_records")

# COMMAND ----------
# ── Step 3: Write to Bronze Delta ──────────────────────────────────────────
(incremental_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print(f"✅ Written {new_record_count} records to Bronze: {BRONZE_PATH}")

# COMMAND ----------
# ── Step 4: Update Watermark ───────────────────────────────────────────────
# Get max updated_at from the batch just written
new_watermark = incremental_df.agg(spark_max("updated_at")).first()[0]

# Update watermark in Azure SQL
spark.sql(f"""
    MERGE INTO watermark.pipeline_watermark AS target
    USING (SELECT '{PIPELINE_NAME}' AS pipeline_name,
                  CAST('{new_watermark}' AS DATETIME2) AS last_processed_ts) AS source
    ON target.pipeline_name = source.pipeline_name
    WHEN MATCHED THEN
        UPDATE SET target.last_processed_ts = source.last_processed_ts,
                   target.last_updated = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (pipeline_name, last_processed_ts, last_updated)
        VALUES (source.pipeline_name, source.last_processed_ts, GETDATE());
""")

print(f"✅ Watermark updated to: {new_watermark}")
dbutils.notebook.exit(str(new_record_count))
