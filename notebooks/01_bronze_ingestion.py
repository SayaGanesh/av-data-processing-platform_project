# Databricks notebook source
# Project: Autonomous Vehicle Data Processing Platform
# Notebook: 01_bronze_ingestion.py
# Purpose: Ingest raw sensor logs, image metadata, and annotation outputs into Bronze Delta layer
# Triggered by: Azure Data Factory Notebook Activity

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Ingestion — Raw Data Landing
# MAGIC Reads raw files from ADLS Gen2 staging area and writes to Bronze Delta tables.
# MAGIC No transformations applied — data landed exactly as received.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit
)
from pyspark.sql.types import StructType
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Parameters passed from ADF pipeline
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("file_format", "json")
dbutils.widgets.text("source_type", "annotation")  # annotation | sensor_log | image_metadata

source_path   = dbutils.widgets.get("source_path")
target_table  = dbutils.widgets.get("target_table")
file_format   = dbutils.widgets.get("file_format")
source_type   = dbutils.widgets.get("source_type")

print(f"Source path  : {source_path}")
print(f"Target table : {target_table}")
print(f"File format  : {file_format}")
print(f"Source type  : {source_type}")

# COMMAND ----------
# Bronze storage paths
BRONZE_BASE    = "abfss://bronze@<storage-account>.dfs.core.windows.net"
CHECKPOINT_BASE = "abfss://checkpoints@<storage-account>.dfs.core.windows.net"

bronze_path    = f"{BRONZE_BASE}/{target_table}"
checkpoint_path = f"{CHECKPOINT_BASE}/bronze/{target_table}"

# COMMAND ----------
# Read raw data — format determined by ADF parameter
def read_raw_data(path: str, fmt: str):
    """
    Read raw files from ADLS staging area.
    Adds audit columns: _ingested_at, _source_file.
    """
    reader = spark.read.option("recursiveFileLookup", "true")

    if fmt == "json":
        df = reader.json(path)
    elif fmt == "csv":
        df = reader.option("header", "true").option("inferSchema", "true").csv(path)
    elif fmt == "parquet":
        df = reader.parquet(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    # Add audit metadata columns
    df = (df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_type", lit(source_type))
    )
    return df

# COMMAND ----------
raw_df = read_raw_data(source_path, file_format)

print(f"Records read from source: {raw_df.count()}")
print("Schema:")
raw_df.printSchema()

# COMMAND ----------
# Write to Bronze Delta — append only, preserve full history
# mergeSchema=true to handle new columns added by source systems over time
(raw_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(bronze_path)
)

print(f"✅ Successfully written to Bronze: {bronze_path}")

# COMMAND ----------
# Log record count to control table for monitoring
record_count = raw_df.count()

spark.sql(f"""
    INSERT INTO audit.pipeline_run_log
    (run_time, source_type, source_path, target_path, records_written, status)
    VALUES (
        current_timestamp(),
        '{source_type}',
        '{source_path}',
        '{bronze_path}',
        {record_count},
        'SUCCESS'
    )
""")

# Pass record count back to ADF for logging
dbutils.notebook.exit(str(record_count))
