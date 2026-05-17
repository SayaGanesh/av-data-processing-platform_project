# Databricks notebook source
# Project: Autonomous Vehicle Data Processing Platform
# Notebook: 02_silver_transformation.py
# Purpose: Cleanse, normalize, and standardize Bronze annotation data → Silver Delta layer

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Transformation — Cleansing & Normalization
# MAGIC Reads from Bronze Delta, applies cleansing and schema normalization,
# MAGIC writes clean records to Silver and invalid records to Quarantine.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, lit, current_timestamp,
    coalesce, regexp_replace, to_timestamp, explode,
    broadcast
)
from pyspark.sql.types import FloatType, StringType, TimestampType
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Storage paths
BRONZE_PATH    = "abfss://bronze@<storage-account>.dfs.core.windows.net/annotation_outputs"
SILVER_PATH    = "abfss://silver@<storage-account>.dfs.core.windows.net/annotations"
QUARANTINE_PATH = "abfss://quarantine@<storage-account>.dfs.core.windows.net/annotation_failures"
SCHEMA_REGISTRY_PATH = "abfss://config@<storage-account>.dfs.core.windows.net/schema_registry.json"

# COMMAND ----------
# --- Step 1: Read Bronze ---
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze records: {bronze_df.count()}")

# COMMAND ----------
# --- Step 2: Schema Normalization ---
# Different annotation tools use different field names for the same concept.
# Schema registry (stored in Azure SQL) maps source field → canonical field.

schema_registry = (
    spark.read
    .format("jdbc")
    .option("url", dbutils.secrets.get("kv-scope", "SYNAPSE-JDBC-URL"))
    .option("dbtable", "config.schema_registry")
    .load()
)

# Build rename map from registry
# (Applied via a loop of withColumnRenamed — kept readable for maintainability)
rename_map = {row["source_field"]: row["canonical_field"]
              for row in schema_registry.collect()}

normalized_df = bronze_df
for src_col, tgt_col in rename_map.items():
    if src_col in normalized_df.columns:
        normalized_df = normalized_df.withColumnRenamed(src_col, tgt_col)

# COMMAND ----------
# --- Step 3: Type Casting & Standardization ---
cleaned_df = (normalized_df
    # Cast coordinate columns to Float
    .withColumn("bbox_x_min",   col("bbox_x_min").cast(FloatType()))
    .withColumn("bbox_x_max",   col("bbox_x_max").cast(FloatType()))
    .withColumn("bbox_y_min",   col("bbox_y_min").cast(FloatType()))
    .withColumn("bbox_y_max",   col("bbox_y_max").cast(FloatType()))
    # Standardize string fields
    .withColumn("object_class", lower(trim(col("object_class"))))
    .withColumn("annotation_tool", lower(trim(col("annotation_tool"))))
    # Standardize timestamp
    .withColumn("annotated_at", to_timestamp(col("annotated_at")))
    # Add processing metadata
    .withColumn("_processed_at", current_timestamp())
    .withColumn("_layer", lit("silver"))
    # Remove exact duplicates
    .dropDuplicates(["annotation_id"])
)

# COMMAND ----------
# --- Step 4: Enrich with Lookup (Approved Label Taxonomy) ---
label_taxonomy = (
    spark.read.table("config.approved_labels")
    .select("object_class", "label_category", "label_id")
)

enriched_df = cleaned_df.join(
    broadcast(label_taxonomy),
    on="object_class",
    how="left"
)

# COMMAND ----------
# --- Step 5: Split Valid vs Invalid Records ---

valid_df = enriched_df.filter(
    col("annotation_id").isNotNull() &
    col("object_class").isNotNull() &
    col("bbox_x_min").isNotNull() &
    col("bbox_x_max").isNotNull() &
    col("bbox_y_min").isNotNull() &
    col("bbox_y_max").isNotNull() &
    (col("bbox_x_max") > col("bbox_x_min")) &
    (col("bbox_y_max") > col("bbox_y_min")) &
    col("label_id").isNotNull()          # failed taxonomy join = unknown label
)

invalid_df = enriched_df.subtract(valid_df).withColumn(
    "_quarantine_reason",
    when(col("annotation_id").isNull(), "MISSING_ANNOTATION_ID")
    .when(col("object_class").isNull(), "MISSING_OBJECT_CLASS")
    .when(col("label_id").isNull(), "UNKNOWN_LABEL_TAXONOMY")
    .when(col("bbox_x_max") <= col("bbox_x_min"), "INVALID_BBOX_X")
    .when(col("bbox_y_max") <= col("bbox_y_min"), "INVALID_BBOX_Y")
    .otherwise("UNKNOWN_VALIDATION_FAILURE")
)

print(f"Valid records  : {valid_df.count()}")
print(f"Invalid records: {invalid_df.count()}")

# COMMAND ----------
# --- Step 6: Write Valid → Silver Delta ---
(valid_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("annotation_date", "project_id")
    .option("mergeSchema", "true")
    .save(SILVER_PATH)
)

print(f"✅ Written to Silver: {SILVER_PATH}")

# COMMAND ----------
# --- Step 7: Write Invalid → Quarantine Delta ---
(invalid_df
    .write
    .format("delta")
    .mode("append")
    .save(QUARANTINE_PATH)
)

print(f"⚠️  Written to Quarantine: {QUARANTINE_PATH}")

# COMMAND ----------
# --- Step 8: Run OPTIMIZE + ZORDER periodically (weekly via separate job) ---
# spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (project_id, annotation_date)")

dbutils.notebook.exit(f"valid={valid_df.count()},quarantined={invalid_df.count()}")
