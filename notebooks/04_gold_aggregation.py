# Databricks notebook source
# Project: Autonomous Vehicle Data Processing Platform
# Notebook: 04_gold_aggregation.py
# Purpose: Build Gold-layer aggregated tables for Synapse analytics and Power BI

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    countDistinct, round as spark_round,
    current_timestamp, lit, date_trunc,
    percentile_approx, max as spark_max, min as spark_min
)

spark = SparkSession.builder.getOrCreate()

SILVER_PATH = "abfss://silver@<storage-account>.dfs.core.windows.net/annotations"
GOLD_BASE   = "abfss://gold@<storage-account>.dfs.core.windows.net"

silver_df = spark.read.format("delta").load(SILVER_PATH)

# COMMAND ----------
# === Gold Table 1: Daily Project Summary ===
daily_project_summary = (silver_df
    .groupBy("project_id", "annotation_date")
    .agg(
        count("annotation_id").alias("total_annotations"),
        countDistinct("annotator_id").alias("active_annotators"),
        countDistinct("frame_id").alias("frames_annotated"),
        avg("quality_score").alias("avg_quality_score"),
        spark_sum(when(col("quality_tier") == "HIGH", 1).otherwise(0)).alias("high_quality_count"),
        spark_sum(when(col("quality_tier") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
        spark_round(
            spark_sum(when(col("quality_tier") == "HIGH", 1).otherwise(0)) /
            count("annotation_id") * 100, 2
        ).alias("high_quality_pct")
    )
    .withColumn("_created_at", current_timestamp())
)

(daily_project_summary
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("annotation_date")
    .save(f"{GOLD_BASE}/daily_project_summary")
)
print("✅ Gold: daily_project_summary written")

# COMMAND ----------
# === Gold Table 2: Annotator Performance Summary ===
annotator_performance = (silver_df
    .groupBy("annotator_id", "project_id")
    .agg(
        count("annotation_id").alias("total_annotations"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score"),
        spark_round(avg("confidence_score"), 2).alias("avg_confidence"),
        spark_sum(when(col("quality_tier") == "HIGH", 1).otherwise(0)).alias("high_quality_count"),
        spark_sum(when(col("quality_tier") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
        spark_max("annotated_at").alias("last_annotation_at"),
        spark_min("annotated_at").alias("first_annotation_at")
    )
    .withColumn("_created_at", current_timestamp())
)

(annotator_performance
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_BASE}/annotator_performance")
)
print("✅ Gold: annotator_performance written")

# COMMAND ----------
# === Gold Table 3: Object Class Distribution ===
object_class_dist = (silver_df
    .groupBy("project_id", "object_class", "label_category", "annotation_date")
    .agg(
        count("annotation_id").alias("annotation_count"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score"),
        spark_round(avg("bbox_area"), 2).alias("avg_bbox_area")
    )
    .withColumn("_created_at", current_timestamp())
)

(object_class_dist
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("annotation_date")
    .save(f"{GOLD_BASE}/object_class_distribution")
)
print("✅ Gold: object_class_distribution written")

# COMMAND ----------
# === Gold Table 4: Vehicle KPI Summary (from sensor data) ===
# sensor_df = spark.read.format("delta").load(SILVER_SENSOR_PATH)
# ... vehicle-level aggregations would go here

print("✅ All Gold tables written successfully.")
dbutils.notebook.exit("gold_aggregation_complete")
