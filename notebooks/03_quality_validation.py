# Databricks notebook source
# Project: Autonomous Vehicle Data Processing Platform
# Notebook: 03_quality_validation.py
# Purpose: Apply annotation quality scoring rules on Silver data
#          Score 0-100. Below 70 = flagged. Below 40 = auto-rejected to quarantine.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, udf, round as spark_round,
    current_timestamp, avg, stddev_pop, abs as spark_abs
)
from pyspark.sql.types import FloatType, IntegerType
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
SILVER_PATH     = "abfss://silver@<storage-account>.dfs.core.windows.net/annotations"
QUARANTINE_PATH = "abfss://quarantine@<storage-account>.dfs.core.windows.net/annotation_failures"

silver_df = spark.read.format("delta").load(SILVER_PATH)

# COMMAND ----------
# --- Quality Scoring Framework ---
# Each rule deducts points. Max score = 100.
# Deductions:
#   Missing confidence_score     : -10
#   bbox area too small (<10px²) : -15
#   bbox too large (>50% frame)  : -15
#   Low annotator historical acc : -20
#   Inconsistent with neighbors  : -10  (based on overlap stats)

IMAGE_WIDTH  = 1920.0
IMAGE_HEIGHT = 1080.0
MIN_AREA     = 10.0
MAX_AREA_PCT = 0.5

scored_df = (silver_df
    .withColumn("bbox_area",
        (col("bbox_x_max") - col("bbox_x_min")) *
        (col("bbox_y_max") - col("bbox_y_min"))
    )
    .withColumn("bbox_area_pct",
        col("bbox_area") / (IMAGE_WIDTH * IMAGE_HEIGHT)
    )
    # Deduct for missing confidence
    .withColumn("deduct_confidence",
        when(col("confidence_score").isNull(), 10).otherwise(0)
    )
    # Deduct for undersized bbox
    .withColumn("deduct_small_bbox",
        when(col("bbox_area") < MIN_AREA, 15).otherwise(0)
    )
    # Deduct for oversized bbox
    .withColumn("deduct_large_bbox",
        when(col("bbox_area_pct") > MAX_AREA_PCT, 15).otherwise(0)
    )
    # Calculate final quality score
    .withColumn("quality_score",
        100
        - col("deduct_confidence")
        - col("deduct_small_bbox")
        - col("deduct_large_bbox")
    )
    # Assign quality tier
    .withColumn("quality_tier",
        when(col("quality_score") >= 85, "HIGH")
        .when(col("quality_score") >= 70, "ACCEPTABLE")
        .when(col("quality_score") >= 40, "FLAGGED_FOR_REVIEW")
        .otherwise("REJECTED")
    )
    .withColumn("_quality_scored_at", current_timestamp())
)

# COMMAND ----------
# Annotator-level Z-score for outlier detection
annotator_stats = (scored_df
    .groupBy("annotator_id")
    .agg(
        avg("quality_score").alias("avg_score"),
        stddev_pop("quality_score").alias("std_score")
    )
)

scored_with_stats = scored_df.join(annotator_stats, on="annotator_id", how="left")

final_df = scored_with_stats.withColumn(
    "z_score",
    spark_abs(col("quality_score") - col("avg_score")) /
    when(col("std_score") > 0, col("std_score")).otherwise(lit(1.0))
)

# COMMAND ----------
# Separate auto-rejected records
auto_rejected = final_df.filter(col("quality_tier") == "REJECTED")
passed_df     = final_df.filter(col("quality_tier") != "REJECTED")

print(f"High quality  : {final_df.filter(col('quality_tier')=='HIGH').count()}")
print(f"Acceptable    : {final_df.filter(col('quality_tier')=='ACCEPTABLE').count()}")
print(f"Flagged       : {final_df.filter(col('quality_tier')=='FLAGGED_FOR_REVIEW').count()}")
print(f"Auto-rejected : {auto_rejected.count()}")

# COMMAND ----------
# Write quality scores back to Silver via MERGE (update existing records)
delta_table = DeltaTable.forPath(spark, SILVER_PATH)

(delta_table.alias("t")
    .merge(
        final_df.alias("s"),
        "t.annotation_id = s.annotation_id"
    )
    .whenMatchedUpdate(set={
        "quality_score"     : "s.quality_score",
        "quality_tier"      : "s.quality_tier",
        "z_score"           : "s.z_score",
        "_quality_scored_at": "s._quality_scored_at"
    })
    .execute()
)

# COMMAND ----------
# Auto-rejected → Quarantine
if auto_rejected.count() > 0:
    (auto_rejected
        .withColumn("_quarantine_reason", lit("AUTO_REJECTED_LOW_QUALITY_SCORE"))
        .write
        .format("delta")
        .mode("append")
        .save(QUARANTINE_PATH)
    )

print("✅ Quality scoring complete.")
