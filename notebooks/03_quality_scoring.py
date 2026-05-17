# Databricks notebook source
# Project: AI Training Data Quality & Annotation Analytics Platform
# Notebook: 03_quality_scoring.py
# Purpose: Apply full quality scoring framework on Silver annotation data
#          Rule-based + statistical (Z-score) validation
#          Scores 0-100. Tiers: HIGH / ACCEPTABLE / FLAGGED / REJECTED

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    avg, stddev_pop, abs as spark_abs,
    round as spark_round, lag, datediff,
    date_trunc, to_date, count
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Load quality rule thresholds from config (externalized — no hardcoding)
config_json = dbutils.fs.head(
    "abfss://config@<storage>.dfs.core.windows.net/quality_rules_config.json"
)
rules = json.loads(config_json)

AUTO_REJECT_THRESHOLD  = rules["auto_reject_threshold"]    # 40
REVIEW_THRESHOLD       = rules["review_threshold"]         # 70
MIN_BBOX_AREA          = rules["min_bbox_area_px"]         # 10
MAX_BBOX_AREA_PCT      = rules["max_bbox_area_pct"]        # 0.5
ZSCORE_ALERT_THRESHOLD = rules["z_score_alert_threshold"]  # 3.0
ACCURACY_DROP_ALERT    = rules["accuracy_drop_alert_pct"]  # 10.0

IMAGE_WIDTH  = 1920.0
IMAGE_HEIGHT = 1080.0

SILVER_PATH = "abfss://silver@<storage>.dfs.core.windows.net/annotations"

# COMMAND ----------
silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"Records to score: {silver_df.count()}")

# COMMAND ----------
# ── Rule-based Scoring ──────────────────────────────────────────────────────
# Score starts at 100 and deductions are applied per failed rule

scored_df = (
    silver_df
    # Geometry calculations
    .withColumn("bbox_area",
        (col("bbox_x_max") - col("bbox_x_min")) *
        (col("bbox_y_max") - col("bbox_y_min"))
    )
    .withColumn("bbox_area_pct",
        col("bbox_area") / (IMAGE_WIDTH * IMAGE_HEIGHT)
    )

    # Deductions
    .withColumn("d_missing_confidence",
        when(col("confidence_score").isNull(), 10).otherwise(0)
    )
    .withColumn("d_small_bbox",
        when(col("bbox_area") < MIN_BBOX_AREA, 10).otherwise(0)
    )
    .withColumn("d_large_bbox",
        when(col("bbox_area_pct") > MAX_BBOX_AREA_PCT, 15).otherwise(0)
    )
    .withColumn("d_unknown_label",
        when(col("label_id").isNull(), 20).otherwise(0)
    )
    .withColumn("d_missing_fields",
        when(
            col("object_class").isNull() |
            col("frame_id").isNull() |
            col("annotator_id").isNull(), 20
        ).otherwise(0)
    )

    # Final score
    .withColumn("quality_score",
        spark_round(
            100
            - col("d_missing_confidence")
            - col("d_small_bbox")
            - col("d_large_bbox")
            - col("d_unknown_label")
            - col("d_missing_fields"),
            2
        )
    )

    # Quality tier assignment
    .withColumn("quality_tier",
        when(col("quality_score") >= 85, "HIGH")
        .when(col("quality_score") >= REVIEW_THRESHOLD, "ACCEPTABLE")
        .when(col("quality_score") >= AUTO_REJECT_THRESHOLD, "FLAGGED_FOR_REVIEW")
        .otherwise("REJECTED")
    )
)

# COMMAND ----------
# ── Statistical: Z-Score Per Annotator ─────────────────────────────────────
# Flags statistical outliers — annotators performing unusually poorly

annotator_window = Window.partitionBy("annotator_id")

scored_with_stats = (
    scored_df
    .withColumn("annotator_avg_score", spark_round(avg("quality_score").over(annotator_window), 2))
    .withColumn("annotator_std_score", spark_round(stddev_pop("quality_score").over(annotator_window), 4))
    .withColumn("z_score",
        spark_round(
            spark_abs(col("quality_score") - col("annotator_avg_score")) /
            when(col("annotator_std_score") > 0, col("annotator_std_score")).otherwise(lit(1.0)),
            3
        )
    )
    .withColumn("is_statistical_outlier",
        when(col("z_score") > ZSCORE_ALERT_THRESHOLD, True).otherwise(False)
    )
    .withColumn("_quality_scored_at", current_timestamp())
)

# COMMAND ----------
# ── Week-over-Week Accuracy Drop Detection ──────────────────────────────────
weekly_accuracy = (
    silver_df
    .withColumn("week", date_trunc("week", to_date(col("annotated_at"))))
    .groupBy("annotator_id", "week")
    .agg(spark_round(avg("quality_score"), 2).alias("weekly_avg_score"))
)

annotator_week_window = Window.partitionBy("annotator_id").orderBy("week")

weekly_with_prev = (
    weekly_accuracy
    .withColumn("prev_week_score", lag("weekly_avg_score", 1).over(annotator_week_window))
    .withColumn("wow_drop_pct",
        spark_round(
            when(col("prev_week_score").isNotNull(),
                (col("prev_week_score") - col("weekly_avg_score")) /
                col("prev_week_score") * 100
            ).otherwise(lit(0.0)),
            2
        )
    )
    .withColumn("trigger_wow_alert",
        when(col("wow_drop_pct") > ACCURACY_DROP_ALERT, True).otherwise(False)
    )
)

# Annotators needing WoW alert
wow_alerts = weekly_with_prev.filter(col("trigger_wow_alert") == True)
print(f"Annotators with >10% WoW accuracy drop: {wow_alerts.count()}")

# COMMAND ----------
# ── Write Scores Back to Silver via MERGE ───────────────────────────────────
delta_table = DeltaTable.forPath(spark, SILVER_PATH)

(delta_table.alias("t")
    .merge(
        scored_with_stats.alias("s"),
        "t.annotation_id = s.annotation_id"
    )
    .whenMatchedUpdate(set={
        "quality_score"         : "s.quality_score",
        "quality_tier"          : "s.quality_tier",
        "z_score"               : "s.z_score",
        "is_statistical_outlier": "s.is_statistical_outlier",
        "bbox_area"             : "s.bbox_area",
        "bbox_area_pct"         : "s.bbox_area_pct",
        "_quality_scored_at"    : "s._quality_scored_at"
    })
    .execute()
)

print("✅ Quality scores merged to Silver.")

# COMMAND ----------
# ── Route REJECTED to Quarantine ────────────────────────────────────────────
QUARANTINE_PATH = "abfss://quarantine@<storage>.dfs.core.windows.net/rejected_annotations"

rejected_df = scored_with_stats.filter(col("quality_tier") == "REJECTED")

if rejected_df.count() > 0:
    (rejected_df
        .withColumn("_quarantine_reason", lit("AUTO_REJECTED_LOW_QUALITY_SCORE"))
        .write
        .format("delta")
        .mode("append")
        .save(QUARANTINE_PATH)
    )

# COMMAND ----------
# Summary
tier_counts = scored_with_stats.groupBy("quality_tier").count().collect()
for row in tier_counts:
    print(f"  {row['quality_tier']:25s}: {row['count']:,}")

print(f"\n  Statistical outliers flagged: {scored_with_stats.filter(col('is_statistical_outlier')==True).count():,}")
print(f"  WoW accuracy alerts         : {wow_alerts.count():,}")

dbutils.notebook.exit("quality_scoring_complete")
