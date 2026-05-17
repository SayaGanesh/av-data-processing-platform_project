# Databricks notebook source
# Notebook: 04_annotator_productivity.py
# Purpose: Calculate per-annotator productivity metrics and write to Gold layer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, current_timestamp, sum as spark_sum, when
spark = SparkSession.builder.getOrCreate()

SILVER_PATH = "abfss://silver@<storage>.dfs.core.windows.net/annotations"
GOLD_PATH   = "abfss://gold@<storage>.dfs.core.windows.net/annotator_productivity"

silver_df = spark.read.format("delta").load(SILVER_PATH)

productivity = (silver_df
    .groupBy("annotator_id", "project_id", "annotation_date")
    .agg(
        count("annotation_id").alias("daily_annotations"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score"),
        spark_round(avg("confidence_score"), 2).alias("avg_confidence"),
        spark_sum(when(col("quality_tier") == "HIGH", 1).otherwise(0)).alias("high_quality_count"),
        spark_sum(when(col("quality_tier") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
    )
    .withColumn("rejection_rate",
        spark_round(col("rejected_count") / col("daily_annotations"), 4)
    )
    .withColumn("_created_at", current_timestamp())
)

productivity.write.format("delta").mode("overwrite").partitionBy("annotation_date").save(GOLD_PATH)
print("✅ Annotator productivity written to Gold.")
