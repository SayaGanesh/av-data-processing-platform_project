# Databricks notebook source
# Notebook: 05_gold_dataset_readiness.py
# Purpose: Calculate dataset readiness metrics per project — what % is ready for model training

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round, when, current_timestamp, lit
spark = SparkSession.builder.getOrCreate()

SILVER_PATH = "abfss://silver@<storage>.dfs.core.windows.net/annotations"
GOLD_PATH   = "abfss://gold@<storage>.dfs.core.windows.net/dataset_readiness"

silver_df = spark.read.format("delta").load(SILVER_PATH)

readiness = (silver_df
    .groupBy("project_id", "annotation_date")
    .agg(
        count("frame_id").alias("total_annotated_frames"),
        count(when(col("quality_tier").isin("HIGH","ACCEPTABLE"), col("frame_id"))).alias("qa_passed_frames"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score")
    )
    .withColumn("qa_pass_rate_pct",
        spark_round(col("qa_passed_frames") / col("total_annotated_frames") * 100, 2)
    )
    .withColumn("ready_for_training",
        when(col("qa_pass_rate_pct") >= 90, lit(True)).otherwise(lit(False))
    )
    .withColumn("_calculated_at", current_timestamp())
)

readiness.write.format("delta").mode("overwrite").partitionBy("annotation_date").save(GOLD_PATH)
print("✅ Dataset readiness written to Gold.")
