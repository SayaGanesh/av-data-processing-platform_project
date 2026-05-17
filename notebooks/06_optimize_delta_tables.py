# Databricks notebook source
# Notebook: 06_optimize_delta_tables.py
# Purpose: Weekly maintenance job — OPTIMIZE + ZORDER all Silver/Gold Delta tables
# Schedule: Every Sunday 01:00 AM via ADF Tumbling Window trigger

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

tables_to_optimize = [
    ("abfss://silver@<storage>.dfs.core.windows.net/annotations",    "project_id, annotation_date"),
    ("abfss://gold@<storage>.dfs.core.windows.net/annotator_productivity", "annotation_date"),
    ("abfss://gold@<storage>.dfs.core.windows.net/dataset_readiness", "annotation_date"),
]

for path, zorder_cols in tables_to_optimize:
    print(f"Optimizing: {path}")
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_cols})")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")  # 7-day retention
    print(f"  ✅ Done.")

print("All tables optimized.")
