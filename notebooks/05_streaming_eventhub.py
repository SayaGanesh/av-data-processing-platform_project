# Databricks notebook source
# Project: Autonomous Vehicle Data Processing Platform
# Notebook: 05_streaming_eventhub.py
# Purpose: Consume real-time annotation events from Azure Event Hub
#          Write to Bronze Delta with exactly-once semantics via checkpointing

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, TimestampType, IntegerType
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Event Hub connection — secrets from Key Vault via Databricks secret scope
EH_NAMESPACE        = dbutils.secrets.get("kv-scope", "EH-NAMESPACE")
EH_NAME             = dbutils.secrets.get("kv-scope", "EH-NAME")
EH_CONNECTION_STRING = dbutils.secrets.get("kv-scope", "EVENT-HUB-CONNECTION-STRING")

BRONZE_STREAM_PATH  = "abfss://bronze@<storage-account>.dfs.core.windows.net/annotation_events_stream"
CHECKPOINT_PATH     = "abfss://checkpoints@<storage-account>.dfs.core.windows.net/annotation_events"

# COMMAND ----------
# Event Hub Spark connector configuration
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        EH_CONNECTION_STRING
    ),
    "eventhubs.consumerGroup"   : "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
}

# COMMAND ----------
# Schema for annotation event payload (JSON body in Event Hub message)
annotation_event_schema = StructType([
    StructField("annotation_id",   StringType(),  True),
    StructField("frame_id",        StringType(),  True),
    StructField("project_id",      StringType(),  True),
    StructField("annotator_id",    StringType(),  True),
    StructField("object_class",    StringType(),  True),
    StructField("bbox_x_min",      FloatType(),   True),
    StructField("bbox_x_max",      FloatType(),   True),
    StructField("bbox_y_min",      FloatType(),   True),
    StructField("bbox_y_max",      FloatType(),   True),
    StructField("confidence_score", FloatType(),  True),
    StructField("annotation_tool", StringType(),  True),
    StructField("event_type",      StringType(),  True),  # submitted | revised | approved | rejected
    StructField("event_timestamp", StringType(),  True),
])

# COMMAND ----------
# Read stream from Event Hub
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# COMMAND ----------
# Parse JSON body + add audit columns
parsed_stream = (
    raw_stream
    .select(
        from_json(
            col("body").cast("string"),
            annotation_event_schema
        ).alias("payload"),
        col("enqueuedTime").alias("eh_enqueued_at"),
        col("offset").alias("eh_offset"),
        col("sequenceNumber").alias("eh_sequence_no")
    )
    .select(
        "payload.*",
        "eh_enqueued_at",
        "eh_offset",
        "eh_sequence_no"
    )
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_layer", lit("bronze_stream"))
)

# COMMAND ----------
# Write stream to Bronze Delta — append mode, checkpointed
stream_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")   # micro-batch every 30s
    .start(BRONZE_STREAM_PATH)
)

print(f"✅ Streaming query started. Writing to: {BRONZE_STREAM_PATH}")
print(f"   Checkpoint: {CHECKPOINT_PATH}")

# Keep streaming — this notebook runs as an always-on job
stream_query.awaitTermination()
