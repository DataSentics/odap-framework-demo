# Databricks notebook source
from pyspark.sql import functions as f
from delta import DeltaTable

# COMMAND ----------

df = (
    spark
    .readStream
    .option("startingVersion", "latest")
    .table("odap_digi_sdm_l2.notification_consent_stream")
    .select(
        f.col("customer_id"),
        f.col("event_timestamp").alias("timestamp"),
        f.col("notification_consent"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("path", "dbfs:/odap_features/notification_consent.delta")
    .option("checkpointLocation", "dbfs:/odap_features/notification_consent.delta/_checkpoint")
    .toTable("odap_features.notification_consent")
)

# COMMAND ----------

def merge_to_latest(batch_df, batch_id):
    (
        batch_df
        .limit(0)
        .write
        .format("delta")
        .mode("append")
        .option("path", "dbfs:/odap_features/notification_consent_latest.delta")
        .saveAsTable("odap_features.notification_consent_latest")
    )
    
    delta_table = DeltaTable.forName(spark, "odap_features.notification_consent_latest")

    df = (
        batch_df
        .groupBy("customer_id")
        .agg(
            f.max(f.struct("timestamp", "notification_consent")).alias("struct")
        )
        .select(
            f.col("customer_id"),
            f.col("struct.timestamp").alias("timestamp"),
            f.col("struct.notification_consent").alias("notification_consent")
        )
    )
    
    (
        delta_table.alias("target")
        .merge(df.alias("source"), "source.customer_id = target.customer_id AND source.timestamp >= target.timestamp")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(
    spark
    .readStream
    .option("ignoreChanges", "true")
    .table("odap_features.notification_consent")
    .writeStream
    .foreachBatch(merge_to_latest)
    .start()
)

# COMMAND ----------

spark.read.table("odap_features.notification_consent").display()

# COMMAND ----------

spark.read.table("odap_features.notification_consent_latest").display()
