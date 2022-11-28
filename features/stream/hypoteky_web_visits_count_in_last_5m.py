# Databricks notebook source
from pyspark.sql import functions as f
from delta import DeltaTable

# COMMAND ----------

df = (
    spark
    .readStream
    .option("startingVersion", "latest")
    .table("odap_digi_sdm_l2.web_visits_stream")
    .groupBy("customer_id", f.window("visit_timestamp", "5 minutes").end.alias("timestamp"))
    .agg(f.sum(f.when(f.col("url").contains("hypoteky"), 1).otherwise(0)).alias("hypoteky_web_visits_count_in_last_5m"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .option("path", "dbfs:/odap_features/hypoteky_web_visits_count_in_last_5m.delta")
    .option("checkpointLocation", "dbfs:/odap_features/hypoteky_web_visits_count_in_last_5m.delta/_checkpoint")
    .toTable("odap_features.hypoteky_web_visits_count_in_last_5m")
)

# COMMAND ----------

def merge_to_latest(batch_df, batch_id):
    (
        batch_df
        .limit(0)
        .write
        .format("delta")
        .mode("append")
        .option("path", "dbfs:/odap_features/hypoteky_web_visits_count_in_last_5m_latest.delta")
        .saveAsTable("odap_features.hypoteky_web_visits_count_in_last_5m_latest")
    )
    
    delta_table = DeltaTable.forName(spark, "odap_features.hypoteky_web_visits_count_in_last_5m_latest")
    
    df = (
        batch_df
        .groupBy("customer_id")
        .agg(
            f.max(f.struct("timestamp", "hypoteky_web_visits_count_in_last_5m")).alias("struct")
        )
        .select(
            f.col("customer_id"),
            f.col("struct.timestamp").alias("timestamp"),
            f.col("struct.hypoteky_web_visits_count_in_last_5m").alias("hypoteky_web_visits_count_in_last_5m")
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
    .table("odap_features.hypoteky_web_visits_count_in_last_5m")
    .writeStream
    .foreachBatch(merge_to_latest)
    .start()
)

# COMMAND ----------

spark.read.table("odap_features.hypoteky_web_visits_count_in_last_5m").display()

# COMMAND ----------

spark.read.table("odap_features.hypoteky_web_visits_count_in_last_5m_latest").display()
