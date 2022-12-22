# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

dbutils.widgets.text("timestamp", "")
dbutils.widgets.text("timeshift", "0")
dbutils.widgets.text("target", "no target")

# COMMAND ----------

(
    # insert your table containing all ids
    spark.table("odap_offline_sdm_l2.customer")
    .select(
        "customer_id",
        f.lit("no target").alias("target"),
        (
            f.lit(dbutils.widgets.get("timestamp")).cast("timestamp")
            - f.expr(f"interval {dbutils.widgets.get('timeshift')} days")
        ).alias("timestamp"),
    )
    # insert your table containing targets
    .unionByName(spark.table("odap_targets.targets"))
    .filter(f.col("target") == dbutils.widgets.get("target"))
).createOrReplaceTempView("target_store")

print("Target store successfully initialized")
