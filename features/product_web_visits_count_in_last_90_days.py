# Databricks notebook source
import datetime as dt
from pyspark.sql import functions as f

# COMMAND ----------

dbutils.widgets.text("timestamp", "2020-12-12")

# COMMAND ----------

df = spark.read.table("odap_digi_sdm_l2.web_visits")

# COMMAND ----------

products = ["investice", "pujcky", "hypoteky"]

# COMMAND ----------

date_end = dt.datetime.strptime(dbutils.widgets.get("timestamp"), "%Y-%m-%d")
date_start = date_end - dt.timedelta(days=90)

# COMMAND ----------

# the resulting dataframe must be called df_final
df_final = (
    df
    .filter(f.col("visit_timestamp").between(date_start, date_end))
    .select(
        "customer_id",
        "url",
        "visit_timestamp",
        *[f.lower("url").contains(product).cast("integer").alias(f"{product}_flag") for product in products]
    )
    .groupby("customer_id")
    .agg(
        *[f.sum(f"{product}_flag").alias(f"{product}_web_visits_count_in_last_90_days") for product in products]
    )
    .withColumn("timestamp", f.lit(dbutils.widgets.get("timestamp")))
)
