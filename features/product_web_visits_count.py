# Databricks notebook source
# MAGIC %pip install ../_packages/odap-1.0.0-py3-none-any.whl

# COMMAND ----------

import datetime as dt
from typing import List
from pyspark.sql import functions as f
from odap.feature_factory import time_windows as tw

# COMMAND ----------

dbutils.widgets.text("timestamp", "2020-12-12")

# COMMAND ----------

products = ["investice", "pujcky", "hypoteky"]

# COMMAND ----------

time_windows = ["14d", "30d", "90d"]

# COMMAND ----------

wdf = tw.WindowedDataFrame(
    df=spark.read.table("odap_digi_sdm_l2.web_visits"),
    time_column="visit_timestamp",
    time_windows=time_windows,
).withColumn("timestamp", f.lit(dbutils.widgets.get("timestamp")).cast("timestamp"))

# COMMAND ----------

def product_agg_features(time_window: str) -> List[tw.WindowedColumn]:    
    return [
        tw.sum_windowed(
            f"{product}_web_visits_count_in_last_{time_window}",
            f.lower("url").contains(product).cast("integer"),
        )
        for product in products
    ]

# COMMAND ----------

df_final = wdf.time_windowed(group_keys=["customer_id", "timestamp"], agg_columns_function=product_agg_features)
