# Databricks notebook source
# MAGIC %pip install odap==0.0.1

# COMMAND ----------

import datetime as dt
from typing import List
from pyspark.sql import functions as f
from odap.feature_factory import time_windows as tw

# COMMAND ----------

dbutils.widgets.text("timestamp", "")

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

# MAGIC %md
# MAGIC # Metadata
# MAGIC ## investice_web_visits_count_in_last_14d	
# MAGIC - description: "Investice web visits count in last 14 days"
# MAGIC ## pujcky_web_visits_count_in_last_14d	
# MAGIC - description: "Pujcky web visits count in last 14 days"
# MAGIC ## hypoteky_web_visits_count_in_last_14d
# MAGIC - description: "Hypoteky web visits count in last 14 days"
# MAGIC ## investice_web_visits_count_in_last_30d	
# MAGIC - description: "Investice web visits count in last 30 days"
# MAGIC ## pujcky_web_visits_count_in_last_30d	
# MAGIC - description: "Pujcky web visits count in last 30 days"
# MAGIC ## hypoteky_web_visits_count_in_last_30d	
# MAGIC - description: "Hypoteky web visits count in last 30 days"
# MAGIC ## investice_web_visits_count_in_last_90d	
# MAGIC - description: "Investice web visits count in last 90 days"
# MAGIC ## pujcky_web_visits_count_in_last_90d	
# MAGIC - description: "Pujcky web visits count in last 90 days"
# MAGIC ## hypoteky_web_visits_count_in_last_90d
# MAGIC - description: "Hypoteky web visits count in last 90 days"

# COMMAND ----------

df_final = wdf.time_windowed(group_keys=["customer_id", "timestamp"], agg_columns_function=product_agg_features)
