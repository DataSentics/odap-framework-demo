# Databricks notebook source
# MAGIC %md #### Run notebooks for initial setup

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %md #### Imports

# COMMAND ----------

from pyspark.sql import functions as f
from odap.feature_factory import time_windows as tw
from typing import List

# COMMAND ----------

# MAGIC %md #### Create widgets

# COMMAND ----------

dbutils.widgets.text("timestamp", "")
dbutils.widgets.text("target", "")
dbutils.widgets.text("entity_column_name", "")

# COMMAND ----------

entity_id_column_name = dbutils.widgets.get("entity_column_name")

# COMMAND ----------

# MAGIC %md #### Source data

# COMMAND ----------

wdf_digi_sdm = tw.WindowedDataFrame(
    df=spark.read.table("dev_new_csas_odap_digi_sdm_l2.digi_sdm"),
    time_column="event_date",
    time_windows=["14d", "30d", "90d"],
)

# COMMAND ----------

digi_sdm = wdf_digi_sdm.join(
    spark.read.table("target_store"), on=entity_id_column_name, how="inner"
).filter(f.col("event_date") <= f.col("timestamp"))

# COMMAND ----------

# MAGIC %md ### Metadata

# COMMAND ----------

metadata = {
    "category": "digital_campaign",
    "features": {
        "web_analytics_traffic_medium_most_common_{time_window}": {
            "description": "Most common traffic medium (direct, paid, ..) the client is acquired by in last {time_window}.",
            "fillna_with": None
        },
    }
}

# COMMAND ----------

# MAGIC %md ### Feature engineering

# COMMAND ----------

def traffic_source_medium_agg_columns(time_window: str) -> List[tw.WindowedColumn]:
    return [
        tw.count_windowed(
            f"freq_{time_window}",
            f.when(f.col("traffic_source_medium").isin(["organic", "cpc", "referral", "cpm", "e-mail", "paidsocial", "social", "sms", "logout_banner"]), f.col("traffic_source_medium")).otherwise("(none)"),
        ),
    ]

# COMMAND ----------

web_visit_with_session_channel_freq_df = digi_sdm.time_windowed(
    agg_columns_function=traffic_source_medium_agg_columns,
    group_keys=[
        entity_id_column_name,
        "traffic_source_medium",
        "timestamp",
        "session_id",
    ],
)

# COMMAND ----------

def most_common_channel_agg_columns(time_window: str) -> List[Column]:
    return [
        most_common(
            f"most_common_traffic_medium_struct_{time_window}",
            f.col(f"freq_{time_window}").alias(f"freq_{time_window}"),
            f.hash(entity_id_column_name, "timestamp", "traffic_source_medium").alias(f"random_number_{time_window}"),
            f.when(f.col(f"freq_{time_window}") > 0, f.col("traffic_source_medium")).alias(f"web_analytics_traffic_medium_most_common_{time_window}"),
        )
    ]

# COMMAND ----------

cols_to_drop = web_visit_with_session_channel_freq_df.get_windowed_column_list(["freq_{time_window}", "random_number_{time_window}"])

# COMMAND ----------

df_final = web_visit_with_session_channel_freq_df.time_windowed(
    agg_columns_function=most_common_channel_agg_columns,
    group_keys=[entity_id_column_name, "timestamp"],
    unnest_structs=True,
).drop(*cols_to_drop)
