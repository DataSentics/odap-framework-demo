# Databricks notebook source
# MAGIC %md #### Run notebooks for initial setup

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %md #### Imports

# COMMAND ----------

from pyspark.sql import Column, functions as f
from odap.common.functions import most_common
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

df_source = spark.table("odap_features.features_customer").select("customer_id", "timestamp")

# COMMAND ----------

# MAGIC %md ### Metadata

# COMMAND ----------

metadata = {
    "category": "digital_ai",
    "features": {
        "ai_lead_submit_estimation": {
            "description": "Probability that the entity will submit a lead based on the trained AI model.",
            "fillna_with": 0.5
        },
    }
}

# COMMAND ----------

# MAGIC %md ### Feature engineering

# COMMAND ----------

df_final = df_source.withColumn("ai_lead_submit_estimation", f.round(f.rand(seed = 42), 3))
