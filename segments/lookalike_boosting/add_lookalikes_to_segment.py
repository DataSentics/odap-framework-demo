# Databricks notebook source
# Databricks notebook source	
# MAGIC %md Steps
# MAGIC - load the segment you need to enrich
# MAGIC - turn the segment into the target
# MAGIC - load the dataset and create label values (load the latest and label those in the segment as ones and those not as zeros)
# MAGIC - load the model for lookalike enrichment
# MAGIC - create list of lookalikes (lift table)
# MAGIC - apply filtering based on widgets
# MAGIC - join with the existing IDs in the segment table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

import datetime as dt
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_date_parts(date_string):
    return [int(date_part) for date_part in date_string.split("-")]

# COMMAND ----------

dbutils.widgets.text("entity_id_column_name", "customer_id")
dbutils.widgets.text("entity_name", "customer")
dbutils.widgets.text("latest_date", "2022-09-30")
dbutils.widgets.text("segment_name", "customers_likely_to_churn")

# COMMAND ----------

latest_year, latest_month, latest_day = get_date_parts(dbutils.widgets.get("latest_date"))

# COMMAND ----------

df_to_enrich = spark.table("odap_segments.segments").filter(f.col("segment") == dbutils.widgets.get("segment_name"))

# COMMAND ----------

df_data = spark.table(f"odap_features.features_{dbutils.widgets.get('entity_name')}").filter(f.col("timestamp") == dt.date(latest_year, latest_month, latest_day))

# COMMAND ----------

# DBTITLE 1,Create model dataset
df_model_dataset = df_data.join(
    df_to_enrich, on=dbutils.widgets.get("entity_id_column_name"), how="inner"
).withColumn("label", f.lit(1)).union(
    df_data.join(
        df_to_enrich, on=dbutils.widgets.get("entity_id_column_name"), how="anti"
    ).withColumn("label", f.lit(0))
)
