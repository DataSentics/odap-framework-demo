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
import ipywidgets as widgets
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create widgets

# COMMAND ----------

criterion_choice = widgets.RadioButtons(
    options=['probability_threshold', 'lookalike_ids_count'],
    disabled=False,
    description="Filter criterion"
)

# COMMAND ----------

count_lookalikes_slider = widgets.IntSlider(min=100, max=100000, step=100, value=5000)

# COMMAND ----------

probability_slider = widgets.FloatSlider(max=1.0, min=0.0,  step=0.01, value=0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use widgets to chose values for lookalike segment definition

# COMMAND ----------

probability_slider

# COMMAND ----------

probability_slider.value

# COMMAND ----------

count_lookalikes_slider

# COMMAND ----------

count_lookalikes_slider.value

# COMMAND ----------

criterion_choice

# COMMAND ----------

criterion_choice.value

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

# COMMAND ----------

# DBTITLE 1,Load model pipeline for lookalike estimation
#mlflow destination

# COMMAND ----------

# DBTITLE 1,Create lookalike list


# COMMAND ----------

# DBTITLE 1,Take chosen count of lookalikes


# COMMAND ----------

# DBTITLE 1,Union with existing segment
# HOW TO ENSURE THAT WE CAN RECOGNIZE THIS? OR SAVE IT AS NEW SEGMENT_NAME WITH + "_BOOSTED"

# COMMAND ----------

#normalization ["Normalizer", "StandardScaler", "MinMaxScaler"]

#model, model_approach (hyperopt and models), hyperopt metric ["f1", "lift_10", "lift_100", "r2", "rmse", "weighted_precision", "weighted_recall"]

#downsampling
#upsampling
#downsample_share
#target_ratio

#choice of features
