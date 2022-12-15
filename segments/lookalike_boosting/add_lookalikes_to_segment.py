# Databricks notebook source
# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

import datetime as dt
import ipywidgets as widgets
import mlflow
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, functions as f
from segments.lookalike_boosting.ml_functions import get_artifact_json_from_mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create widgets

# COMMAND ----------

criterion_choice = widgets.RadioButtons(
    options=['probability_threshold', 'lookalike_ids_count'],
    disabled=False,
    description="Filter criterion"
)

count_lookalikes_slider = widgets.IntSlider(min=100, max=100000, step=100, value=5000)

probability_slider = widgets.FloatSlider(max=1.0, min=0.0,  step=0.01, value=0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use widgets to chose values for lookalike segment definition

# COMMAND ----------

dbutils.widgets.text("entity_id_column_name", "customer_id")
dbutils.widgets.text("entity_name", "customer")
dbutils.widgets.text("latest_date", "2022-09-30")
dbutils.widgets.text("model_uri", "runs:/1ffc9dd4c3834751b132c70df455a00d/pipeline")
dbutils.widgets.text("segment_name", "customers_likely_to_churn")

# COMMAND ----------

probability_slider

# COMMAND ----------

count_lookalikes_slider

# COMMAND ----------

criterion_choice

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_date_parts(date_string):
    return [int(date_part) for date_part in date_string.split("-")]

# COMMAND ----------

def predict(features: DataFrame, model: PipelineModel, feature_name: str):
    predictions_df = model.transform(features)

    if "probability" in predictions_df.columns:
        df_output = predictions_df.select(
            dbutils.widgets.get("entity_id_column_name"),
            "timestamp",
            f.round(
                f.element_at(vector_to_array(f.col("probability")), 2).cast("float"), 3
            ).alias(feature_name),
        )
    else:
        df_output = predictions_df.select(
            dbutils.widgets.get("entity_id_column_name"),
            "timestamp",
            f.round("prediction", 3).alias(feature_name),
        )
    return df_output


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data

# COMMAND ----------

latest_year, latest_month, latest_day = get_date_parts(
    dbutils.widgets.get("latest_date")
)

df_data = spark.table(
    f"odap_digi_features.features_{dbutils.widgets.get('entity_name')}"
).filter(f.col("timestamp") == dt.date(latest_year, latest_month, latest_day))

df_to_enrich = (
    spark.table("odap_digi_use_case_segments.segments")
    .select(dbutils.widgets.get("entity_id_column_name"))
    .filter(f.col("segment") == dbutils.widgets.get("segment_name"))
)

df_inference_dataset = df_data.join(
    df_to_enrich,
    on=dbutils.widgets.get("entity_id_column_name"),
    how="anti",
).withColumn("label", f.lit(0))

# COMMAND ----------

# DBTITLE 1,Load model pipeline for lookalike estimation
model = mlflow.spark.load_model(dbutils.widgets.get("model_uri"))

features = ["web_analytics_traffic_medium_most_common_90d", "web_analytics_loan_visits_count_30d", "web_analytics_time_on_site_avg_90d"]
#je potrebné dať logovať coefficients
#features = get_artifact_json_from_mlflow(dbutils.widgets.get("model_uri"))

feature_store_features = [
    feature for feature in features if feature not in ["intercept", "intercept_vector"]
]
        
df_final = predict(df_inference_dataset, model, "probability_of_lookalike")

# COMMAND ----------

# DBTITLE 1,Create lookalike list
df_final.display()

# COMMAND ----------

# DBTITLE 1,Apply the criterion to choose number of lookalikes to add to the segment
if criterion_choice.value == "probability_threshold":
    df_lookalikes = (
        df_final.sort("probability_of_lookalike", ascending=False)
        .filter(
            f.col("probability_of_lookalike") >= probability_slider.value
        )
        .select(dbutils.widgets.get("entity_id_column_name"))
    )
else:
    df_lookalikes = (
        df_final.sort("probability_of_lookalike", ascending=False)
        .limit(count_lookalikes_slider.value)
        .select(dbutils.widgets.get("entity_id_column_name"))
    )

# COMMAND ----------

# DBTITLE 1,Union with existing segment
df_origin_segment = spark.table("odap_digi_use_case_segments.segments").filter(
    f.col("segment") == dbutils.widgets.get("segment_name")
)

export_id_origin = df_origin_segment.select("export_id").distinct().collect()[0][0]

df_lookalikes = df_lookalikes.withColumn(
    "export_id", f.lit(export_id_origin)
).withColumn("segment", f.lit("lals_" + dbutils.widgets.get("segment_name")))

# COMMAND ----------

# DBTITLE 1,Save segment to a destination
df_lookalikes.write.format("delta").save("__SPECIFY_DESTINATION__")
