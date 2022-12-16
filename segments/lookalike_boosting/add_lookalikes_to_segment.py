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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create widgets

# COMMAND ----------

dbutils.widgets.text("entity_id_column_name", "customer_id")
dbutils.widgets.text("entity_name", "customer")
dbutils.widgets.text("latest_date", "2022-09-30")
dbutils.widgets.text("model_uri", "runs:/1ffc9dd4c3834751b132c70df455a00d/pipeline")
dbutils.widgets.text("segment_name", "customers_likely_to_churn")

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
run_id = dbutils.widgets.get("model_uri").split("/")[1]

#specify path for the feature names logged in your mlflow experiment
features = mlflow.artifacts.load_text(f"dbfs:/databricks/mlflow-tracking/21eba1de169f4aabb0c709f2f34475ed/{run_id}/artifacts/features.txt")

feature_store_features = [
    feature for feature in features if feature not in ["intercept", "intercept_vector"]
]
        
df_final = predict(df_inference_dataset, model, "probability_of_lookalike")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create widgets for filtering lookalikes

# COMMAND ----------

criterion_choice = widgets.RadioButtons(
    options=['probability_threshold', 'lookalike_ids_count'],
    disabled=False,
    description="Filter criterion"
)

criterion_choice

# COMMAND ----------

def return_slider(criterion):
    if (criterion == "probability_threshold"):
        slider = widgets.FloatSlider(max=1.0, min=0.0,  step=0.01, value=0.5)
    else:
        slider = widgets.IntSlider(min=100, max=100000, step=100, value=500)
    return slider

# COMMAND ----------

dbutils

# COMMAND ----------

# DBTITLE 1,Interactive way to display number of lookalikes or lowest probability
# Please note that it takes time for result to display (several seconds), progress bar or something might be added later
@widgets.interact(value=return_slider(criterion_choice.value).value, fit=True)
def define_lookalikes(value):
    print(f"Result for the value {value} is being computed.")

    if (isinstance(value, float)):
        df_lookalikes = (
            df_final.sort("probability_of_lookalike", ascending=False)
            .filter(
                f.col("probability_of_lookalike") >= value
            )
            .select(dbutils.widgets.get("entity_id_column_name"))
        )
        output_string = f"Number of lookalikes to add based on probability threshold: {df_lookalikes.count()}"
    else:
        df_lookalikes = (
            df_final.sort("probability_of_lookalike", ascending=False)
            .limit(value)
            .select(dbutils.widgets.get("entity_id_column_name"), "probability_of_lookalike")
        )

        lowest_prob = round(df_lookalikes.select(f.min("probability_of_lookalike")).collect()[0][0], 4)

        output_string = f"The lowest probability of the lookalike subject admitted: {lowest_prob}"
    return output_string

# COMMAND ----------

# DBTITLE 1,Apply the criterion to choose number of lookalikes to add to the segment
if criterion_choice.value == "probability_threshold":
    df_lookalikes = (
        df_final.sort("probability_of_lookalike", ascending=False)
        .filter(
            f.col("probability_of_lookalike") >= slider.value
        )
        .select(dbutils.widgets.get("entity_id_column_name"))
    )
    print(f"Number of lookalikes to add based on probability threshold: {df_lookalikes.count()}")
else:
    df_lookalikes = (
        df_final.sort("probability_of_lookalike", ascending=False)
        .limit(slider.value)
        .select(dbutils.widgets.get("entity_id_column_name"), "probability_of_lookalike")
    )

    lowest_prob = round(df_lookalikes.select(f.min("probability_of_lookalike")).collect()[0][0], 4)

    print(f"The lowest probability of the lookalike subject admitted: {lowest_prob}")

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
#df_lookalikes.write.format("delta").save("__SPECIFY_DESTINATION__")
