# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** This notebook contains functionality to load preferred model and data to make the prediction.
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, setup of widgets  
# MAGIC   **Loading of the dataset** = load the feature store snapshot for prediction   
# MAGIC **Prediction phase** = predict the results with using the selected models and loaded dataset 
# MAGIC 
# MAGIC **Goal:** At the end of the notebook, you have obtained a dataframe with predictions created by your selected model 
# MAGIC 
# MAGIC **Tip:** If you need help on datasciencefunctions library, please use the help() functionality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for the notebook

# COMMAND ----------

# DBTITLE 1,Run setup functionality
# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# DBTITLE 1,Import libraries
import datetime as dt
import mlflow
from pyspark.sql import DataFrame, functions as f
from pyspark.ml.functions import vector_to_array

from lib.utils_mlflow import get_artifact_json_from_mlflow
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel

# COMMAND ----------

# DBTITLE 1,Create widgets
dbutils.widgets.text("feature_name", "")
dbutils.widgets.text("model_uri", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction phase
# MAGIC After loading the dataset, we can move to the prediction phase
# MAGIC * Firstly, ensure that you've specified values in the widgets:  
# MAGIC   **feature_name** = simply the name of the column that will contain predictions  
# MAGIC   **model** = you must provide the widget with the run_id of an experiment (find it in the MLFlow) for the chosen model in the format that follows: runs:/{run_id}/pipeline  
# MAGIC * Then, based on your model flavour (spark, scikit-learn), choose the particular cell to create predictions  
# MAGIC * Results are displayed after the run of the cell

# COMMAND ----------

def predict(features: DataFrame, model: PipelineModel, feature_name: str):
    predictions_df = model.transform(features)

    if "probability" in predictions_df.columns:
        return predictions_df.select(
            "cookie_id",
            f.round(
                f.element_at(vector_to_array(f.col("probability")), 2).cast("float"), 3
            ).alias(feature_name),
        )

    return predictions_df.select(
        "cookie_id",
        f.round("prediction", 3).alias(feature_name),
    )

# COMMAND ----------

# MAGIC %md ## Use the next cell only for spark models

# COMMAND ----------

model_uri = dbutils.widgets.get("model_uri")
feature_name = dbutils.widgets.get("feature_name")

model = mlflow.spark.load_model(model_uri)
features = get_artifact_json_from_mlflow(model_uri, True)
feature_store_features = [
    feature for feature in features if feature not in ["intercept", "intercept_vector"]
]

df_features = spark.read.table("odap_feature_store.features_client").filter(f.col("timestamp") == dt.datetime(2022, 9, 30))#.select(#*feature_store_features#)

for i in range(len(df_features.columns)):
    if("MapType" in str(df_features.schema[i].dataType)):
        df_features = df_features.withColumn(str(df_features.schema[i].name), f.col(str(df_features.schema[i].name))[0])
        
df_features = df_features.fillna(0).fillna("unknown")
        
df_final = predict(df_features, model, feature_name)
df_final.display()
