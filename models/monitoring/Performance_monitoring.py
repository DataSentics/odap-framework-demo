# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** This notebook contains functionality to load preferred model and data to make the prediction.
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, definition of functions   
# MAGIC **Preparing the dataset** = load the latest feature snapshot and target snapshot from the monitored period  
# MAGIC **Prediction** = predict the results with using the model in the production and the monitoring dataset  
# MAGIC **Log performance results in the mlflow** = organize your monitoring neatly in the mlflow experiment dedicated for this purpose
# MAGIC 
# MAGIC **Goal:** At the end of the notebook, you have logged importance metrics to validate that your model performs as expected
# MAGIC 
# MAGIC **Tip:** If you need help on datasciencefunctions library, please use the help() functionality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for the notebook

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/datasciencefunctions-0.2.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# DBTITLE 1,Import libraries
import datetime as dt
import matplotlib.pyplot as plt
import mlflow
from lib.utils_mlflow import get_artifact_json_from_mlflow
from datasciencefunctions.supervised import get_model_metrics
from pyspark.sql import DataFrame, functions as f
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from sklearn.metrics import confusion_matrix

# COMMAND ----------

# DBTITLE 1,Functions
def intersection(lst1, lst2):
    return [value for value in lst1 if value in lst2]


def union_target_latest_features(target, latest, compute_id):
    # creating list of common columns from target-based set of features and latest features
    common_columns = intersection(target.columns, latest.columns)
    # creating list of ids for filtering from latest-based set of features
    list_of_ids = list(target.select(compute_id).toPandas()[compute_id])
    filtered_latest = latest.filter(~f.col(compute_id).isin(list_of_ids))
    return (target[common_columns].withColumn("label", f.lit(1))).unionByName(
        filtered_latest[common_columns].withColumn("label", f.lit(0))
    )


def predict(features: DataFrame, model: PipelineModel, feature_name: str):
    predictions_df = model.transform(features)

    if True:
        return predictions_df

    if "probability" in predictions_df.columns:
        return predictions_df.select(
            "cookie_id",
            f.round(
                f.element_at(vector_to_array(f.col("probability")), 2).cast("float"), 3
            ).alias(feature_name),
        )
    return predictions_df
    predictions_df.select(
        "cookie_id",
        f.round("prediction", 3).alias(feature_name),
    )

# COMMAND ----------

# MAGIC %md ## Widgets

# COMMAND ----------

dbutils.widgets.text("target", "")

# COMMAND ----------

# MAGIC %md ## Preparing the dataset

# COMMAND ----------

features_metadata = (
    spark.read.table("odap_feature_store.metadata_client")
    .filter(f.col("last_compute_date").isNotNull())
    .filter(f.col("variable_type").isin(["numerical", "categorical"]))
)

# COMMAND ----------

numerical_features = (
    features_metadata.filter(f.col("variable_type") == "numerical")
    .toPandas()["feature"]
    .tolist()
)
categorical_features = (
    features_metadata.filter(f.col("variable_type") == "categorical")
    .toPandas()["feature"]
    .tolist()
)

print("Numerical features = ", numerical_features)
print("Categorical features = ", categorical_features)

all_features = numerical_features + categorical_features

# COMMAND ----------

# DBTITLE 1,Data of the last month
latest_features = spark.read.table("odap_feature_store.features_client").filter(f.col("timestamp") == dt.datetime(2022, 9, 30)).select(
    "cookie_id", "timestamp", *all_features
)
latest_features.display()

# COMMAND ----------

for i in range(len(latest_features.columns)):
    if("MapType" in str(latest_features.schema[i].dataType)):
        latest_features = latest_features.withColumn(str(latest_features.schema[i].name), f.col(str(latest_features.schema[i].name))[0])

# COMMAND ----------

target_store = spark.read.table("odap_feature_store.targets_client").filter(f.col("target_id") == dbutils.widgets.get("target")).filter(f.col("timestamp").between(dt.date(2022, 9, 1), dt.date(2022, 10, 1)))
feature_store_full = spark.read.table("odap_feature_store.features_client")

target_features = feature_store_full.join(target_store, on=["cookie_id", "timestamp"])

# COMMAND ----------

for i in range(len(target_features.columns)):
    if("MapType" in str(target_features.schema[i].dataType)):
        target_features = target_features.withColumn(str(target_features.schema[i].name), f.col(str(target_features.schema[i].name))[0])

# COMMAND ----------

monitoring_data_df = union_target_latest_features(
    target_features, latest_features, "cookie_id"
)

# COMMAND ----------

# DBTITLE 1,Fill null values
monitoring_data_df = monitoring_data_df.fillna(0).fillna("unknown")

# COMMAND ----------

# DBTITLE 1,Display the distribution of label
display(monitoring_data_df.groupBy('label').count())

# COMMAND ----------

# DBTITLE 1,Downsampling the majority class
monitoring_data_df = monitoring_data_df.filter(f.col("label") == 1).union(
    monitoring_data_df.filter(f.col("label") == 0).sample(0.01)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction

# COMMAND ----------

# DBTITLE 1,Get the model pipeline
model_pipeline = mlflow.spark.load_model("runs:/70505cba32b9497ba339ff75a633881c/pipeline")

# COMMAND ----------

# DBTITLE 1,Load the model for prediction on the monitoring data
model_uri = "runs:/70505cba32b9497ba339ff75a633881c/pipeline"
feature_name = "ai_lead_submit_prob"

model = mlflow.spark.load_model(model_uri)
features = get_artifact_json_from_mlflow(model_uri)
feature_store_features = [feature for feature in features if feature not in ["intercept", "intercept_vector"]]
df_features = monitoring_data_df.fillna(0).fillna("unknown")

predictions_df = predict(df_features, model, feature_name)

# COMMAND ----------

y_true = [data[0] for data in predictions_df.select("label").collect()]
y_predict = [data[0] for data in predictions_df.select("prediction").collect()]

# COMMAND ----------

# DBTITLE 1,Confusion matrix
conf_matrix = confusion_matrix(y_true=y_true, y_pred=y_predict)
#
# Print the confusion matrix using Matplotlib
#
fig, ax = plt.subplots(figsize=(7.5, 7.5))
ax.matshow(conf_matrix, cmap=plt.cm.Blues, alpha=0.3)
for i in range(conf_matrix.shape[0]):
    for j in range(conf_matrix.shape[1]):
        ax.text(x=j, y=i,s=conf_matrix[i, j], va='center', ha='center', size='xx-large')
 
plt.xlabel('Predictions', fontsize=18)
plt.ylabel('Actuals', fontsize=18)
plt.title('Confusion Matrix', fontsize=18)
plt.show()

# COMMAND ----------

# DBTITLE 1,Performance metrics
new_metrics = get_model_metrics(model_pipeline.stages[-1], predictions_df.drop("prediction", "rawPrediction", "probability"))

# COMMAND ----------

old_metrics = mlflow.get_run("70505cba32b9497ba339ff75a633881c").data.metrics

# COMMAND ----------

training_metrics = {}
for key, value in old_metrics.items():
    training_metrics[key + "_training"] = value

# COMMAND ----------

monitoring_metrics = {}
for key, value in new_metrics.items():
    monitoring_metrics[key + "_monitoring"] = value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log performance monitoring to mlflow

# COMMAND ----------

# DBTITLE 1,Setting up a mlflow experiment for monitoring
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/ai_probab_lead_monitoring')

# COMMAND ----------

# DBTITLE 1,Log metrics, parameters and figures
with mlflow.start_run():
    mlflow.log_metrics(monitoring_metrics)
    mlflow.log_metrics(training_metrics)
    mlflow.log_param('Model_name', "ai_lead_estimation_prob")
    mlflow.log_figure(fig, "confusion_matrix.png")
