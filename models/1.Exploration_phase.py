# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** The first notebook contains functionality to create, explore and choose features of your model dataset.  
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, setup of widgets, definition of functions  
# MAGIC **Loading and preparation of the dataset** = retrieve needed data from the feature store to create the model dataset  
# MAGIC **Exploration of the dataset**  = explore the list of features and their descriptive statistics/metadata, perform feature selection and explore the feature's relationship with the target
# MAGIC 
# MAGIC **Goal:** At the end of the notebook, your model dataset should be prepared for the next (training) phase.
# MAGIC 
# MAGIC **Tip:** If you need help on datasciencefunctions library, please use the help() functionality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for the notebook

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/datasciencefunctions-0.2.0-py3-none-any.whl

# COMMAND ----------

# DBTITLE 1,Import libraries
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
import datetime as dt
from lib.profiling import profile_features
import pandas as pd
from pyspark.sql import DataFrame, functions as f
from pyspark.mllib.stat import Statistics

# COMMAND ----------

# DBTITLE 1,Create widgets
dbutils.widgets.text("target", "")

# COMMAND ----------

# DBTITLE 1,Functions
def compute_correlation_matrix(df: DataFrame, method="pearson"):
    df_rdd = df.rdd.map(lambda row: row[0:])
    corr_mat = Statistics.corr(df_rdd, method=method)
    corr_mat_df = pd.DataFrame(corr_mat, columns=df.columns, index=df.columns)
    return corr_mat_df


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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading data

# COMMAND ----------

# MAGIC %md
# MAGIC This part utilizes functionalities of the Feature Store (centralized repository for features) to create the model dataset.  
# MAGIC 
# MAGIC The model dataset should contain two groups of observations:  
# MAGIC 
# MAGIC **1. Observations that belong to the target group**
# MAGIC * these carried out the event specified as the target
# MAGIC * these will get the 1 as the value of the label column
# MAGIC * we are interested in their behaviour before the target happened
# MAGIC 
# MAGIC **2. Observations that belong to the non-target group** 
# MAGIC * these have NOT carried out the event specified as the target
# MAGIC * these will get the 0 as the value of the label column
# MAGIC * we are interested in their most actual behaviour available in the Feature Store
# MAGIC 
# MAGIC Utilizing the union_target_latest_features function results in the union of these two particular groups of observations = the model dataframe
# MAGIC 
# MAGIC More about how to configure the feature store and its functionalities can be found here.

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

# DBTITLE 1,Get the latest snapshot of the feature store
latest_features = spark.read.table("odap_feature_store.features_client").filter(f.col("timestamp") == dt.datetime(2022, 8, 30)).select(
    "cookie_id", "timestamp", *all_features
)
latest_features.display()

# COMMAND ----------

for i in range(len(latest_features.columns)):
    if("MapType" in str(latest_features.schema[i].dataType)):
        latest_features = latest_features.withColumn(str(latest_features.schema[i].name), f.col(str(latest_features.schema[i].name))[0])

# COMMAND ----------

# DBTITLE 1,Get the target snapshot of the feature store
target_store = spark.read.table("odap_feature_store.targets_client").filter(f.col("target_id") == dbutils.widgets.get("target")).filter(f.col("timestamp").between(dt.date(2022, 1, 1), dt.date(2022, 8, 30)))
feature_store_full = spark.read.table("odap_feature_store.features_client")

target_features = feature_store_full.join(target_store, on=["cookie_id", "timestamp"])

# COMMAND ----------

for i in range(len(target_features.columns)):
    if("MapType" in str(target_features.schema[i].dataType)):
        target_features = target_features.withColumn(str(target_features.schema[i].name), f.col(str(target_features.schema[i].name))[0])

# COMMAND ----------

# DBTITLE 1,Join the latest and target snapshot to create the model dataset
union_data_df = union_target_latest_features(
    target_features, latest_features, "cookie_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata and data profiling

# COMMAND ----------

union_data_df.display()

# COMMAND ----------

# DBTITLE 1,Fill null values with other values
union_data_df = union_data_df.fillna(0).fillna("unknown")

# COMMAND ----------

# DBTITLE 1,Create dataframe with statistics about features
profile_features(union_data_df, features_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration
# MAGIC 
# MAGIC Explore features in their relationship to the target:
# MAGIC * Use metadata table to get an idea what features are available and how to understand them  
# MAGIC * Use features statistics and data profiling to find out basic information about each feature  
# MAGIC * Use correlation matrix to execute feature selection in order to decide what features could be best for modelling  

# COMMAND ----------

display(union_data_df.groupBy("label").count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can use correlation matrix of available features to perform feature selection with the functionality from datasciencefunctions library
# MAGIC The method:
# MAGIC * favors those features highly correlated with the target
# MAGIC * penalizes those features highly correlated with other features

# COMMAND ----------

df_pandas = union_data_df.sample(0.1).toPandas()

# COMMAND ----------

# DBTITLE 1,Compute the Correlation Matrix
df_corr = df_pandas[numerical_features + ["label"]].corr()
df_corr

# COMMAND ----------

# DBTITLE 1,Feature selection based on the Correlation Matrix
selected_features, feature_selection_history = feature_selection_merits(
    features_correlations=df_corr.loc[
        df_corr.columns != "label", df_corr.index != "label"
    ].abs(),
    target_correlations=df_corr.loc[
        df_corr.columns == "label", df_corr.index != "label"
    ].abs(),
    algorithm="forward",
    max_iter=20,
    best_n=3,
)

selected_features

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can use datasciencefunctions to obtain a plot which gives us a good overview of each feature and its relationship with the target.
# MAGIC It visualises:
# MAGIC * the frequencies of values of all features (numeric features are binned) as well as their basic statistics (min, max, mean)
# MAGIC * how the target value depends on the value of each individual feature

# COMMAND ----------

plot_feature_hist_with_binary_target(
    df=union_data_df,
    target_col="label",
    num_cols=["web_analytics_channel_device_count_distinct_30d"],
    #cat_cols=categorical_features,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save training data permanently

# COMMAND ----------

union_data_df.select("label", *all_features).write.mode("overwrite").format("delta").saveAsTable("odap_datasets.lead_submit_training_data")
