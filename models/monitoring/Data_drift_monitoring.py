# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** This notebook contains functionality to load preferred model and data to make the prediction.
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, definition of functions   
# MAGIC **Using quick insights for detecting data drift** = run the script used for calculating necessary parameters  
# MAGIC **Log results to MlFlow** = log significant differences in features to mlflow
# MAGIC 
# MAGIC **Goal:** At the end of the notebook, you have obtained logged siginificant feature differences in MlFlow
# MAGIC 
# MAGIC **Tip:** If you need help on datasciencefunctions library, please use the help() functionality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for the notebook

# COMMAND ----------

# MAGIC %pip install --force-reinstall /dbfs/FileStore/odap_widgets-1.0.0-py3-none-any.whl

# COMMAND ----------

token = dbutils.secrets.get(scope="key-vault", key="dbx-token")

# COMMAND ----------

import mlflow
from odap.main import show
from pyspark.sql import functions as f

# COMMAND ----------

res = show(
    show_display=False,
    features_table="odap_datasets.qi_upsell_leads_sample",
    segments_table="hive_metastore.odap_app_main.segments",
    destinations_table="hive_metastore.odap_app_main.destinations",
    data_path='["dbfs:/tmp/sb/sb_odap_feature_store/datasets/data_for_monitoring.delta"]',
    databricks_host="https://adb-8406481409439422.2.azuredatabricks.net",
    databrics_token=str(token),
    cluster_id="0511-092604-a9jb3hrb",
    notebook_path=(
        "/Repos/persona/skeleton-databricks/src/__myproject__/_export/p360_export"
    ),
    lib_version="0.1.3",
    stats_table="odap_datasets.stats_with_label",
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # How to use

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Specify conditions to set two groups for comparison

# COMMAND ----------

conds = ['(label == 1)', '(label == 0)']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run quick insights to detect data drift

# COMMAND ----------

res.set_condition(obs_cond=conds[0],base_cond=conds[1]) 
qi_results = res.get_qi()
for item in qi_results:
    item["rate"] = float(item["rate"])
    item["distances"] = float(item["distances"])
df_qi_results = spark.createDataFrame(qi_results)

# COMMAND ----------

df_qi_results = spark.table("odap_datasets.qi_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log data drift monitoring to MlFlow

# COMMAND ----------

# DBTITLE 1,Setting up mlflow experiment for monitoring
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/ai_probab_lead_monitoring')

# COMMAND ----------

dfp_qi_results_for_logging = df_qi_results.filter(
    f.col("id").isin(
        [
            "web_analytics_time_on_site_avg_90d",
            "web_analytics_mobile_user_30d",
            "web_analytics_events_sum_change_14d_30d",
        ]
    )
).toPandas()

# COMMAND ----------

feature_records = dfp_qi_results_for_logging.to_dict(orient="records")

with mlflow.start_run():
    for i in range(len(dfp_qi_results_for_logging)):
        feature_name = feature_records[i]["id"]

        for key, value in feature_records[i].items():
            if key not in ["id", "obvious", "true_rate"]:
                mlflow.log_metric(feature_name + "_" + key, float(value))
