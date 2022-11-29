# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** This notebook contains functionality to load preferred model and data to make the prediction.
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, definition of functions   
# MAGIC **Using quick insights for detecting data drift** =  
# MAGIC **Log results to MlFlow** =  
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

from odap.main import show

# COMMAND ----------

res = show(
    show_display=False,
    features_table="sb_odap_feature_store.upsell_leads_data",
    segments_table="hive_metastore.odap_app_main.segments",
    destinations_table="hive_metastore.odap_app_main.destinations",
    #data_path='["dbfs:/user/hive/warehouse/sb_odap_feature_store.db/union_data_non_delta"]',
    #data_path='["abfss://featurestore@odapczlakeg2dev.dfs.core.windows.net/latest/client.delta"]',
    data_path='["dbfs:/tmp/sb/sb_odap_feature_store/datasets/data_for_monitoring.delta"]',
    databricks_host="https://adb-8406481409439422.2.azuredatabricks.net",
    databrics_token=str(token),
    cluster_id="0511-092604-a9jb3hrb",
    notebook_path=(
        "/Repos/persona/skeleton-databricks/src/__myproject__/_export/p360_export"
    ),
    lib_version="0.1.3",
    stats_table="sb_odap_feature_store.stats_with_label",
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # How to use Quick Insights

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Display saved segments

# COMMAND ----------

conditions = spark.sql("select * from hive_metastore.odap_app_main.segments")
display(conditions)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Select target group and base

# COMMAND ----------

import pyspark.sql.functions as F
base_id = "139cb633-fce9-4398-bd08-fee0d6db3bc5" # "5c062c64-9e86-40ee-be30-b9856d123b12"
target_id = "5909a9c9-5b78-4e83-941f-e2bbc8db4c03" # "96e7e123-0a2f-4c0a-87cc-c480d6cf2e12"
base_conds_ = (conditions
               .filter(F.col("id")==base_id)
              ).collect()
target_conds_ = (conditions
                 .filter(F.col("id")==target_id)
                ).collect()

base_conds = [item.conditions for item in base_conds_]
target_conds = [item.conditions for item in target_conds_]

conds = [base_conds[0], target_conds[0]]

# COMMAND ----------

conds = ['(label == 1)', '(label == 0)']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run quick isnights

# COMMAND ----------

res.set_condition(obs_cond=conds[0],base_cond=conds[1])
res.get_qi()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log data drift monitoring to MlFlow

# COMMAND ----------

# DBTITLE 1,Setting up mlflow experiment for monitoring
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/ai_probab_lead_monitoring')

# COMMAND ----------

# TO-DO: Logic for MlFlow logging
with mlflow.start_run():
    mlflow.log_metrics(monitoring_metrics)
    mlflow.log_metrics(training_metrics)
    mlflow.log_param('Model_name', "ai_lead_estimation_prob")
    mlflow.log_figure(fig, "confusion_matrix.png")
