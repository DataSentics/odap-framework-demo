# Databricks notebook source
import os

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS odap_offline_sdm_l2")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_digi_sdm_l2")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_features")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_segments")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_targets")

# COMMAND ----------

card_transactions = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/card_transactions.parquet")
customer = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/customer.parquet")
web_visits = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/web_visits.parquet")
target_store = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/target_store.parquet")
accounts = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/accounts.parquet")

# COMMAND ----------

card_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_offline_sdm_l2.card_transactions")
customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_offline_sdm_l2.customer")
web_visits.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_digi_sdm_l2.web_visits")
target_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_targets.targets")
accounts.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_features.features_account")
