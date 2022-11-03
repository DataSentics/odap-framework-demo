# Databricks notebook source
# MAGIC %pip install odap==0.0.4

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text target default "no target";
# MAGIC create widget text timestamp default "2020-12-12"

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

from odap.feature_factory.orchestrate import orchestrate

orchestrate()
