# Databricks notebook source
# MAGIC %pip install ../_packages/odap-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text timestamp default "2020-12-12"

# COMMAND ----------

from odap.feature_factory.orchestrate import orchestrate

orchestrate()
