# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

from odap.feature_factory.dry_run import create_dry_run_widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text target default "no target";
# MAGIC create widget text timestamp default "2020-12-12"

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %run ../init/window_functions

# COMMAND ----------

create_dry_run_widgets()

# COMMAND ----------

from odap.feature_factory.orchestrate import orchestrate

orchestrate()
