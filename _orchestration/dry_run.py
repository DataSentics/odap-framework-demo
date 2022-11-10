# Databricks notebook source
# MAGIC %pip install odap==0.0.4

# COMMAND ----------

from odap.feature_factory.dry_run import dry_run, create_notebook_widget

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text target default "no target";
# MAGIC create widget text timestamp default "2021-12-12"

# COMMAND ----------

create_notebook_widget()

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %run ../init/window_functions

# COMMAND ----------

dry_run()
