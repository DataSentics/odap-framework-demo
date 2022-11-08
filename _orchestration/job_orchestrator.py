# Databricks notebook source
# MAGIC %pip install odap==0.0.4

# COMMAND ----------

from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.exports import run_export

export_name = dbutils.widgets.get("export_name")
segment_name = dbutils.widgets.get("segment_name")

feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

run_export(segment_name, export_name, feature_factory_config, segment_factory_config)
