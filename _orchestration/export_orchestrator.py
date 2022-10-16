# Databricks notebook source
# MAGIC %pip install ../_packages/odap-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %pip install ../_packages/odap-1.0.0-py3-none-any.whl

# COMMAND ----------

from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_segments, get_exports
from odap.segment_factory.exports import run_export

# COMMAND ----------

config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)
segments = list(get_segments(config).keys())
exporters = list(get_exports(config).keys())


dbutils.widgets.dropdown(
  name='segment_name',
  defaultValue=segments[0],
  choices=segments,
  label='1. Segment'
)

dbutils.widgets.dropdown(
  name='export_name',
  defaultValue=exporters[0],
  choices=exporters,
  label='2. Export'
)

# COMMAND ----------

segment_name = dbutils.widgets.get("segment_name")
export_name = dbutils.widgets.get("export_name")

run_export(segment_name=segment_name, export_name=exporter)

