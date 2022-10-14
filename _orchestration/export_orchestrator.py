# Databricks notebook source
# MAGIC %pip install ../_packages/odap-1.0.0-py3-none-any.whl

# COMMAND ----------

from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_segments, get_exporters
from odap.segment_factory.exports import run_export

# COMMAND ----------

config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)
segments = list(get_segments(config).keys())
exporters = list(get_exporters(config).keys())

dbutils.widgets.dropdown(
  name='segment',
  defaultValue=segments[0],
  choices=segments,
  label='Segment'
)

dbutils.widgets.dropdown(
  name='exporter',
  defaultValue=exporters[0],
  choices=exporters,
  label='Exporter'
)



# COMMAND ----------

segment = dbutils.widgets.get("segment")
exporter = dbutils.widgets.get("exporter")

run_export(segment_name=segment, exporter=exporter)

