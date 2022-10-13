-- Databricks notebook source
create widget text timestamp default "2020-12-12"

-- COMMAND ----------

select
  customer_id,
  timestamp(getargument("timestamp")) as timestamp,
  customer_email
from
  hive_metastore.odap_offline_sdm_l2.customer
