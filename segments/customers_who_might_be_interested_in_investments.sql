-- Databricks notebook source
create widget text timestamp default "2020-12-12"

-- COMMAND ----------

select
  customer_id,
  timestamp
from
  hive_metastore.odap_features.customer
where
  timestamp = date(getargument("timestamp")) and
  sum_amount_in_last_30_days > 50000 and
  investice_web_visits_count_in_last_90_days > 0
