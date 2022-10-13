-- Databricks notebook source
create widget text timestamp default "2020-12-12"

-- COMMAND ----------

select
  customer_id
from
  hive_metastore.odap_features.customer
where
  timestamp = timestamp(getargument("timestamp")) and
  transactions_sum_amount_in_last_30d >= 50000 and
  investice_web_visits_count_in_last_90d > 0
