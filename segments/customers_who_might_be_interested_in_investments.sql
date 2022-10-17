-- Databricks notebook source
select
  customer_id
from
  hive_metastore.odap_features.features_customer
where
  transactions_sum_amount_in_last_30d >= 50000 and
  investice_web_visits_count_in_last_90d > 0
