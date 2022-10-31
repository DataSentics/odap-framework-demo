-- Databricks notebook source
select
  c.customer_id,
  a.account_id
from
  hive_metastore.odap_features.features_customer AS c
inner join
  hive_metastore.odap_features.features_account AS a
on
  c.customer_id == a.customer_id
where
  c.investice_web_visits_count_in_last_90d > 0 and
  a.incoming_transactions_sum_amount_in_last_90d >= 200000
