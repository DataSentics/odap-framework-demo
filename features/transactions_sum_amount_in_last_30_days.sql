-- Databricks notebook source
create widget text timestamp default "2020-12-12"

-- COMMAND ----------

create or replace temporary view card_transactions as (
  select
    *
  from
    hive_metastore.odap_offline_sdm_l2.card_transactions
  where
    process_date <= to_timestamp(getargument("timestamp"))
)

-- COMMAND ----------

select
  customer_id,
  to_timestamp(getargument("timestamp")) as timestamp,
  sum(amount_czk) as transactions_sum_amount_in_last_30d
from
  card_transactions
where
  process_date between date(getargument("timestamp")) - interval 30 days and date(getargument("timestamp"))
group by
  customer_id
