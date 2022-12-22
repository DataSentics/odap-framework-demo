-- Databricks notebook source
-- MAGIC %run ../init/target_store $timeshift=3

-- COMMAND ----------

-- MAGIC %run ../init/window_functions

-- COMMAND ----------

create widget text timestamp default "";
create widget text target default "no target"

-- COMMAND ----------

create
or replace temporary view card_transactions as (
  select
    *
  from
    hive_metastore.odap_offline_sdm_l2.card_transactions
    join target_store using (customer_id)
  where
    process_date <= timestamp(getargument("timestamp"))
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metadata = {
-- MAGIC     "category": "transactions",
-- MAGIC     "location": "product_features",
-- MAGIC     "features": {
-- MAGIC         "transactions_sum_amount_in_last_{time_window}": {
-- MAGIC             "description": "Total volume of transactions in last {time_window}",
-- MAGIC         }
-- MAGIC     }
-- MAGIC }

-- COMMAND ----------

select
  customer_id,
  timestamp,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "30 days")) as transactions_sum_amount_in_last_30d,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "60 days")) as transactions_sum_amount_in_last_60d,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "90 days")) as transactions_sum_amount_in_last_90d
from
  card_transactions
group by
  customer_id, timestamp
