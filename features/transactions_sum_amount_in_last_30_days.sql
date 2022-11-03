-- Databricks notebook source
-- MAGIC %run ../init/target_store

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
-- MAGIC     "features": {
-- MAGIC         "transactions_sum_amount_in_last_30d": {
-- MAGIC             "description": "Total volume of transactions in last 30 days",
-- MAGIC         }
-- MAGIC     }
-- MAGIC }

-- COMMAND ----------

select
  customer_id,
  timestamp(getargument("timestamp")) as timestamp,
  sum(amount_czk) as transactions_sum_amount_in_last_30d
from
  card_transactions
where
  process_date between date(getargument("timestamp")) - interval 30 days and date(getargument("timestamp"))
group by
  customer_id
