-- Databricks notebook source
create widget text timestamp default ""

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metadata = {
-- MAGIC     "category": "personal",
-- MAGIC     "features": {
-- MAGIC         "customer_email": {
-- MAGIC             "description": "User's email",
-- MAGIC             "tags": ["email", "sensitive"],
-- MAGIC         }
-- MAGIC     }
-- MAGIC }

-- COMMAND ----------

select
  customer_id,
  timestamp(getargument("timestamp")) as timestamp,
  customer_email
from
  hive_metastore.odap_offline_sdm_l2.customer
