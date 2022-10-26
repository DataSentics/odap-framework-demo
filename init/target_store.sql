-- Databricks notebook source
create widget text timestamp default ""

-- COMMAND ----------

create widget text target default "no target"

-- COMMAND ----------

create
or replace temporary view target_store as
select
  *
from
  (
    select
      *
    from
      -- insert your targets table here - schema [id, timestamp, target]
      odap_targets.targets
    union all
    select
      customer_id,
      timestamp(getargument("timestamp")) as timestamp,
      "no target" as target
    from
      -- insert a table containing all entity ids
      odap_offline_sdm_l2.customer
  )
where
  target = getargument("target")
