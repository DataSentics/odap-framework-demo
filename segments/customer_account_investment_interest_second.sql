-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW max_date AS (SELECT max(last_compute_date) AS max_date FROM odap_features.metadata_customer);

-- COMMAND ----------

SELECT
  c.customer_id,
  a.account_id
FROM
  odap_features.features_customer AS c
INNER JOIN
  odap_features.features_account AS a
ON
  c.customer_id == a.customer_id
FULL JOIN 
  max_date
WHERE
  c.investice_web_visits_count_in_last_90d > 1 AND
  a.incoming_transactions_sum_amount_in_last_90d >= 270000 AND
  c.timestamp == max_date
