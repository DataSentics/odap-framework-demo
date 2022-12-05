-- Databricks notebook source
SELECT
  customer_id
FROM
  odap_features.features_customer_latest
WHERE
  pujcky_web_visits_count_in_last_30d > 0
