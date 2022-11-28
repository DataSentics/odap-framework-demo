-- Databricks notebook source
SELECT cookie_id
FROM
  odap_feature_store.features_client
WHERE
  ai_lead_submit_estimation > 0.01
