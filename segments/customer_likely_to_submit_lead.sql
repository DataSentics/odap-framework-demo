-- Databricks notebook source
SELECT 
  customer_id 
FROM
  odap_features.features_customer
WHERE
  ai_lead_submit_estimation > 0.9
