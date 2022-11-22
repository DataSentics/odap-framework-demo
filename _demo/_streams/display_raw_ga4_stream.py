# Databricks notebook source
spark.readStream.table("odap_raw_ga4.events_stream").display()
