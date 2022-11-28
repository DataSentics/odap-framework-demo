# Databricks notebook source
spark.readStream.table("odap_parsed_ga4.events_stream").display()
