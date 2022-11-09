# Databricks notebook source
import datetime as dt
import time
import random

# COMMAND ----------

clients = [row.customer_id for row in spark.read.table("odap_offline_sdm_l2.customer").limit(1000).collect()]

# COMMAND ----------

while True:
    (
        spark.createDataFrame(
            [
                [random.choice(clients), dt.datetime.now(), random.choice([True, False])]
            ],
            ["customer_id", "event_timestamp", "notification_consent"]
        )
        .write
        .mode("append")
        .saveAsTable("odap_digi_sdm_l2.notification_consent_stream")
    )

    time.sleep(random.randint(1, 20))
