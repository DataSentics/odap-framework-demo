# Databricks notebook source
import os
import time
import random
import datetime as dt
from pyspark.sql import functions as f

# COMMAND ----------

ga4_events = spark.read.table("odap_raw_ga4.events")

# COMMAND ----------

def get_latest_emitted_event_timestamp():
    if not os.path.exists("/dbfs/tmp/ga4_events_stream_state"):
        return ga4_events.select(f.min("event_timestamp")).collect()[0][0]
    
    with open("/dbfs/tmp/ga4_events_stream_state", mode="r") as fd:
        return int(fd.read())

# COMMAND ----------

def write_latest_emitted_event_timestamp(event_timestamp):
    with open("/dbfs/tmp/ga4_events_stream_state", mode="w") as fd:
        fd.write(str(event_timestamp))

# COMMAND ----------

while True:
    latest_emitted_timestamp = get_latest_emitted_event_timestamp()
    ten_seconds = 10000000
    
    ga4_events_window = (
        ga4_events
        .filter(
            (f.col("event_timestamp") > latest_emitted_timestamp) & (f.col("event_timestamp") < latest_emitted_timestamp + ten_seconds)
        )
        .orderBy(f.col("event_timestamp"))
    )
    
    event = ga4_events_window.limit(1)
    event_timestamp = ga4_events_window.limit(1).select("event_timestamp").collect()[0][0]
    
    event.write.format("delta").mode("append").saveAsTable("odap_raw_ga4.events_stream")
    write_latest_emitted_event_timestamp(event_timestamp)
    
    time.sleep(random.randint(1, 20))
