# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS odap_parsed_ga4")

# COMMAND ----------

df = spark.readStream.table("odap_raw_ga4.events_stream")

# COMMAND ----------

parsed_df = (
    df
    .withColumn("event_date", f.to_date(f.col("event_date"), "yyyyMMdd"))
    .withColumn("event_timestamp", f.to_timestamp(f.col("event_timestamp") / 1e6))
    .withColumn("event_previous_timestamp", f.to_timestamp(f.col("event_previous_timestamp") / 1e6))
    .withColumn("user_first_touch_timestamp", f.to_timestamp(f.col("user_first_touch_timestamp") / 1e6))
    .select(
        f.col("dataset_id"),
        f.col("event_date"),
        f.col("downloaded_at"),
        f.col("event_timestamp"),
        f.col("event_name"),
        f.col("event_previous_timestamp"),
        f.col("event_value_in_usd"),
        f.col("event_bundle_sequence_id"),
        f.col("event_server_timestamp_offset"),
        f.col("user_id"),
        f.col("user_pseudo_id"),
        f.col("privacy_info.analytics_storage").cast("string").alias("privacy_info_analytics_storage"),
        f.col("privacy_info.ads_storage").cast("string").alias("privacy_info_ads_storage"),
        f.col("privacy_info.uses_transient_token").alias("privacy_info_uses_transient_token"),
        f.col("user_first_touch_timestamp"),
        f.col("user_ltv.revenue").alias("user_ltv_revenue"),
        f.col("user_ltv.currency").alias("user_ltv_currency"),
        f.col("device.category").alias("device_category"),
        f.col("device.mobile_brand_name").alias("device_mobile_brand_name"),
        f.col("device.mobile_model_name").alias("device_mobile_model_name"),
        f.col("device.mobile_marketing_name").alias("device_mobile_marketing_name"),
        f.col("device.mobile_os_hardware_model").cast("string").alias("device_mobile_os_hardware_model"),
        f.col("device.operating_system").alias("device_operating_system"),
        f.col("device.operating_system_version").alias("device_operating_system_version"),
        f.col("device.vendor_id").cast("string").alias("device_vendor_id"),
        f.col("device.advertising_id").cast("string").alias("device_advertising_id"),
        f.col("device.language").alias("device_language"),
        f.col("device.is_limited_ad_tracking").alias("device_is_limited_ad_tracking"),
        f.col("device.time_zone_offset_seconds").alias("device_time_zone_offset_seconds"),
        f.col("device.web_info.browser").alias("device_web_info_browser"),
        f.col("device.web_info.browser_version").alias("device_web_info_browser_version"),
        f.col("geo.continent").alias("geo_continent"),
        f.col("geo.country").alias("geo_country"),
        f.col("geo.region").alias("geo_region"),
        f.col("geo.city").alias("geo_city"),
        f.col("geo.sub_continent").alias("geo_sub_continent"),
        f.col("geo.metro").alias("geo_metro"),
        f.col("app_info.id").alias("app_info_id"),
        f.col("app_info.version").alias("app_info_version"),
        f.col("app_info.install_store").alias("app_info_install_store"),
        f.col("app_info.firebase_app_id").alias("app_info_firebase_app_id"),
        f.col("app_info.install_source").alias("app_info_install_source"),
        f.col("traffic_source.name").alias("traffic_source_name"),
        f.col("traffic_source.medium").alias("traffic_source_medium"),
        f.col("traffic_source.source").alias("traffic_source_source"),
        f.col("stream_id").cast("string"),
        f.col("platform"),
        f.col("event_dimensions.hostname").alias("event_dimensions_hostname"),
        f.col("ecommerce.total_item_quantity").alias("ecommerce_total_item_quantity"),
        f.col("ecommerce.purchase_revenue_in_usd").alias("ecommerce_purchase_revenue_in_usd"),
        f.col("ecommerce.purchase_revenue").alias("ecommerce_purchase_revenue"),
        f.col("ecommerce.refund_value_in_usd").alias("ecommerce_refund_value_in_usd"),
        f.col("ecommerce.refund_value").alias("ecommerce_refund_value"),
        f.col("ecommerce.shipping_value_in_usd").alias("ecommerce_shipping_value_in_usd"),
        f.col("ecommerce.shipping_value").alias("ecommerce_shipping_value"),
        f.col("ecommerce.tax_value_in_usd").alias("ecommerce_tax_value_in_usd"),
        f.col("ecommerce.tax_value").alias("ecommerce_tax_value"),
        f.col("ecommerce.unique_items").alias("ecommerce_unique_items"),
        f.col("ecommerce.transaction_id").alias("ecommerce_transaction_id"),
    )
)

# COMMAND ----------

(
    parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", "abfss://sdm@devodapstorage.dfs.core.windows.net/odap_parsed_ga4_stream.delta")
    .option("checkpointLocation", "abfss://sdm@devodapstorage.dfs.core.windows.net/odap_parsed_ga4_stream.delta/_checkpoint")
    .toTable("odap_parsed_ga4.events_stream")
)
