# Databricks notebook source
# MAGIC %md
# MAGIC #### Overview
# MAGIC ##### Features:
# MAGIC * (web_loan)
# MAGIC   * web_analytics_loan_visits_count
# MAGIC   * web_analytics_loan_days_since_last_visit
# MAGIC * (web_mortgage)
# MAGIC   * web_analytics_mortgage_visits_count
# MAGIC   * web_analytics_mortgage_days_since_last_visit

# COMMAND ----------

# MAGIC %md #### Run notebooks for initial setup

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %md #### Imports

# COMMAND ----------

from logging import Logger
from typing import Dict, List
from pyspark.sql import Column, DataFrame, functions as f

from odap.feature_factory import time_windows as tw

# COMMAND ----------

# MAGIC %md #### Create widgets

# COMMAND ----------

dbutils.widgets.text("timestamp", "")
dbutils.widgets.text("target", "")
dbutils.widgets.text("entity_column_name", "")

# COMMAND ----------

entity_id_column_name = dbutils.widgets.get("entity_column_name")

# COMMAND ----------

# MAGIC %md #### Source

# COMMAND ----------

# DBTITLE 1,Create windowed dataframe with specified time windows
wdf_digi_sdm = tw.WindowedDataFrame(
    df=spark.read.table("odap_digi_sdm_l2.digi_sdm").withColumnRenamed("user_properties_csas_user_id", "customer_id"),
    time_column="event_date",
    time_windows=["14d", "30d", "90d"],
)

# COMMAND ----------

digi_sdm = wdf_digi_sdm.join(
    spark.read.table("target_store"), on="customer_id", how="inner"
).filter(f.col("event_date") <= f.col("timestamp"))

# COMMAND ----------

# MAGIC %md ### Metadata

# COMMAND ----------

metadata = {
    "features": {
        "web_analytics_{product}_visits_count_{time_window}": {
            "description": "How many times the client visited web pages containing information about {product}s in last {time_window}.",
            "category": "digital_product",
            "fillna_with": None
        },
        "web_analytics_{product}_days_since_last_visit_{time_window}": {
            "description": "Number of days since the last visit of the web page containing information about {product}s in last {time_window}.",
            "category": "digital_product",
            "fillna_with": None
        },
    }
}

# COMMAND ----------

# MAGIC %md ### Data quality checks

# COMMAND ----------

dq_checks = [
	'missing_percent(web_analytics_loan_visits_count_14d) < 50%',
    'missing_percent(web_analytics_loan_visits_count_30d) < 50%',
    'missing_percent(web_analytics_loan_visits_count_90d) < 50%',
]

# COMMAND ----------

# MAGIC %md ### Variables for features

# COMMAND ----------

product_list = {
    "loan": ["osobni-finance/pujcky", "personal-finance/loans"],
    "mortgage": ["osobni-finance/hypoteky", "personal-finance/mortgages"],
}

# COMMAND ----------

entity_time_column = "timestamp"

# COMMAND ----------

# MAGIC %md ### Feature engineering

# COMMAND ----------

def get_agg_columns(products: Dict[str, List[str]]):
    def is_relevant(urls: List[str]):
        return f.col("full_url").rlike("|".join(urls)) & ~f.col(
            "full_url"
        ).contains("blog")

    def agg_columns(time_window: str) -> List[tw.WindowedColumn]:
        return [
            tw.count_distinct_windowed(
                f"web_analytics_{product}_visits_count_{time_window}",
                [f.when(is_relevant(urls), f.col("page_screen_view_timestamp"))],
            )
            for product, urls in products.items()
        ] + [
            tw.max_windowed(
                f"web_analytics_{product}_last_visit_date_{time_window}",
                f.when(is_relevant(urls), f.col("page_screen_view_date")),
            )
            for product, urls in products.items()
        ]
    return agg_columns

# COMMAND ----------

def get_non_agg_columns(products: Dict[str, List[str]]):
    def non_agg_columns(time_window: str) -> List[Column]:
        return [
                f.datediff(
                    entity_time_column,
                    f.col(f"web_analytics_{product}_last_visit_date_{time_window}"),
                ).alias(f"web_analytics_{product}_days_since_last_visit_{time_window}")
            for product in products
        ]
    return non_agg_columns

# COMMAND ----------

def get_cols_to_drop(wdf: tw.WindowedDataFrame, products: Dict[str, List[str]]):
    cols_to_drop = []
    for product in products:
        cols_to_drop.extend(
            wdf.get_windowed_column_list(
                [f"web_analytics_{product}_last_visit_date_" + "{time_window}"]
            )
        )
    return cols_to_drop

# COMMAND ----------

cols_to_drop = get_cols_to_drop(digi_sdm, product_list)

# COMMAND ----------

df_final = digi_sdm.time_windowed(
    agg_columns_function=get_agg_columns(product_list),
    non_agg_columns_function=get_non_agg_columns(product_list),
    group_keys=["customer_id", "timestamp"],
).drop(*cols_to_drop)
