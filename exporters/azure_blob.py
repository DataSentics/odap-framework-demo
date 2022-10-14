from typing import Dict
from pyspark.sql import SparkSession
from odap.feature_factory.config import get_features_table, get_features_table_path
from odap.segment_factory.config import get_segment_table
from odap.common.config import get_config_namespace, ConfigNamespace


def exporter(segment: str, exporter_config: Dict):
    """Simple exporter example
    Joins the segment table and the feature table
    and export the columns specified in the exporter configuration.

    Parameters
    ----------
    segment : str
        The name of the exported segment
    exporter_config : Dict
        The exporter configuration specified in the config.yaml file.
    """
        
    spark = SparkSession.getActiveSession()
    
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    features_table_name = get_features_table(feature_factory_config)
    
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)
    segment_table_name = get_segment_table(segment, segment_factory_config)
    
    segment_df = spark.read.table(f"hive_metastore.{segment_table_name}")
    featurestore_df = spark.read.table(f"hive_metastore.{features_table_name}")
    
    result_df = segment_df.join(featurestore_df, "customer_id", "inner").select('customer_id', *exporter_config["attributes"])
    
    output_path = exporter_config["path"]
    output_blob_folder = f"abfss://odap-demo@odapczlakeg2dev.dfs.core.windows.net{output_path}/{segment}"
    
    result_df.display()
    (result_df.write
     .mode("overwrite")
     .option("header", "true")
     .format("csv")
     .save(output_blob_folder))
    