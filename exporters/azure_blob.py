from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from odap.feature_factory.config import get_features_table, get_features_table_path
from odap.segment_factory.config import get_segment_table
from odap.common.config import get_config_namespace, ConfigNamespace


def export(segment: str, segment_df: DataFrame, segment_config: Dict, export_config: Dict):
    """Simple exporter example
    Export DF as CSV to blob storage.

    Parameters
    ----------
    segment : str
        The name of the exported segment
    segment_df : DataFrame
        Segment dataframe
    segment_config : Dict
        The segment configuration specified in the config.yaml file.
    exporter_config : Dict
        The exporter configuration specified in the config.yaml file.
    """
        
    spark = SparkSession.getActiveSession()
    
    output_path = export_config["path"]
    output_blob_folder = f"dbfs:/fake_azure_blob{output_path}/{segment}"
    
    (segment_df.write
     .mode("overwrite")
     .option("header", "true")
     .format("csv")
     .save(output_blob_folder))
    