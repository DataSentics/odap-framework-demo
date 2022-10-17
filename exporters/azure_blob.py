from typing import Dict
from pyspark.sql import SparkSession, DataFrame, functions as f


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
    output_blob_path = f"/dbfs/fake_azure_blob{output_path}/{segment}.csv"
    
    (segment_df.withColumn("segment", f.lit(segment_config["name"])).toPandas().to_csv(output_blob_path, index=False))
