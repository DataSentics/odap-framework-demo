from pyspark.sql import functions as f
import pandas as pd

def profile_features(dataset, metadata_df):
    metadata_pd = metadata_df.filter(f.col("feature").isin(dataset.columns)).toPandas()

    categorical_cols = list(metadata_pd[metadata_pd["variable_type"] == "categorical"]["feature"])
    numerical_cols = list(metadata_pd[metadata_pd["variable_type"] == "numerical"]["feature"])
    binary_cols = list(metadata_pd[metadata_pd["variable_type"] == "binary"]["feature"])

    statistics_df = (dataset
                     .select(
                         *[f.max(col).alias("max_"+col) for col in numerical_cols], 
                         *[f.min(col).alias("min_"+col) for col in numerical_cols], 
                         *[f.avg(col).alias("avg_"+col) for col in numerical_cols], 
                     )
                    )
    statistics_dict = statistics_df.collect()[0].asDict()

    binary_df = dataset.select(*(f.col(c).cast("double").alias(c) for c in binary_cols))
    binary_statistics_df = (binary_df
                            .select(
                                *[f.max(col).alias("max_"+col) for col in binary_cols], 
                                *[f.min(col).alias("min_"+col) for col in binary_cols], 
                                *[f.avg(col).alias("avg_"+col) for col in binary_cols], 
                            )
                           )
    binary_statistics_dict = binary_statistics_df.collect()[0].asDict()

    non_nulls_df = (dataset
                    .select(
                        [f.count(f.when(f.col(c).isNotNull() , c)).alias(c) for c in dataset.columns]
                           )
                   )
    non_nulls_dict = non_nulls_df.collect()[0].asDict()

    metadata_dict = {}
    for column in categorical_cols:
        statistics = {}
        statistics["count_not_null"] = non_nulls_dict[column]
        distinct_column_vals = dataset.select(column).distinct().collect()
        distinct_column_vals = [v[column] for v in distinct_column_vals]
        statistics["number_distinct_values"] = len(distinct_column_vals)
        statistics["distinct_values"] = distinct_column_vals
        metadata_dict[column] = statistics
    for column in numerical_cols:
        statistics = {}
        statistics["count_not_null"] = non_nulls_dict[column]
        statistics["min_value"] = statistics_dict["min_"+column]
        statistics["max_value"] = statistics_dict["max_"+column]
        statistics["avg_value"] = statistics_dict["avg_"+column]
        metadata_dict[column] = statistics
    for column in binary_cols:
        statistics = {}
        statistics["count_not_null"] = non_nulls_dict[column]
        statistics["min_value"] = binary_statistics_dict["min_"+column]
        statistics["max_value"] = binary_statistics_dict["max_"+column]
        statistics["avg_value"] = binary_statistics_dict["avg_"+column]
        metadata_dict[column] = statistics
    
    return pd.DataFrame.from_dict(metadata_dict)