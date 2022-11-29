# Databricks notebook source
# MAGIC %md
# MAGIC ## About the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** The second notebook contains functionality to train different types of models and log results to mlflow for further inspection.  
# MAGIC 
# MAGIC The notebook is organized as follows:  
# MAGIC **Setup for the notebook** = setup of the environment, import of libraries, setup of widgets  
# MAGIC **Loading of the dataset** = load the model dataset prepared in the first exploration notebook  
# MAGIC **Training phase** = train selected models by going through all essential steps in a loop-like fashion 
# MAGIC 
# MAGIC **Goal:** At the end of the notebook, you can compare performance of selected models and various logged artifacts in mlflow environment  
# MAGIC 
# MAGIC **Tip:** If you need help on datasciencefunctions library, please use the help() functionality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for the notebook

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/datasciencefunctions-0.2.0-py3-none-any.whl

# COMMAND ----------

# DBTITLE 1,Import libraries
import mlflow
from pyspark.sql import functions as f
from pyspark.sql import types as t

import datasciencefunctions as ds
from datasciencefunctions.data_processing import (
    train_test_split,
    fit_transformation_pipeline,
    apply_transformation_pipeline,
)
from datasciencefunctions.supervised import (
    fit_supervised_model,
    get_model_summary,
    log_model_summary,
)
from lib.figures import create_cum_lift_fig

# COMMAND ----------

dbutils.widgets.text("feature_name", "")
dbutils.widgets.dropdown(
    "metric_name",
    "auroc",
    ["auroc", "AUPRC", "lift_10", "lift_100", "accuracy", "f1", "recall", "precision"],
)
dbutils.widgets.multiselect(
    "model_type",
    "spark_logistic_regression",
    [
        "spark_logistic_regression",
        "spark_GBT_classifier",
        "spark_decision_tree_classifier",
        "spark_random_forest_classifier",
        "spark_GLM_binomial",
        "sparkdl_xgboost_classifier",
        "sklearn_random_forest_classifier",
        "sklearn_gradient_boosting_classifier",
        "sklearn_logistic_regression",
        "xgboost_xgbclassifier",
    ],
)
dbutils.widgets.text("target", "")

# COMMAND ----------

# DBTITLE 1,Assign widgets' values
feature_name = dbutils.widgets.get("feature_name")
metric_name = dbutils.widgets.get("metric_name")
models = dbutils.widgets.get("model_type").split(",")
target_name = dbutils.widgets.get("target")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading data
# MAGIC 
# MAGIC By now, the dataset for the modelling should be prepared. If not, please refer to the first part of the flow ---> [Exploration phase notebook](https://adb-8406481409439422.2.azuredatabricks.net/?o=8406481409439422#notebook/404406204469516/command/684051081804498)  
# MAGIC We will now load the dataset and proceed to the training phase.  

# COMMAND ----------

# DBTITLE 1,Load the dataset
target_name = target_name.replace(" ", "_")
df_data = spark.read.table("odap_datasets.lead_submit_training_data").drop("cookie_id", "timestamp")

# COMMAND ----------

# DBTITLE 1,Define which columns we consider as categorical/numerical
skip_cols = ["customer_id", "timestamp", "label"]
cat_cols = [
    f.name for f in df_data.schema.fields if isinstance(f.dataType, t.StringType)
]
num_cols = [
    f.name
    for f in df_data.schema.fields
    if isinstance(
        f.dataType,
        (t.IntegerType, t.DoubleType, t.FloatType, t.DecimalType, t.LongType),
    )
]

# COMMAND ----------

# DBTITLE 1,Downsampling the majority class
df_data = df_data.filter(f.col("label") == 1).union(
    df_data.filter(f.col("label") == 0).sample(0.01)
)

# COMMAND ----------

df_data.groupBy("label").count().display()

# COMMAND ----------

# DBTITLE 1,Prepare datasets for particular model flavours
spark_models = [
    model_name
    for model_name in models
    if ds.MlModel[model_name].framework in ["spark", "sparkdl"]
]
non_spark_models = [
    model_name
    for model_name in models
    if ds.MlModel[model_name].framework not in ["spark", "sparkdl"]
]

if spark_models:
    df_spark = df_data.cache()

if non_spark_models:
    df_pandas = df_data.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training process
# MAGIC 
# MAGIC Training is executed for all selected models in the widget and each loop iteration represents one of these trainings  
# MAGIC * Some parts of code are executed for spark models, whereas some are considered only for scikit-learn models  
# MAGIC * The code contains some functionality related to mlflow in order to keep track of important pieces of information about the modelling process  
# MAGIC * Firstly, splitting of the model dataset is performed (to get the train and test dataset)  
# MAGIC * Then, encoding of features and other necessary transformations are applied  
# MAGIC * The process continues with hyperparameter optimization 
# MAGIC * Figures are made and logged to MlFlow for further inspection
# MAGIC * Finally, the model is fitted and the process can begin again until all selected models are trained
# MAGIC * Find important information about the model in the model summary object at the end of the notebook

# COMMAND ----------

train_test_ratio = 0.8

# COMMAND ----------

#mlflow settings
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/digi_use_case_lead_submit_training')

# COMMAND ----------

# DBTITLE 1,Training cycle
models_summaries = []
for model_name in models:
    # using pyspark dataframe for spark models and pandas df for vanilla-python models
    df_data = (
        df_spark
        if ds.MlModel[model_name].framework == "spark"
        or ds.MlModel[model_name].framework == "sparkdl"
        else df_pandas
    )

    # spark.conf.set("spark.databricks.mlflow.trackMLlib.enabled", "false")
    model_type = ds.MlModel[model_name]

    # train-test split
    train_data, test_data = train_test_split(df_data, ratio=train_test_ratio)

    # fitting the transformation pipeline
    pipeline = fit_transformation_pipeline(
        train_data,
        skip_cols=skip_cols,
        num_cols=num_cols,
        cat_cols=cat_cols,
    )

    # applying the transformation pipeline
    train_data, test_data = apply_transformation_pipeline(
        pipeline,
        train_data,
        test_data,
    )

    modified_param_space = (
        model_type.default_hyperopt_param_space
    )  # default_param_grid_values, default_hyperopt_param_space

    # fitting a classification model
    # only xgboost_xgbclassifier, sklearn_random_forest, sklearn_gradient_boosting_classifier have parameter max_depth
    model = fit_supervised_model(
        model_type,
        train_data,
        param_space_search="hyperopt",
        max_evals=1,
        metric_name=metric_name,
        custom_params={"max_depth": modified_param_space.get("max_depth")}
        if model_name
        in [
            "xgboost_xgbclassifier",
            "sklearn_random_forest",
            "sklearn_gradient_boosting_classifier",
        ]
        else None,
    )

    # obtaining a model summary
    model_summary = get_model_summary(model, pipeline, test_data)
    model_summary["model_type"] = model_name

    models_summaries.append(model_summary)
    fig, lift_fig = create_cum_lift_fig(model_summary, spark)

    # logging the summary to MLFlow
    with mlflow.start_run():
        log_model_summary(model_summary)
        mlflow.log_param("Model_name", feature_name)
        mlflow.log_param("Model_type", model_name)

        if lift_fig:
            mlflow.log_figure(lift_fig, "cum_lift.html")
        if fig:
            mlflow.log_figure(fig, "feature_importances.html")
