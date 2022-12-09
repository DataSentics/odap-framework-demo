# Databricks notebook source
# MAGIC %md #### Imports & Settings

# COMMAND ----------

from hyperopt import hp
from hyperopt.pyll import scope
import json
import math
import mlflow
import mlflow.spark as mlflow_spark
from mlflow.tracking.client import MlflowClient
import plotly.express as px
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    LogisticRegression,
    GBTClassifier,
    DecisionTreeClassifier,
    RandomForestClassifier,
    MultilayerPerceptronClassifier,
    LinearSVC,
    NaiveBayes,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, Normalizer, StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.util import MLUtils
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from segments.lookalike_boosting.ml_functions import lift_curve_colname_specified, ith, process_multiple_segments_input, compute_lift_train_test
import shap

#from pyspark.sql.session import SparkSession
#from datasciencefunctions.supervised import fit_supervised_model #remove

# COMMAND ----------

# MAGIC %md ##Widgets

# COMMAND ----------

dbutils.widgets.dropdown("model_approach", "hyperopt", ["basic", "hyperopt"])
dbutils.widgets.dropdown("hyperopt_metric", "lift_10", ["f1", "lift_10", "lift_100", "r2", "rmse", "weighted_precision", "weighted_recall"])
dbutils.widgets.dropdown("downsample",  "Yes", ["Yes", "No"])
dbutils.widgets.text("downsample_share", "0.3")
dbutils.widgets.text("target_ratio", "0.125")
dbutils.widgets.dropdown("upsample", "Yes", ["Yes", "No"])
dbutils.widgets.dropdown("normalize", "No", ["Yes", "No"])
dbutils.widgets.dropdown("normalize_approach", "StandardScaler", ["Normalizer", "StandardScaler", "MinMaxScaler"])

# COMMAND ----------

# MAGIC %md #### Load data

# COMMAND ----------

df_data = spark.table("odap_features.features_customer")

# COMMAND ----------

# MAGIC %md #### Feature names

# COMMAND ----------

df_metadata = spark.table("odap_features.metadata_customer")

# COMMAND ----------

feature_names = [feature[0] for feature in df_metadata.select("feature").collect()]
feature_names.remove("customer_email")

# COMMAND ----------

# MAGIC %md #### Assemble features

# COMMAND ----------

assembler = VectorAssembler(inputCols=features_names, outputCol="features", handleInvalid="skip")
df_data = assembler.transform(df).select('label', 'features')

# COMMAND ----------

# MAGIC %md #### Split, downsample, scale, normalize

# COMMAND ----------

#split data to test and train
# if downsample, then downsample

#if normalize, then normalize and choose normalize approach
    if normalize == "Yes":
        scaler_norm = Normalizer(inputCol="features_raw", outputCol="features", p=1.0)
        scaler_stan = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
        scaler_mm = MinMaxScaler(inputCol="features_raw", outputCol="features")
        
        df_train = df_train.withColumnRenamed("features", "features_raw")
        df_test = df_test.withColumnRenamed("features", "features_raw")
        
        if normalize_approach == "Normalizer":
            df_train = scaler_norm.transform(df_train).drop("features_raw")
            df_test = scaler_norm.transform(df_test).drop("features_raw")
            scaler = scaler_norm
        elif normalize_approach == "MinMaxScaler":
            scaler_stan = scaler_stan.fit(df_train)
            df_train = scaler_stan.transform(df_train).drop("features_raw")
            df_test = scaler_stan.transform(df_test).drop("features_raw")
            scaler = scaler_stan
        else:
            scaler_mm = scaler_mm.fit(df_train)
            df_train = scaler_mm.transform(df_train).drop("features_raw")
            df_test = scaler_mm.transform(df_test).drop("features_raw")
            scaler = scaler_mm
    else:
        scaler = None    

# COMMAND ----------

#caching
df_train.cache()
df_test.cache()

display(df.groupBy('label').count())
display(df_train.groupBy('label').count())
display(df_test.groupBy('label').count())}

# COMMAND ----------

# calculate target share and corresponding oversampling ratio
n_target_1 = df_train.filter(f.col('label') == 1).count()
n_total = df_train.count()
actual_target_ratio = n_target_1/n_total
scale_target_ratio = float(wanted_target_ratio)/actual_target_ratio

smote_perc_over = ((n_total - n_target_1)*scale_target_ratio)/(n_total - scale_target_ratio*n_target_1)
smote_perc_over = int(smote_perc_over)*100

# no upsampling needed
if scale_target_ratio <= 1.0 or n_target_1 <= 1000:
    upsample = "No"

return {
    'train': df_train,
    'test': df_test,
    'smote_perc_over': smote_perc_over,
    'upsample': upsample,
    'scaler': scaler,
}

# COMMAND ----------

# MAGIC %md Smote

# COMMAND ----------

spark.conf

# COMMAND ----------

# make variables visible to scala
@dp.notebook_function(sampling.result['upsample'])
def transfer_vars_to_scala(upsample):
    spark.conf.set('smote_perc_over', sampling.result['smote_perc_over'])
    spark.conf.set('smote_input_table', define_paths.result['train_sample_location'])
    spark.conf.set('smote_output_table', define_paths.result['train_sample_smote_location'])
    spark.conf.set('upsample', upsample)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.instance.ASMOTE
# MAGIC import org.apache.spark.sql.functions
# MAGIC 
# MAGIC // variables
# MAGIC val set_perc_over = spark.conf.get("smote_perc_over").toInt
# MAGIC val input_table = spark.conf.get("smote_input_table")
# MAGIC val output_table = spark.conf.get("smote_output_table")
# MAGIC val upsample = spark.conf.get("upsample")
# MAGIC 
# MAGIC if (upsample == "Yes") {
# MAGIC     // read dataset
# MAGIC     val ds = spark.read
# MAGIC                   .format("delta")
# MAGIC                   .load(input_table)
# MAGIC                   .withColumn("label", functions.col("label").cast("double"))
# MAGIC                   .select("label", "features")
# MAGIC 
# MAGIC     // using ASMOTE
# MAGIC     val asmote = new ASMOTE().setK(5)
# MAGIC                              .setPercOver(set_perc_over)
# MAGIC                              .setSeed(46)
# MAGIC     val newDF = asmote.transform(ds)
# MAGIC 
# MAGIC     newDF.write.format("delta").mode("overwrite").save(output_table)
# MAGIC }

# COMMAND ----------

# MAGIC %md #### Model estimation

# COMMAND ----------

# MAGIC %md Load samples

# COMMAND ----------

# MAGIC %md Target distribution - train

# COMMAND ----------

n_rows = df.count()

display(df.groupBy('label').count().withColumn('share', f.col('count')/n_rows))

# COMMAND ----------

# MAGIC %md Fit model

# COMMAND ----------

def modeling(
    df_train = read_train
df_test = read_test
trackingpoint_ids = process_traits_input.result['table_name_suffix']# FILL IN TABLE NAME SUFFIX
model_name = dp.get_widget_value("model") # FILL IN MODEL TYPE
model_approach = dp.get_widget_value("model_approach") # basic, not needed 
hyperopt_metric = dp.get_widget_value("hyperopt_metric")
define_paths = define_paths
assembler = assemble_features.result['assembler']
sampling_res = sampling.result
normalize = dp.get_widget_value("normalize")
normalize_approach = dp.get_widget_value("normalize_approach")

):

    if model_approach == "basic":
        MODEL_TYPES = [
            "LogisticRegression",
            "SVM",
            "NaiveBayes",
            "XGBoost",
            "DecisionTree",
            "RandomForest",
            "MLP",
        ]
        CLASSIFIERS = {
            "LogisticRegression": LogisticRegression(),
            "XGBoost": GBTClassifier(maxIter=10),
            "DecisionTree": DecisionTreeClassifier(),
            "RandomForest": RandomForestClassifier(),
            "SVM": LinearSVC(),
            "NaiveBayes": NaiveBayes(modelType="multinomial"),
            "MLP": MultilayerPerceptronClassifier(layers=[5000, 250, 250, 2]),
        }
        
        
    mlflow.set_experiment(
        "/adpicker/mlflow_model/ADLookalike_model/ADLookalike_TP"
    )
    
    # Train and log the sklearn model
    with mlflow.start_run():
        mlflow.log_param("Model_name", "Test_model")
        mlflow.log_param("Model_type", model_name)
        mlflow.log_param("Product", "trackingpoint") #do not know what it is
        
        # save features names
        features_names = assembler.getInputCols()
        with open('/tmp/features.txt', 'w') as out_file:
            out_file.write(",".join(features_names))
        mlflow.log_artifact('/tmp/features.txt')

        if model_approach == "basic":
            classifier = CLASSIFIERS[model_name].setFeaturesCol("features").setLabelCol("label")
            spark_pipeline = Pipeline(stages=[classifier])
            model_pipeline = spark_pipeline.fit(df_train)

            # fitting a classification model
            model_pipeline = fit_supervised_model(
                clf,
                df_train,
                param_space_search="hyperopt",
                max_evals=5,
                metric_name = hyperopt_metric,
                custom_params=custom_params[model_name],
            )
            
        # final model pipeline
        if normalize == "Yes":
            scaler = sampling_res['scaler']
            print(scaler)
            model_pipeline_all = PipelineModel(stages = [assembler, scaler, model_pipeline])
        else:
            model_pipeline_all = PipelineModel(stages = [assembler, model_pipeline])
            

        m_info = mlflow_spark.log_model(spark_model=model_pipeline_all, artifact_path="Model_" + trackingpoint_ids)

        # predictions
        prediction_df = model_pipeline.transform(df_test)
        prediction_df_train = model_pipeline.transform(df_train)
        
        score_df = prediction_df.withColumn(
            "score1", ith("probability", f.lit(0))
        ).withColumn("score2", ith("probability", f.lit(1)))
        
        score_df_train = prediction_df_train.withColumn(
            "score1", ith("probability", f.lit(0))
        ).withColumn("score2", ith("probability", f.lit(1)))
            
        # lift10 test
        lift10 = lift_curve_colname_specified(
            prediction_df, "label", 10, "probability"
        )
        lift10.toPandas().to_csv("/tmp/lift10.csv")
        mlflow.log_artifact("/tmp/lift10.csv")
        lift10_number = (
            lift10.filter(f.col("bucket") == 1).select("cum_lift").collect()[0][0]
        )
        mlflow.log_param("Lift_10", lift10_number)
        
        # lift10 train
        lift10_train = lift_curve_colname_specified(
            prediction_df_train, "label", 10, "probability"
        )
        lift10_train.toPandas().to_csv('/tmp/lift10_train.csv')
        mlflow.log_artifact('/tmp/lift10_train.csv')
        lift10_train_number = lift10_train.filter(f.col('bucket') == 1).select('cum_lift').collect()[0][0]
        mlflow.log_param('Lift_10_train', lift10_train_number)
        
        # lift 100
        lift100 = lift_curve_colname_specified(
            prediction_df, "label", 100, "probability"
        )
        lift100.toPandas().to_csv("/tmp/lift100.csv")
        mlflow.log_artifact("/tmp/lift100.csv")
        lift100_number = (
            lift100.filter(f.col("bucket") == 1).select("cum_lift").collect()[0][0]
        )
        mlflow.log_param("Lift_100", lift100_number)
        
    return {
        'train': score_df_train,
        'test': score_df,
        'model_obj': model_pipeline,
        'model_obj_all': model_pipeline_all,
        'm_info': m_info,
    }

# COMMAND ----------

# MAGIC %md #### Model testing

# COMMAND ----------

def prediction_ov_1(df_train: modeling.result['train'], df_test: modeling.result['test'], prob_th=0.5):
    try:
        df_train = (df_train
                    .select('label', f.when(f.col("score2") >= prob_th, 1).otherwise(0).alias("TARGET_PRED"))
                    .groupBy('label', "TARGET_PRED")
                    .count()
                    .sort(f.desc('count'))
                   )

        print('train')
        display(df_train)

        df_test = (df_test
                   .select('label', f.when(f.col("score2") >= prob_th, 1).otherwise(0).alias("TARGET_PRED"))
                   .groupBy('label', "TARGET_PRED")
                   .count()
                   .sort(f.desc('count'))
                  )

        print('test')
        display(df_test)

        print('accuracy test')
        n_label_1 = df_test.filter(f.col('label') == 1).groupBy().agg(f.sum('count').alias('count')).select('count').collect()[0][0]
        n_pred_1 = df_test.filter((f.col('label') == 1) & (f.col('TARGET_PRED') == 1)).groupBy().agg(f.sum('count').alias('count')).select('count').collect()[0][0]
        print(n_pred_1/n_label_1)
    except BaseException as e:
        print(f"ERROR: preiction_ov_1: {e}")

# COMMAND ----------

def prediction_ov_2(df_train: modeling.result['train'], df_test: modeling.result['test']):
    try:
        n_train = df_train.count()

        df_train = (df_train
                        .withColumn('pred', f.round('score2', 2))
                        .groupBy("pred")
                        .count()
                        .sort(f.desc("pred"))
                        .withColumn('SHARE', f.col('count')/n_train)
                       )

        print('train')
        display(df_train)

        n_test = df_test.count()

        df_test = (df_test
                        .withColumn('pred', f.round('score2', 2))
                        .groupBy("pred")
                        .count()
                        .sort(f.desc("pred"))
                        .withColumn('SHARE', f.col('count')/n_test)
                       )

        print('test')
        display(df_test)
    except BaseException as e:
        print(f"ERROR: preiction_ov_2: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get lift

# COMMAND ----------

def get_lift(df_train = modeling.result['train'], df_test = modeling.result['test']):
    try:
        display(compute_lift_train_test(df_train, df_test, "label").withColumnRenamed('lift_train', 'Lift Train').withColumnRenamed('lift_test', 'Lift Test'))
    except BaseException as e:
        print(f"ERROR: lift: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Feature importances

# COMMAND ----------

def get_feature_importance(model_obj=modeling.result['model_obj'], model_approach=dp.get_widget_value("model_approach"), feature_columns=get_features_names.result):
    try:
        if model_approach == 'basic':
            model_obj = model_obj.stages[0]
        else:
            model_obj
                     
        feature_importances = list(model_obj.featureImportances.toArray())
        feature_importances_with_names = []

        for feature_name, importance in zip(feature_columns, feature_importances):
            feature_importances_with_names.append((feature_name, float(importance)))

        feature_importances = spark.createDataFrame(feature_importances_with_names, schema="`feature` STRING, `importance` FLOAT") 
        feature_importances = feature_importances.orderBy("importance")

        importance_min = feature_importances.select('importance').groupBy().agg(f.min('importance')).collect()[0][0]
        importance_max = feature_importances.select('importance').groupBy().agg(f.max('importance')).collect()[0][0]

        if importance_min < 0:
            color_schema = "greenred"
        else:
            color_schema = "rdylgn"
        
        feature_importances = feature_importances.withColumn('feature',  f.regexp_replace('feature', '_', ' '))
        feature_importances = feature_importances.filter(f.col('importance') > 0)
        
        fig = px.bar(feature_importances.orderBy("importance", ascending=False).limit(30).orderBy("importance").toPandas(), x="feature", y="importance",
                         hover_data=["feature", "importance"], color="importance",
                         color_continuous_scale=color_schema,
                         height=700)
        fig.show()
        
        # top 15
        fig = px.bar(feature_importances.orderBy("importance").limit(15).toPandas(), x="feature", y="importance",
                     hover_data=["feature", "importance"], color="importance",
                     color_continuous_scale=color_schema, range_color=[importance_min, importance_max],
                     height=700,)
        fig.show()
        
        # min 15
        fig = px.bar(feature_importances.orderBy("importance", ascending=False).limit(15).orderBy("importance").toPandas(), x="feature", y="importance",
                     hover_data=["feature", "importance"], color="importance",
                     color_continuous_scale=color_schema, range_color=[importance_min, importance_max],
                     height=700, )
        fig.show()
    except BaseException as e:
        print(f"ERROR: features importances not available: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Feature values

# COMMAND ----------

def features_values(df=modeling.result['train'], feature_columns=get_features_names.result,, model_obj=modeling.result['model_obj'], prob_th=0.5):
    try:
        df = (df
              .withColumn("xs", vector_to_array("features"))
              .select(["label", "score2"] + [f.col("xs")[i].alias(name) for i, name in enumerate(feature_columns)])
              .withColumn("TARGET_PRED", f.when(f.col("score2") >= prob_th, 1).otherwise(0))
             )
        
        df_target = (df
                     .groupBy("label")
                     .agg(
                         f.count(f.lit(1)).alias('count'),
                         *[f.mean(f.col(c)).alias(c) for c in feature_columns]
                     )
                     .sort('label')
                    )
        
        df_pred = (df
                   .groupBy("TARGET_PRED")
                   .agg(
                       f.count(f.lit(1)).alias('count'),
                       *[f.mean(f.col(c)).alias(c) for c in feature_columns]
                   )
                   .sort("TARGET_PRED")
                  )
        
        display(df_target)
        display(df_pred)
        
    except BaseException as e:
         print(f'ERROR: {e}')

# COMMAND ----------

def features_values_test(df=modeling.result['test'], feature_columns=get_features_names.result, model_obj=modeling.result['model_obj'], prob_th=0.5):
    try:
        df = (df
              .withColumn("xs", vector_to_array("features"))
              .select(["label", "score2"] + [f.col("xs")[i].alias(name) for i, name in enumerate(feature_columns)])
              .withColumn("TARGET_PRED", f.when(f.col("score2") >= prob_th, 1).otherwise(0))
             )
        
        df_target = (df
                     .groupBy("label")
                     .agg(
                         f.count(f.lit(1)).alias('count'),
                         *[f.mean(f.col(c)).alias(c) for c in feature_columns]
                     )
                     .sort('label')
                    )
        
        df_pred = (df
                   .groupBy("TARGET_PRED")
                   .agg(
                       f.count(f.lit(1)).alias('count'),
                       *[f.mean(f.col(c)).alias(c) for c in feature_columns]
                   )
                   .sort("TARGET_PRED")
                  )
        
        display(df_target)
        display(df_pred)
        
    except BaseException as e:
         print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Shapley values

# COMMAND ----------

def shapley_vals(df=modeling.result['train'], feature_columns=get_features_names.result, model_obj=modeling.result['model_obj']):
    try:
        df = (df
              .withColumn("xs", vector_to_array("features"))
              .select(["label"] + [f.col("xs")[i].alias(name) for i, name in enumerate(feature_columns)])
             )
        
        X = df.select(feature_columns).toPandas()
        explainer = shap.TreeExplainer(model_obj)
        shap_values = explainer.shap_values(X, check_additivity=False)
        
        shap.summary_plot(shap_values, X, plot_size=[30,20])
        shap.summary_plot(shap_values, X, plot_type='violin', plot_size=[30,20])
        shap.summary_plot(shap_values, X, plot_type="bar")
        
    except BaseException as e:
            print(f'ERROR: Shapley values not available: {e}')
