import plotly.express as px
import pandas as pd
from pyspark.sql import functions as f


def create_cum_lift_fig(model_summary, spark):
    fig, lift_fig = None, None
    model_name = model_summary["model_type"]

    # create feature importances figure
    if bool(model_summary["artifacts"]["coefficients"]) & (
        model_name != "spark_GLM_binomial"
    ):
        feature_importance_coeffs = model_summary["artifacts"]["coefficients"].items()
    elif bool(model_summary["artifacts"]["coefficients"]) & (
        model_name == "spark_GLM_binomial"
    ):
        feature_importance_dict = {}
        for key, value in model_summary["artifacts"]["coefficients"].items():
            feature_importance_dict[key] = value["value"]
        feature_importance_coeffs = feature_importance_dict.items()
    else:
        feature_importance_coeffs = model_summary["artifacts"][
            "feature_importances"
        ].items()

    if bool(feature_importance_coeffs):
        feature_importances = spark.createDataFrame(
            (pd.DataFrame(feature_importance_coeffs)).iloc[:-2],
            schema="`feature` STRING, `importance` FLOAT",
        )
        feature_importances = feature_importances.orderBy("importance")
        importance_min = (
            feature_importances.select("importance")
            .groupBy()
            .agg(f.min("importance"))
            .collect()[0][0]
        )
        importance_max = (
            feature_importances.select("importance")
            .groupBy()
            .agg(f.max("importance"))
            .collect()[0][0]
        )

        # set color schema based on the classifier type
        if importance_min < 0:
            color_schema = "greenred"
        else:
            color_schema = "rdylgn"
        
        if (model_name != "spark_random_forest_classifier"):
            feature_order = [0, 1, 2, 3, 4, 5, -6, -5, -4, -3, -2, -1]
        else:
            feature_order = [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12]
        
        fig = px.bar(
            feature_importances.orderBy("importance")
            .toPandas()
            .iloc[feature_order],
            x="feature",
            y="importance",
            hover_data=["feature", "importance"],
            color="importance",
            color_continuous_scale="rdylgn",
            range_color=[importance_min, importance_max],
            height=700,
        )

        fig.show()

    # create cumulative lift figure
    if "lift" in model_summary["artifacts"]:
        lift_fig = px.bar(
            model_summary["artifacts"]["lift"].toPandas(),
            x="bucket",
            y="cum_lift",
            color_discrete_sequence=["blue"] * 10,
            hover_data=["bucket", "cum_lift"],
            height=700,
        ).update_xaxes(tickvals=list(range(1, 11)))
        lift_fig.show()

    return fig, lift_fig
