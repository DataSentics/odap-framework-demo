from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# LIFT
def lift_curve_colname_specified(predictions, target, bin_count, colname):
    vectorElement = udf(lambda v: float(v[1]))
    lift_df = (
        predictions.select(vectorElement(colname).cast("float").alias(colname), target)
        .withColumn("rank", F.ntile(bin_count).over(Window.orderBy(F.desc(colname))))
        .select(colname, "rank", target)
        .groupBy("rank")
        .agg(
            F.count(target).alias("bucket_row_number"),
            F.sum(target).alias("bucket_lead_number"),
            F.avg(colname).alias("avg_model_lead_probability"),
        )
        .withColumn(
            "cum_avg_leads",
            F.avg("bucket_lead_number").over(
                Window.orderBy("rank").rangeBetween(Window.unboundedPreceding, 0)
            ),
        )
    )

    avg_lead_rate = (
        lift_df.filter(F.col("rank") == bin_count)
        .select("cum_avg_leads")
        .collect()[0]
        .cum_avg_leads
    )  # cislo = cum_avg_leads 10. decilu napr(317.2)

    cum_lift_df = lift_df.withColumn(
        "cum_lift", F.col("cum_avg_leads").cast("float") / avg_lead_rate
    ).selectExpr(
        "rank as bucket",
        "bucket_row_number",
        "bucket_lead_number",
        "avg_model_lead_probability",
        "cum_avg_leads",
        "cum_lift",
    )
    return cum_lift_df

# split score
@F.udf(returnType=T.DoubleType())
def ith(v, i):
    try:
        return float(v[i])
    except ValueError:
        return None

def process_multiple_segments_input(t):
    t = t.replace(" ", "")
    
    t_list = list(t.split(","))
        
    t_table_name = t.replace(',', '_')
    
    return {
        'converted_list': t_list,
        'table_name_suffix': t_table_name,
        'db_name': t,
    }

def compute_lift_train_test(predictions_train, predictions_test, label_column):
    lift_train = (
    lift_curve(predictions_train, label_column, 10)
    .select("bucket", "cum_lift")
    .withColumnRenamed("cum_lift", "lift_train")
  )

    lift_test = (
    lift_curve(predictions_test, label_column, 10)
    .select("bucket", "cum_lift")
    .withColumnRenamed("cum_lift", "lift_test")
  )

    return lift_train.join(lift_test, on="bucket")