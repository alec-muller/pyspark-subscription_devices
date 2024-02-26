from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

domain_table_subscriptions = spark.read.format("delta").load("./Warehouse/Silver/subscriptions/")

domain_table_subscriptions.show(10,False)

# ### Plans

df_plans = (
    domain_table_subscriptions
    .select(
        "user_id",
        "plan",
        "price",
        col("subscription_importance").alias("importance"),
        "model",
        "event_datetime"
    )
    .filter(col("plan").isNotNull())
)

df_plans.show(10,False)

df_plans.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("./Warehouse/Gold/plans")

# ### Models

domain_table_subscriptions.show(10,False)

df_models = (
    domain_table_subscriptions
    .groupBy("model")
    .agg(
        f.count("plan").alias("total_plans"),
        f.count_distinct("plan").alias("distinct_plans"),
        f.sum("price").alias("total_value")
    )
    .sort(col("total_value").desc())
)

df_models.show(10,False)

df_models.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("./Warehouse/Gold/models")

spark.stop()


