
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

raw_dev = spark.read.format("json").option("inferSchema","true").load("./Storage/device/*.json")

raw_dev.show(10,False)

raw_dev.write.format("delta").mode("overwrite").save("./Warehouse/Bronze/device")

raw_subscription = spark.read.format("json").option("inferSchema","true").load("./Storage/subscription/*.json")

raw_subscription.show(10,False)

raw_subscription.write.format("delta").mode("overwrite").save("./Warehouse/Bronze/subscription")

spark.stop()


