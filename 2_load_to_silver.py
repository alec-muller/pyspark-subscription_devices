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

# ### Device

bronze_device = spark.read.format("delta").load("./Warehouse/Bronze/device/")

bronze_device.show(10,False)

bronze_device.printSchema()

silver_device = (
    bronze_device
    .withColumn("dt_current_timestamp", f.date_format((col("dt_current_timestamp")/1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    .withColumns(
        {
            "manufacturer": f.trim(f.initcap("manufacturer")),
            "model": f.trim(f.initcap("model")),
            "platform": f.trim(f.initcap("platform"))
        }
    )
    .select(
        col("uid").alias("_id"),
        col("id").alias("id_device"),
        "build_number",
        "manufacturer",
        "model",
        "platform",
        "version",
        "user_id",
        col("dt_current_timestamp").alias("device_eventdate"),
    )
)

silver_device.printSchema()

# ### Subscription

bronze_subscription = spark.read.format("delta").load("./Warehouse/Bronze/subscription/")

bronze_subscription.show(10,False)

silver_subscription = (
    bronze_subscription
    .withColumns(
        {
            "payment_method": f.initcap(f.trim(col("payment_method"))),
            "payment_term": f.initcap(f.trim(col("payment_term"))),
            "plan": f.initcap(f.trim(col("plan"))),
            "status": f.initcap(f.trim(col("status"))),
            "subscription_term": f.initcap(f.trim(col("subscription_term"))),
            "dt_current_timestamp": f.date_format((col("dt_current_timestamp")/1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
        }
    )
    .withColumn(
        "subscription_importance",
        f.when(col("plan").isin("Platinum", "Diamond", "Premium", "Business", "Gold"), "High")
        .when(col("plan").isin("Silver", "Professional", "Bronze", "Standard"), "Medium")
        .when(col("plan").isin("Starter", "Essential", "Basic") & col("subscription_term").isin("Lifetime", "Triennal", "Biennal"), "Medium")
        .otherwise("Low")
    )
    .withColumn("subscription_price", f.expr(
        """
        CASE plan
            WHEN 'Basic' THEN 6.00
            WHEN 'Bronze' THEN 8.00
            WHEN 'Business' THEN 10.00
            WHEN 'Diamond' THEN 14.00
            WHEN 'Essential' THEN 9.00
            WHEN 'Free Trial' THEN 0.00
            WHEN 'Gold' THEN 25.00
            WHEN 'Platinum' THEN 9.00
            WHEN 'Premium' THEN 13.00
            WHEN 'Professional' THEN 17.00
            WHEN 'Silver' THEN 11.00
            WHEN 'Standard' THEN 13.00
            WHEN 'Starter' THEN 5.00
            WHEN 'Student' THEN 2.00
            ELSE 0.00
        END
        """
        )
    )
    .select(
        col("uid").alias("_id"),
        col("id").alias("id_subscription"),
        "payment_method",
        "payment_term",
        "plan",
        "subscription_price",
        "status",
        "subscription_term",
        "subscription_importance",
        "user_id",
        col("dt_current_timestamp").alias("subscription_eventdate")
    )
)

silver_subscription.printSchema()

# ### Domain Table - Silver

silver_device.show(5,False)

silver_subscription.show(5,False)

domain = silver_device.join(silver_subscription, "user_id", how="left")

domain.show(5,False)

silver_subscriptions = (
    domain
    .select(
        "user_id",
        "id_device",
        "manufacturer",
        "model",
        "platform",
        col("subscription_price").alias("price"),
        "payment_method",
        "payment_term",
        "plan",
        "status",
        "subscription_term",
        "subscription_importance",
        col("subscription_eventdate").alias("event_datetime")
    )
)

silver_subscriptions.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("./Warehouse/Silver/subscriptions")

spark.stop()


