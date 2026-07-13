from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, current_timestamp

# 1. Initialize local Spark with BOTH Postgres and Delta Lake dependencies explicitly loaded
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaPipeline") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("📖 Reading raw data from the Bronze Delta table...")

# Read from our local Bronze Delta directory
df_bronze = spark.read.format("delta").load("./spark_catalog/bronze_orders")

#  Apply Silver Cleaning Transformations
df_silver = df_bronze \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("total_amount") > 0) \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_timestamp(col("order_date"))) \
    .withColumn("cleaned_at", current_timestamp())

#  Save as a local Silver Delta Table
print(" Saving clean records to the Silver Delta Table...")
df_silver.write.format("delta").mode("overwrite").save("./spark_catalog/silver_orders")

print(" Successfully created the Local Silver Layer!")
df_silver.show(5)

# Print a quick count comparison to see how many bad records were filtered
print(f" Bronze Records: {df_bronze.count()} | Cleaned Silver Records: {df_silver.count()}")