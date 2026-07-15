from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, current_timestamp, lit

# 1. Initialize local Spark with Scala 2.13 and Postgres/Delta dependencies
spark = SparkSession.builder \
    .appName("Local_Silver_Cleaning") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.13:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Read raw data from Bronze
print(" Reading raw data from Bronze Delta table...")
df_bronze = spark.read.format("delta").load("./spark_catalog/bronze_orders")

# 3. Apply Initial Type Casting & Deduplication
df_conformed = df_bronze \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_timestamp", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("total_amount", col("total_amount").cast("decimal(10,2)")) \
    .withColumn("order_status", when(col("status").isNull(), "UNKNOWN").otherwise(col("status").upper())) \
    .withColumn("silver_processed_at", current_timestamp())

# 4.  DEFINE DATA QUALITY RULES (Expectations)
# We define conditions that MUST be met to be considered "clean" data.
is_valid_order = col("order_id").isNotNull()
is_valid_customer = col("customer_id").isNotNull()
is_valid_amount = (col("total_amount").isNotNull()) & (col("total_amount") >= 0)
is_valid_date = col("order_timestamp").isNotNull() & (col("order_timestamp") <= current_timestamp())

# Combine rules for the "Pass" filter
clean_data_filter = is_valid_order & is_valid_customer & is_valid_amount & is_valid_date

# 5.  Split Data into Clean and Quarantined
print(" Routing records through Data Quality checks...")

# Clean DataFrame
df_silver = df_conformed.filter(clean_data_filter).select(
    "order_id", "customer_id", "order_timestamp", "total_amount", "order_status", "silver_processed_at"
)

# Quarantine DataFrame (records that failed ANY of the rules)
# We add a reason column to help developers debug the source issue!
df_quarantine = df_conformed.filter(~clean_data_filter) \
    .withColumn("quarantine_reason", 
        when(~is_valid_order, "Missing order_id")
        .when(~is_valid_customer, "Missing customer_id")
        .when(~is_valid_amount, "Negative or null total_amount")
        .when(~is_valid_date, "Invalid or future order_date")
        .otherwise("Unknown DQ Failure")
    ) \
    .withColumn("quarantined_at", current_timestamp())

# 6.  Write both datasets to their respective Delta tables
print(f" Clean records processed: {df_silver.count()}")
print(f"Quarantined records caught: {df_quarantine.count()}")

# Write Clean Data to Silver
print(" Saving clean records to Silver Delta table...")
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./spark_catalog/silver_orders")

# Write Bad Data to Quarantine (using append so we build a history of bad data over time)
if df_quarantine.count() > 0:
    print(" Saving flagged records to Quarantine Delta table...")
    df_quarantine.write \
        .format("delta") \
        .mode("append") \
        .save("./spark_catalog/quarantine_orders")
    
    # Show what failed so we can see it in our terminal
    print("\nPreview of Quarantined Records:")
    df_quarantine.select("order_id", "customer_id", "total_amount", "quarantine_reason").show(5, truncate=False)

print("Silver data quality pipeline executed successfully!")