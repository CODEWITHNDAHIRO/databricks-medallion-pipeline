from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

# 1. Initialize local Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("Local_Gold_Upsert") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.13:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "./spark_catalog/silver_orders"
gold_path = "./spark_catalog/gold_orders_fact"

# 2. Read the latest clean data from Silver
print(" Reading clean data from Silver Delta table...")
df_silver = spark.read.format("delta").load(silver_path)

# 3. Perform the Upsert (MERGE) into Gold
print("Running Silver-to-Gold Incremental Merge...")

# Check if the Gold Delta table already exists
if not os.path.exists(gold_path):
    # FIRST TIME RUN: Bootstrap/initialize the Gold table with Silver data
    print("✨ Gold table doesn't exist yet. Initializing Gold table...")
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_path)
    print(" Gold table successfully initialized!")
else:
    # INCREMENTAL RUNS: Perform the Upsert (MERGE)
    print("⚡ Gold table found. Merging changes incrementally...")
    
    # Load the existing target Gold table as a DeltaTable object
    gold_table = DeltaTable.forPath(spark, gold_path)
    
    # Execute the MERGE logic
    gold_table.alias("target") \
        .merge(
            source=df_silver.alias("updates"),
            condition="target.order_id = updates.order_id"
        ) \
        .whenMatchedUpdate(set={
            "customer_id": "updates.customer_id",
            "order_timestamp": "updates.order_timestamp",
            "total_amount": "updates.total_amount",
            "order_status": "updates.order_status",
            "silver_processed_at": "updates.silver_processed_at"
        }) \
        .whenNotMatchedInsert(values={
            "order_id": "updates.order_id",
            "customer_id": "updates.customer_id",
            "order_timestamp": "updates.order_timestamp",
            "total_amount": "updates.total_amount",
            "order_status": "updates.order_status",
            "silver_processed_at": "updates.silver_processed_at"
        }) \
        .execute()
        
    print("Incremental Merge completed successfully!")

# 4. Preview the updated Gold table
print("\n🏆 Gold Table Preview (Latest Analytical State):")
df_gold = spark.read.format("delta").load(gold_path)
df_gold.orderBy("order_timestamp").show(10, truncate=False)