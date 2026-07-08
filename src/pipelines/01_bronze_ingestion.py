# src/pipelines/01_bronze_ingestion.py
import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip

#  Loading Environment Variables
load_dotenv()

#  Initialize PySpark with Delta Lake and PostgreSQL Support
# We bundle both jars into a list so delta-pip config doesn't drop the postgres driver.
extra_packages = [
    "org.postgresql:postgresql:42.6.0"
]

builder = SparkSession.builder \
    .appName("Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Passing the extra postgres jar into the delta pip configuration utility directly
spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

#  Connection Configurations with Local Fallbacks if .env fails
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME', 'ecom_source')
db_user = os.getenv('DB_USER', 'data_engineer')
db_password = os.getenv('DB_PASSWORD', 'password123')

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

def ingest_bronze():
    print("Starting Bronze Ingestion...")
    
    # Read Raw Data from PostgreSQL Source
    df_raw = spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties)
    
    #  Add Metadata Audit Columns
    df_bronze = df_raw.withColumn("ingested_at", current_timestamp()) \
                      .withColumn("source_system", lit("postgres_ecom"))
    
    #  Write Data as a Delta Lake Table
    bronze_path = "./storage/bronze/orders"
    df_bronze.write.format("delta").mode("overwrite").save(bronze_path)
    
    print(f" Successfully wrote raw data to Bronze Layer at: {bronze_path}")
    df_bronze.show()

if __name__ == "__main__":
    ingest_bronze()