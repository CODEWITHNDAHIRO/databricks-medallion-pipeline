# src/pipelines/02_cloud_bronze_ingestion.py
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def get_spark_session():
    """
    Returns an active Spark session. 
    If running inside Databricks, it uses the existing cloud session.
    If running locally/CI-CD, it initializes a local session with Delta support.
    """
    try:
        # Check if 'spark' already exists globally (Databricks native environment)
        return spark
    except NameError:
        print("atabricks environment not detected. Initializing cloud-ready local Spark session...")
        from delta import configure_spark_with_delta_pip
        
        extra_packages = ["org.postgresql:postgresql:42.6.0"]
        builder = SparkSession.builder \
            .appName("Cloud_Bronze_Ingestion_Dev") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

def ingest_bronze_cloud():
    print("Starting Cloud-Ready Bronze Ingestion Pipeline...")
    spark_session = get_spark_session()
    
    #  Fetch configurations dynamically (Handles Databricks Secrets or Environment variables)
    # Inside Databricks, replace os.getenv with dbutils.secrets.get(scope="...", key="...")
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ecom_source')
    db_user = os.getenv('DB_USER', 'data_engineer')
    db_password = os.getenv('DB_PASSWORD', 'password123')
    
    #  Define dynamic Storage Base Paths
    # If CLOUD_STORAGE_PATH environment variable is specified, use cloud storage; otherwise, fall back to local dev directory
    storage_base_path = os.getenv('CLOUD_STORAGE_PATH', './storage')
    bronze_output_path = f"{storage_base_path}/bronze/orders"
    
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    
    #  Read Raw Data from Source Database
    print(f"Connecting to database source: {db_host}:{db_port}/{db_name}")
    df_raw = spark_session.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties)
    
    #  Add Metadata Audit Columns
    df_bronze = df_raw.withColumn("ingested_at", current_timestamp()) \
                      .withColumn("source_system", lit("postgres_ecom"))
    
    # Write Data as a Delta Lake Table to Cloud Storage Target
    print(f"Writing Delta table destination -> {bronze_output_path}")
    df_bronze.write.format("delta").mode("overwrite").save(bronze_output_path)
    
    print("Bronze Ingestion complete.")
    df_bronze.show(5)

if __name__ == "__main__":
    # If running locally, load .env file for development purposes
    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from dotenv import load_dotenv
        load_dotenv()
        
    ingest_bronze_cloud()