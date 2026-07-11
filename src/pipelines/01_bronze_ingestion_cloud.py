#Databricks Notebook: 01_bronze_ingestion_cloud
from pyspark.sql.functions import current_timestamp, lit
# ACTIVE Ngrok Tunnel forwading endpoints
db_host ="5.tcp.eu.ngrok.io"
db_port = "25497"

# DATABASE credentials matching your local Docker setup
db_name = "ecom_source"
db_user = "data_engineer"
db_password = "password123"

#  ESTABLISHING THE JDBC CLOUD CONNECTION URL
jdbc_url = f"jdbc:postgresql://{5.tcp.eu.ngrok.io}:{25497}/{ecom_source}" 
connection_properties = {
    "user": "data_engineer",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}
print(f"Attempting cloud handshake with local DB via tunnel:{jdbc:postgresql://{5.tcp.eu.ngrok.io}:{25497}/{ecom_source}}")

# Extract data from local Docker over the web 
df_raw = spark.read.jdbc(url = jdbc_url, table = "orders",properties = connection_properties)

#Transform into Bronze format 
df_bronze = df_raw.withColumn("ingested_at", current_timestamp()) \
                    .withColumn("source_system", lit("postgres_ecom"))

# save as a Managed Delta Table in the CLoud Catalog
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

print(" successfully ingested raw data into cloud bronze layer!")
display(df_bronze.limit(5))