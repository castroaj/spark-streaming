from pyspark.sql import SparkSession
from delta import *
import os

def create_spark_session():
    """
    Creates a SparkSession with Delta Lake and S3/MinIO configurations.
    """
    builder = SparkSession.builder \
        .appName("EnterpriseLakehouseInit") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.warehouse.dir", "s3a://warehouse/")

    # Add required Maven packages for Delta + AWS S3
    # Note: Versions must be compatible with the Spark version in the Dockerfile (3.5.0)
    return configure_spark_with_delta_pip(builder, extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]).getOrCreate()

def init_tables(spark):
    print("--- Initializing Gold Table (Users) ---")
    
    # 1. Create a DataFrame
    data = [
        (1, "Alice", "Engineering", "2023-01-01"),
        (2, "Bob", "Sales", "2023-01-02"),
        (3, "Charlie", "Marketing", "2023-01-03")
    ]
    columns = ["id", "name", "department", "hire_date"]
    df = spark.createDataFrame(data, columns)

    # 2. Write as Delta Table to S3
    # We use 'overwrite' to make this script idempotent
    table_path = "s3a://warehouse/gold/users"
    
    print(f"Writing Delta table to {table_path}...")
    df.write.format("delta").mode("overwrite").save(table_path)
    
    # 3. Register in the Metastore (Catalog)
    # This allows us to use SQL: SELECT * FROM users
    spark.sql(f"CREATE TABLE IF NOT EXISTS users USING DELTA LOCATION '{table_path}'")
    
    print("Table 'users' created successfully.")

def verify_data(spark):
    print("\n--- Verifying Data via SQL ---")
    spark.sql("SELECT * FROM users").show()
    
    print("\n--- Verifying Delta History ---")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, "s3a://warehouse/gold/users")
    dt.history().select("version", "timestamp", "operation").show(truncate=False)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Suppress excessive logging
    spark.sparkContext.setLogLevel("WARN")
    
    init_tables(spark)
    verify_data(spark)
    
    spark.stop()