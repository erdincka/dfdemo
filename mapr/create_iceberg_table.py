import configparser

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import StructType, StructField, IntegerType, StringType  # type: ignore

# Read AWS credentials from ~/.aws/credentials
config = configparser.ConfigParser()
config.read("/home/mapr/.aws/credentials")
aws_endpoint = "https://127.0.0.1:9000"
aws_access_key = config["default"]["aws_access_key_id"]
aws_secret_key = config["default"]["aws_secret_access_key"]

iceberg_warehouse = f"s3a://demobucket/iceberg/"
iceberg_table = "demo.users"

# 🚀 Create Spark session with Iceberg and S3 support
spark = (
    SparkSession.builder.appName("IcebergCreation")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", iceberg_warehouse)
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

print("✅ Spark session created")

# 🧊 Create Iceberg table if it doesn't exist
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {iceberg_table} (
        id INT,
        title STRING,
        first STRING,
        last STRING,
        street STRING,
        city STRING,
        state STRING,
        postcode STRING,
        country STRING,
        gender STRING,
        email STRING,
        uuid STRING,
        username STRING,
        password STRING,
        phone STRING,
        cell STRING,
        dob STRING,
        registered STRING,
        large STRING,
        medium STRING,
        thumbnail STRING,
        nat STRING
    )
    USING iceberg
"""
)

print(f"✅ Iceberg table '{iceberg_table}' created")
