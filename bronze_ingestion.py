from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define sales schema
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sales_amount", IntegerType(), True),
    StructField("transaction_ts", TimestampType(), True)
])

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# Kafka config
kafka_bootstrap_servers = "kafka-broker:9092"
kafka_topic = "retail_sales_raw"

# Read streaming data from Kafka
bronze_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), sales_schema).alias("data"))
    .select("data.*")
)

# Write to Delta Bronze Table (raw data)
bronze_table_path = "/mnt/datalake/bronze/retail_sales"

query = (
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/datalake/checkpoints/bronze")
    .start(bronze_table_path)
)

query.awaitTermination()
