from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("SilverCleanEnrich").getOrCreate()

bronze_table_path = "/mnt/datalake/bronze/retail_sales"
silver_table_path = "/mnt/datalake/silver/retail_sales_clean"
product_dim_path = "/mnt/datalake/dimensions/product"
store_dim_path = "/mnt/datalake/dimensions/store"

# Read Bronze data stream
silver_input_df = spark.readStream.format("delta").load(bronze_table_path)

# Read dimension data (static)
product_dim = spark.read.format("delta").load(product_dim_path)
store_dim = spark.read.format("delta").load(store_dim_path)

# Clean and enrich
silver_df = (
    silver_input_df
    .dropDuplicates(["transaction_id"])
    .filter(col("sales_amount") > 0)
    .withColumn("transaction_date", expr("CAST(transaction_ts AS DATE)"))
    .join(product_dim, "product_id", "left")
    .join(store_dim, "store_id", "left")
)

# Write to Silver Delta table
query = (
    silver_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/datalake/checkpoints/silver")
    .start(silver_table_path)
)

query.awaitTermination()
