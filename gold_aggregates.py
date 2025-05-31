from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, countDistinct

spark = SparkSession.builder.appName("GoldAggregates").getOrCreate()

silver_table_path = "/mnt/datalake/silver/retail_sales_clean"
gold_table_path = "/mnt/datalake/gold/daily_sales_aggregates"

# Read Silver layer (batch read for scheduled aggregation)
silver_df = spark.read.format("delta").load(silver_table_path)

# Aggregate daily sales by store and product category
agg_df = (
    silver_df.groupBy("transaction_date", "store_id", "product_category")
    .agg(
        _sum("sales_amount").alias("total_sales"),
        _sum("quantity").alias("total_quantity"),
        _avg("sales_amount").alias("avg_transaction_value"),
        countDistinct("transaction_id").alias("num_transactions")
    )
)

# Write or overwrite daily aggregates in Gold Delta table
agg_df.write.format("delta").mode("overwrite").save(gold_table_path)

print("Gold layer daily sales aggregates updated successfully.")
