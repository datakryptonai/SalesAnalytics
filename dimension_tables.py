from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("DimensionTables").getOrCreate()

# Sample product dimension data
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True)
])

product_data = [
    ("P001", "Laptop", "Electronics"),
    ("P002", "T-Shirt", "Clothing"),
    ("P003", "Coffee Maker", "Home Goods")
]

product_df = spark.createDataFrame(product_data, product_schema)

# Write product dimension to Delta
product_df.write.format("delta").mode("overwrite").save("/mnt/datalake/dimensions/product")

# Sample store dimension data
store_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("store_location", StringType(), True)
])

store_data = [
    ("S001", "Store A", "New York"),
    ("S002", "Store B", "San Francisco"),
    ("S003", "Store C", "Chicago")
]

store_df = spark.createDataFrame(store_data, store_schema)

# Write store dimension to Delta
store_df.write.format("delta").mode("overwrite").save("/mnt/datalake/dimensions/store")

print("Dimension tables created successfully.")
