import os
from pyspark.sql import SparkSession
print("Starting spark...")
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
print("Spark started. Testing computation...")
df = spark.createDataFrame([("A", 1), ("B", 2)], ["letter", "number"])
df.show()
print("Done!")
spark.stop()
