from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# set variables
in_folder = "../in"
out_folder = "../out"
out_file = "out_2_2.txt"

# create spark session
spark = SparkSession.builder.appName("task2_2").getOrCreate()

# read parquet file
df = spark.read.parquet(f"{in_folder}/*.parquet")

# do aggregations
out = df.agg(
    F.min(F.col("price")).alias("min_price"),
    F.max(F.col("price")).alias("max_price"),
    F.count("*").alias("row_count"),
).collect()

# convert to dictionary
out_dict = out[0].asDict()

# save output to file
with open(f"{out_folder}/{out_file}", "w") as f:
    # in instruction there were spaces after comma but treat this
    # as done for instruction readability, will cause issues when reading csv
    print(*out_dict.keys(), sep=",", file=f)
    print(*out_dict.values(), sep=",", file=f)
