from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# set variables
in_folder = "../in"
out_folder = "../out"
out_file = "out_2_3.txt"

# create spark session
spark = SparkSession.builder.appName("task2_3").getOrCreate()

# read parquet file
df = spark.read.parquet(f"{in_folder}/*.parquet")

# do aggregations
out = (
    df.filter((F.col("price") > 5000) & (F.col("review_scores_value") == 10.0))
    .agg(F.avg(F.col("bathrooms")).alias("avg_bathrooms"), F.avg(F.col("bedrooms")).alias("avg_bedrooms"))
    .collect()
)

# create output dict
out_dict = out[0].asDict()

# save to file
with open(f"{out_folder}/{out_file}", "w") as f:
    print(*out_dict.keys(), sep=",", file=f)
    print(*out_dict.values(), sep=",", file=f)
