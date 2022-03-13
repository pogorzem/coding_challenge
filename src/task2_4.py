from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# set variables
in_folder = "../in"
out_folder = "../out"
out_file = "out_2_4.txt"

# create spark session
spark = SparkSession.builder.appName("task2_4").getOrCreate()

# read parquet
df = spark.read.parquet(f"{in_folder}/*.parquet")


# get number of accomodates
accomodates = (
    df.orderBy(F.col("price").asc(), F.col("review_scores_rating").desc())
    .select(F.col("accommodates"))
    .collect()[0][0]
)

# save to single file
with open(f"{out_folder}/{out_file}", "w") as f:
    print(accomodates, file=f)
