from pyspark.sql import SparkSession

# set variables
source_file = "../in/groceries.csv"
output_file = "../out/out_1_2a.txt"

# set spark session and read file
spark = SparkSession.builder.appName("task1_2a").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(source_file)
out = rdd.flatMap(lambda x: x.split(","))

# collect unique product list
unique_list = out.distinct().collect()

# save to single file
with open(output_file, "w") as f:
    print(*unique_list, sep="\n", file=f)
