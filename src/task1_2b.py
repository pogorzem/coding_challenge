from pyspark.sql import SparkSession

# set variables
source_file = "../in/groceries.csv"
output_file = "../out/out_1_2b.txt"

# create spark session and read file
spark = SparkSession.builder.appName("task1_2").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(source_file)
f1 = rdd.flatMap(lambda x: x.split(","))

# total count of products
count = f1.count()

# save output to single file
with open(output_file, "w") as f:
    print("Count:", file=f)
    print(count, file=f)
