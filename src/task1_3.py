from operator import add

from pyspark.sql import SparkSession

# set variables
source_file = "../in/groceries.csv"
output_file = "../out/out_1_3.txt"

# create spark session and read file
spark = SparkSession.builder.appName("task1_3").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(source_file)
in_rdd = rdd.flatMap(lambda x: x.split(","))

# rdd mapped [product,1]
rdd_map_prod = in_rdd.map(lambda x: [x, 1])

# rdd reduced
rdd_prod_red = rdd_map_prod.reduceByKey(add)

# rdd sorted and top 5 selected
rdd_top_5 = rdd_prod_red.sortBy(keyfunc=lambda x: x[1], ascending=False).take(5)

# save to file
with open(output_file, "w") as f:
    for row in rdd_top_5:
        print(row, file=f)
