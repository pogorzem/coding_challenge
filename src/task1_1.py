import os

import requests
from pyspark.sql import SparkSession

# set variables
in_folder = "../in"
source_file = "groceries.csv"
url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"

# pull file
response = requests.get(url)

# will reise excepiton in case of http error
response.raise_for_status()

# check if in folder exists and if not create it
if not os.path.exists("my_folder"):
    os.makedirs("my_folder")

# save response text to file
with open(f"{in_folder}/{source_file}", "w") as f:
    f.write(response.text)

# create spark session and read file
spark = SparkSession.builder.appName("task1_1").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(f"{in_folder}/{source_file}")
out = rdd.map(lambda x: x.split(","))

# display 5 elements
print(out.take(5))
"""
[
    ["citrus fruit", "semi-finished bread", "margarine", "ready soups"],
    ["tropical fruit", "yogurt", "coffee"],
    ["whole milk"],
    ["pip fruit", "yogurt", "cream cheese ", "meat spreads"],
    ["other vegetables", "whole milk", "condensed milk", "long life bakery product"],
]
"""
