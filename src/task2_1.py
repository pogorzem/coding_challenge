import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# small webscraping just for fun
in_folder = "../in"
url = "https://github.com/databricks/LearningSparkV2/tree/master/mlflow-project-example/data/sf-airbnb-clean.parquet"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")
links = soup.find_all("a", class_="js-navigation-open Link--primary")
for link in links:
    if link["href"].endswith(".parquet"):
        path = link["href"].replace("/blob/", "/raw/")
        file_name = url.split("/")[-1]
        response = requests.get(f"https://github.com{path}")
        with open(f"{in_folder}/{file_name}", "wb") as f:
            f.write(response.content)


# create spark session
spark = SparkSession.builder.appName("task2_1").getOrCreate()

# data into dataframe
df = spark.read.parquet(f"{in_folder}/*.parquet")

# show 5 records
df.show(5)
