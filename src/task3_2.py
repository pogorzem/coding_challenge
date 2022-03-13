from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task3_2").getOrCreate()

# columns definition
columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
feature_columns = columns[:-1]

# read dat for learning
model_df = spark.read.csv("../in/iris.csv", inferSchema=True, header=False).toDF(*columns)

# create string index
search_engine_indexer = StringIndexer(inputCol="class", outputCol="label").fit(model_df)
model_df = search_engine_indexer.transform(model_df)

# create column with vector of the features
df_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
model_df = df_assembler.transform(model_df)


# choose columns needed for learning
model_df = model_df.select(["features", "label"])

# split the data training and test sets
training_df, test_df = model_df.randomSplit([0.70, 0.30])

# set params close to task3_1 and train
log_reg = LogisticRegression(regParam=1 / 100000.0, tol=0.0001, standardization=False).fit(training_df)

# evaluiate with test set
train_results = log_reg.evaluate(test_df).predictions

# evaluate of the model
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
accuracy = evaluator.evaluate(train_results)

print("Prediction Accuracy: ", accuracy)
print(f"Test Error = {1.0 - accuracy}")

# sample prediction
pred_data = spark.createDataFrame(
    [(5.1, 3.5, 1.4, 0.2), (6.2, 3.4, 5.4, 2.3)], ["sepal_length", "sepal_width", "petal_length", "petal_width"]
)

# create column with vector of the features for predictions
predictions = df_assembler.transform(pred_data)

# get predictions
predictions = log_reg.transform(predictions.select(predictions["features"]))

# get class names from predictions
inverter = IndexToString(inputCol="prediction", outputCol="class", labels=search_engine_indexer.labels)
predictions = inverter.transform(predictions)

# prepare data to save using pandas
classes_pdf = predictions.select("class").toPandas()

# save data using pandas
classes_pdf.to_csv("../out/out_3_2.txt", index=False)
