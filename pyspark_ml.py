from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName('Filter').getOrCreate()

training = spark.read.csv('test4.csv',header=True,inferSchema=True)

training.printSchema()
featureassembler = VectorAssembler(inputCols=["Age","Experience"], outputCol="Independent Features")
output = featureassembler.transform(training)
output.show()
finalized_data = output.select("Independent Features","Salary")
finalized_data.show()

train_data, test_data = finalized_data.randomSplit([0.75, 0.25])
regressor = LinearRegression(featuresCol='Independent Features', labelCol="Salary")
regressor = regressor.fit(train_data)

print(regressor.coefficients)
print(regressor.intercept)

pred_results = regressor.evaluate(test_data)
pred_results.predictions.show()