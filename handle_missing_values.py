from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('Practice').getOrCreate()

df_pyspark = spark.read.csv('test3.csv',header=True,inferSchema=True)

# threshold and how
df_pyspark.na.drop(how='any',thresh=2).show()

# subset
df_pyspark.na.drop(how='any',subset=['Experience']).show()

# fill missing values
# df_pyspark.na.fill('Missing Values','Experience').show()

imputer = Imputer(
    inputCols=['Age', 'Experience', 'Salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age', 'Experience', 'Salary']]
).setStrategy("mean")

imputer.fit(df_pyspark).transform(df_pyspark).show()
