import pyspark
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()

# df_pyspark = spark.read.csv('test.csv')
# print(df_pyspark.show())

df_pyspark = spark.read.option('header','true').csv('test.csv')
df_pyspark.show()
print(type(df_pyspark))
print(df_pyspark.printSchema())
