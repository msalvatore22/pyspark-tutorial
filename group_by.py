from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Agg').getOrCreate()

df_pyspark = spark.read.csv('test5.csv',header=True,inferSchema=True)

# group by
df_pyspark.groupBy('Name').max().show()
df_pyspark.groupBy('Department').sum().show()
df_pyspark.groupBy('Department').mean().show()
df_pyspark.groupBy('Department').count().show()

# agg
df_pyspark.agg({'Salary': 'sum'}).show()