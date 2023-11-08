from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Filter').getOrCreate()

df_pyspark = spark.read.csv('test4.csv',header=True,inferSchema=True)

df_pyspark.filter("Salary<=50000").show()
df_pyspark.filter(~(df_pyspark['Salary']<=60000)).show()
df_pyspark.filter((df_pyspark['Salary']<=60000) & (df_pyspark['Salary']>20000 )).show()