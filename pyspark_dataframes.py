from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()
# read the dataset

# df_pyspark = spark.read.option('header', 'true').csv('test2.csv', inferSchema=True)
df_pyspark = spark.read.csv('test2.csv',header=True,inferSchema=True)
df_pyspark.show()
df_pyspark.printSchema()

# select a column
df_pyspark.select(['Name', 'Experience']).show()
print(df_pyspark.dtypes)
df_pyspark.describe().show()

# adding column in data frame

df_pyspark = df_pyspark.withColumn('Experience After 2 year', df_pyspark['Experience']+2)
df_pyspark.show()

# drop the column

df_pyspark = df_pyspark.drop('Experience After 2 year')
df_pyspark.show()

# rename the column

df_pyspark = df_pyspark.withColumnRenamed('Name', 'New Name')
df_pyspark.show()