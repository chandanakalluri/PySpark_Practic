from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("arrays").getOrCreate()
df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])
df.show()
df.select(array_contains(df.a,"b")).show()