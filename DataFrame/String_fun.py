from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("string_fun").getOrCreate()
df=spark.read.csv(r"/Users/maheswarreddyjallu/Desktop/emps.csv",header=True,inferSchema=True)
df.show()
df_upper = df.withColumn("name_upper", upper(col("Lname")))
df_upper.show()
lower=df.withColumn("lower_case",lower(col("Lname")))
lower.show()
trim=df.withColumn("trim_name",trim(col("Lname")))
trim.show()
subs=df.withColumn('subs',substring(col("Lname"),1,3))
subs.show()
repeate=df.withColumn("duplicat",repeat(col("Lname"),1))
repeate.show()
len=df.withColumn("len",length(col("Lname")))
len.show()