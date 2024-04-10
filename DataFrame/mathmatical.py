from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("mathematical_fun").getOrCreate()
df = spark.read.csv(r"/Users/maheswarreddyjallu/Desktop/emps.csv", header=True, inferSchema=True)
df.show()
avg = df.agg(avg(df["sal"])).show()
sum = df.agg(sum(df["sal"])).alias("sum_sal").show()
coll_set=df.agg(collect_list(df["Lname"])).show()
coll_get=df.agg(collect_set(df["Fname"])).show()
count=df.agg(count("*")).alias("count").show()
count_d=df.agg(countDistinct(df["Fname"])).show()
