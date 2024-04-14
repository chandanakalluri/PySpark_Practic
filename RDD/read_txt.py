from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("reading txt file").getOrCreate()
rdd=spark.sparkContext.textFile(r"/Users/maheswarreddyjallu/Desktop/1.txt")
print(rdd.collect())
print(rdd.count())
rdd2=rdd.flatMap(lambda x:x.split(' '))
print(rdd2.collect())