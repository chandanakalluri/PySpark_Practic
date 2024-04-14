from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Broadcast Example")
broadcastVar = sc.broadcast([1, 2, 3])
print(broadcastVar)