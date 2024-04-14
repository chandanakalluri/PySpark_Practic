from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("duplicates").getOrCreate()
schema=["id","name"]
data=[(1,"chandu"),(2,"dev"),(3,"lavi"),(1,"chandu"),(5,"chandu")]
df=spark.createDataFrame(data=data,schema=schema)
df.show()
unique=df.distinct()
unique.show()
duni=df.dropDuplicates().show()
df.dropDuplicates()
df.dropDuplicates(["name"]).show()
df.dropDuplicates(["id","name"]).show()