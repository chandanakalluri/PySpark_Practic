from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark=SparkSession.builder.appName("nulls").getOrCreate()

data=[(1,"chandu",20000,"kvp"),(2,"keeru",30000,None),(None,"dev",200000,"ytv")]
schema=["id", "name", "sal", "add"]
df=spark.createDataFrame(data=data,schema=schema)
df.show()
drop = df.na.drop(subset=["name","add","id"])
drop.show()
fillna=df.fillna({"add":"unknown","id":"0"})
fillna.show()
#fetching first on_null values
coalece=df.withColumn("first_not_null",coalesce("id","name","sal"))
coalece.show()
