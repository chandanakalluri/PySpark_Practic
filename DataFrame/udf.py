from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

spark=SparkSession.builder.appName("select").getOrCreate()
df=spark.read.csv(r"/Users/maheswarreddyjallu/Desktop/emps.csv",header=True,inferSchema=True)
df.show()
def sal(s):
    if s>20000:
        return "good"
    else:
        return "improve"
res=udf(sal,StringType())
df=df.withColumn("ststus", res(df["sal"]))
df.show()
pivote=df.groupBy().pivote(df("sal").sum(df("sal")))
pivote.show()