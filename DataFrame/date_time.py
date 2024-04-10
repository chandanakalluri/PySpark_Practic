from calendar import month
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("date_time").getOrCreate()
schema = StructType([
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("timestamp", StringType(), True)
])
data = [("2024-04-10", "2024-04-15", "2024-04-10 12:34:56")]
df = spark.createDataFrame(data, schema)
df.show()
day = df.withColumn("day", day(to_date(df["start_date"], "yyyy-MM-dd")))
day.show()
year = df.withColumn("year", year(to_date(df["start_date"], "yyyy-MM-dd")))
year.show()
month = df.withColumn("month", month(to_date(df["start_date"], "yyyy-MM-dd")))
month.show()
c_date=df.select(current_date().alias("c_date")).show()
c_t=df.select(current_timestamp()).show()
add_d=df.withColumn("add_5",date_add(df["start_date"],5)).show()
sub_d=df.withColumn("sub3",date_sub(df["start_date"],3)).show()
date_diff=df.select(datediff(df["start_date"],df["end_date"])).show()