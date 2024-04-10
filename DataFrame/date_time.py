from calendar import month
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, day, year, month
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
