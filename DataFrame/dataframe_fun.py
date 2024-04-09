from pyspark.sql import SparkSession
from pyspark.sql.types import *
from setuptools.command.alias import *

spark = SparkSession.builder.appName("selecct").getOrCreate()
schema = StructType([StructField("id", IntegerType()),
                     StructField("name", StringType())])
data = [(101, "chandu"), (102, "lavi")]
df = spark.createDataFrame(schema=schema, data=data)
df.show()
columns=(df.select(df["id"])).alias("id_column")
columns.show()
rows=df.select(df["name"])
rows.show()
