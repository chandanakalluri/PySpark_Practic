# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import *
# from setuptools.command.alias import *
#
# spark = SparkSession.builder.appName("selecct").getOrCreate()
# schema = StructType([StructField("id", IntegerType()),
#                      StructField("name", StringType())])
# data = [(101, "chandu"), (102, "lavi")]
# df = spark.createDataFrame(schema=schema, data=data)
# df.show()
# columns=(df.select(df["id"])).alias("id_column")
# columns.show()
# rows=df.select(df["name"])
# rows.show()
#dataframe functions:
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import *

spark=SparkSession.builder.appName("dataframefun").getOrCreate()
schema=StructType([StructField("id",IntegerType()),
                   StructField("name",StringType()),
                   StructField("age",IntegerType()),
                   StructField("gender",StringType())
                   ])
data=[(1,"chandu",23,"f"),(2,"gani",24,"f"),(3,"lakshmi",22,"f"),(4,"k7",24,"m"),(5,"david",20,"m")]

df=spark.createDataFrame(data=data,schema=schema)
df.show()
count=df.groupBy("age").count()
count.show()
#Filter()
fil=df.filter(df["id"]==2)
fil.show()
where=df.where(df["id"]==4)
where.show()
#Like()
like=df.filter(df["name"].like("k%"))
like.show()
alias=df.select(df["name"].alias("names"))
alias.show()
st=df.withColumn("status",when(df["age"]>20,"young").otherwise("old"))
st.show()