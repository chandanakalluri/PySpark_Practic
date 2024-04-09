
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InnerJoinExample") \
    .getOrCreate()

# Create two DataFrames
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
df2 = spark.createDataFrame([(1, 25), (2, 30), (4, 35)], ["id", "age"])

# Inner join based on 'id'
inner_join_df = df1.join(df2, "id", "inner")
df1.show()
df2.show()

inner_join_df.show()
join=df1.join(df2,"id","inner")
join.show()
right=df1.join(df2,"id","right")
right.show()
left=df1.join(df2,"id","left")
left.show()
left_s=df1.join(df2,"id","leftsemi")
left_s.show()
left_anti=df1.join(df2,"id","leftanti")
left_anti.show()
#broad=df1.join(broadcast(df2),"id")
#broad.show()
cross=df1.join(df2,"id","cross")
cross.show()