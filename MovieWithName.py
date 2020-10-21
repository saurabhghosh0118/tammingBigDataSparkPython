from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

conf = SparkConf().setAppName("MostPolularMovie").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# WITH BROADCAST VARIABLE
# def movieName():
#     movieNameMap = {}
#     with open(r"D:\pythonProject\tammingBigDataSparkPython\ml-100k\u.item", "r") as file:
#         for line in file:
#             movieNameMap[int(line.split("|")[0])] = line.split("|")[1].split(" ")[0]
#         return movieNameMap
#
#
# nameDict = spark.sparkContext.broadcast(movieName())
#
# mySchema = StructType(
#     [
#         StructField("user_id", IntegerType()),
#         StructField("movie_id", IntegerType()),
#         StructField("rating", IntegerType()),
#         StructField("timestamp", StringType())
#     ]
# )
#
# df1 = spark.read.option("sep", "\t").schema(mySchema).csv(
#     "file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
#
#
# # df1.show(5)
#
# def lookup(movieID):
#     return nameDict.value[movieID]
#
# lookupDef = udf(lookup)
#
# df2 = df1.withColumn("movie_name", lookupDef("movie_id"))
#
# df2.show(5)

#WITH JOIN
name_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

data_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("movie_id", IntegerType()),
        StructField("rating", IntegerType()),
        StructField("timestamp", StringType())
    ]
)

name_df = spark.read.schema(name_schema).option("sep","|").csv(r"D:\pythonProject\tammingBigDataSparkPython\ml-100k\u.item")
name_df.show(5)
name_df.rdd.repartition(2)
print(name_df.rdd.getNumPartitions())
name_df.write.mode("overwrite").csv(r"D:\Output")

data_df = spark.read.option("sep", "\t").schema(data_schema).csv("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
data_df.show(5)
print(data_df.rdd.getNumPartitions())


join_df = name_df.join(data_df,name_df.id == data_df.id,"inner")
join_df.show(5)
print(join_df.rdd.getNumPartitions())

#input("press to stop")
