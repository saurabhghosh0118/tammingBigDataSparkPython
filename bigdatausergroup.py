from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import to_date, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

conf = SparkConf().setAppName("Max Temperature").setMaster("local[3]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("type", StringType())
])


# def my_fun(x, y):
#     if y == "tri":
#         return x + 1
#     else:
#         return x
#
#
# def my_fun2(lines):
#     x = 0
#     fields = lines.split(" ")
#     for field in fields:
#         x = x + field
#     return x
#
#
# df1 = spark.read.schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\bigdatausergroup")
# # df1.show()
# 
# tem_fun = udf(lambda x, y: my_fun(x, y), IntegerType())
#
# df2 = df1.withColumn("value", tem_fun(df1.id, df1.type))
# # df2.show()
#
# df3 = df1.withColumn("new", f.when(df1.type == "tri",df1.id + 1).when(df1.type == "event",df1.id**2).otherwise(df1.id))
# df3.show()


df1 = spark.read.schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\bigdatausergroup")
df1.show()