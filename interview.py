from collections import Counter

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import to_date, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType, MapType, Row

conf = SparkConf().setAppName("Max Temperature").setMaster("local[3]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


#Question 1 =
# temp1 ={}
#
# my_schema = StructType([
#     StructField("id", IntegerType()),
#     StructField("value", StringType())
# ])
#
# def myDict(col):
#     global temp1
#     x= {}
#     for y in col:
#         if y in x:
#             x[y] = x[y] + 1
#         else:
#             x[y] = 1
#     temp1 = x
#     return x
#
#
# my_udf = udf(lambda s: dict(Counter(s.split(' '))), MapType(StringType(), IntegerType()))
# my_udf2 = udf(lambda x: myDict(x), MapType(StringType(), IntegerType()))
#
#
# df1 = spark.read.schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Graph2")
# df1.show()
# df2 = df1.withColumn("values", f.split(df1.value, " ")).drop(df1.value)
# df2.show()
# df3 = df1.withColumn('value', my_udf(df1.value))
# df3.show()
# df4 = df2.withColumn("new", my_udf2(df2.values))
# df4.show()
#
# print(temp1)

#Question 2
data = [Row(101,[[1,2],[3]]),
        Row(102,[[1],[2],[1,2]]),
        Row(103,[[1],[1],[1]]),

      ]

my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("value", ArrayType(ArrayType(IntegerType())))
])

def my_def1(col2):
    # x = dict(Counter(col2))
    # y = [k for k,v in x.items() if v > 1 ]
    print(col2)
    print(type(col2))
    temp = [item for items in col2 for item in items]
    x = dict(Counter(temp))
    print(x)
    y = [k for k, v in x.items() if v > 1]
    print(y)
    return len(y)

my_udf1 = udf(lambda x: my_def1(x), IntegerType())

df1 = spark.createDataFrame(data, schema=my_schema)
df1.show()
print(df1.schema)

df2 = df1.withColumn("new", my_udf1(df1.value))
df2.show()