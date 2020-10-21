from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import expr

spark = SparkSession.builder \
        .appName("fake_friends") \
        .master("local[3]") \
        .getOrCreate()

my_schema = """id INT, name STRING, age INT, friends_count INT"""

#df1 = spark.read.csv("file:///D:/pythonProject/tammingBigDataSparkPython/fakefriends.csv")
df1 = spark.read.format("csv")\
        .schema(my_schema)\
        .load(r"D:\pythonProject\tammingBigDataSparkPython\fakefriends.csv")

# rdd2 = df1.select('age','friends_count').rdd.mapValues(lambda x: (x,1))
# df2 = spark.createDataFrame(rdd2).toDF("age","friend_count")
# df2.show(5)
#
# rdd3 = rdd2.mapValues(lambda x: x).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
# df3 = spark.createDataFrame(rdd3).toDF("age","reduce_by_age_friend_count")
# df3.show(5)
#
# rdd4 = rdd3.mapValues(lambda x: int(x[0]/x[1]))
# df4 = spark.createDataFrame(rdd4)
# df4.show()

#Doing with dataframe only

onlyDF1 = df1.select(f.col('age'),df1.friends_count).groupBy("age").agg(f.sum("friends_count").alias("sum friends_count"),
                f.round(f.avg("friends_count"),0).alias("avg friend count"),
                f.count("friends_count").alias(" friend count"))
onlyDF1.show(5)

