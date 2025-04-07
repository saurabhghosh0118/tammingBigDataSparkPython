from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Max Temperature").setMaster("local[3]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

rdd1 = sc.textFile(r"D:\pythonProject\tammingBigDataSparkPython\Book")
rdd2 = rdd1.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: (x+y))
df2 = spark.createDataFrame(rdd2)
df2.show(20)
#This comment is written in test branch
#This is my local branch