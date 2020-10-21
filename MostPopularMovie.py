from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("MostPolularMovie").setMaster("local[3]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

nTuple = namedtuple("movie",["user_id","movie_id","rating","timestamp"])

# rdd1 = sc.textFile("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
# movies = rdd1.map(lambda x: (int(x.split()[1]), 1))

