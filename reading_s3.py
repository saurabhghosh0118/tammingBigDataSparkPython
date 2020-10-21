from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set("spark.jars.packages","com.amazonaws:aws-java-sdk:1.7.4")
conf.set("spark.jars","org.apache.hadoop:hadoop-aws:2.7.2")

#-packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7

spark = SparkSession.builder\
        .appName("cassandra")\
        .master("local[3]")\
        .config(conf=conf)\
        .getOrCreate()

df1 = spark.read.text("s3a://commoncrawl/crawl-data/CC-MAIN-2020-16/segment.paths.gz")
df1.show(5)