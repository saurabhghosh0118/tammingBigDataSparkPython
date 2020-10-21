from collections import namedtuple
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

conf = SparkConf().setMaster("local").setAppName("min_temperature")
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf=conf)
# spark = SparkSession.builder.config(conf=conf).getOrCreate()


TempRecord = namedtuple('Record', ['id', 'date', 'temperature', 'tmp_value'])


def split_data(lines):
    line = lines.split(',')
    id = line[0]
    date = line[1]
    temperature = line[2]
    tmp_value = int(line[3])
    return id, date, temperature, tmp_value


my_schema = StructType([
    StructField("id", StringType()),
    StructField("date", StringType()),
    StructField("temperature", StringType()),
    StructField("tmp_value", IntegerType())
])

rdd1 = sc.textFile(r"D:\pythonProject\tammingBigDataSparkPython\1800.csv")

# rdd2 = rdd1.map(lambda x: x.split(','))
rdd2 = rdd1.map(lambda x: split_data(x))

# rdd3 = rdd2.map(lambda x: TempRecord(x[0],x[1],x[2],x[3]))

df1 = spark.createDataFrame(rdd2, my_schema)
# df2 = df1.filter("temperature == 'TMIN'")
df2 = df1.withColumn('date', to_date('date', 'yyyyMMdd'))
df1.select("id").distinct().show()
# df1.show(5)
# df2.show(5)
# print(df2.schema.simpleString())
rdd1 = df1.filter("temperature == 'TMIN'").rdd.map(lambda x: (x[0], x[3])).reduceByKey(lambda x,y: min(x,y))
df4 = spark.createDataFrame(rdd1)
df4.show()
df3 = df2.filter("temperature == 'TMIN'").groupBy("id").agg(f.min("tmp_value"))
df3.show()