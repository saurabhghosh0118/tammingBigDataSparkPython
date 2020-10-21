from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as f

conf = SparkConf().setAppName("Total amount spent by Customer").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

my_schema = StructType([
    StructField("id",IntegerType()),
    StructField("_",StringType()),
    StructField("amount",FloatType())
])

df1 = spark.read.format("csv") \
        .schema(my_schema) \
        .load(r"D:\pythonProject\tammingBigDataSparkPython\customer-orders.csv")
print(df1.schema)
df2 = df1.groupBy("id").agg(f.sum("amount").alias("spent_amount")).orderBy(f.desc("spent_amount"))
print(df2.schema)
df2.show(5)