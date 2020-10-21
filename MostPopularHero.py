from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import to_date, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

conf = SparkConf().setAppName("Max Temperature").setMaster("local[3]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()



my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

#df1 = spark.read.schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Graph")
df1 = spark.read.text(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Graph")
# df1.show(5)

df2 = df1.withColumn("id",f.split(df1.value," ")[0].cast("integer"))\
    .withColumn("friends",f.size(f.split(df1.value," ")).cast("integer")).drop(df1.value)
df2.show(5)

df3 = spark.read.schema(my_schema).option("sep"," ").csv(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Names")
df3.show(5)

df4 = df3.join(df2,df2.id == df3.id,"inner").select(df3.name,df2.id,df2.friends)
df4.show(5)

df5 = df4.groupBy(df4.name).agg(f.sum(df4.friends))

df5.show(5)

