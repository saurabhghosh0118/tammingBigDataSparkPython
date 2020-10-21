from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

spark = SparkSession.builder.master("local").appName("obscure_superhero").getOrCreate()

my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])
df1 = spark.read.option("sep"," ").schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Names")
df1.show(5)

df2 = spark.read.text(r"D:\pythonProject\tammingBigDataSparkPython\Marvel+Graph")
df3 = df2.withColumn("id",f.split(df2.value," ")[0])\
        .withColumn("friendcount", f.size(f.split(df2.value," ")).cast(IntegerType())-1).drop(df2.value)
df3.show(5)
print(df3.schema)
df4 = df3.filter(df3.friendcount == 1)
df4.show(5)
df5 = df4.join(df1,df1.id == df4.id,"inner")
df5.show()
