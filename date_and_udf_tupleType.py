# [("a",2,"2020-01-02"),("b",1,"2020-01-01"),("c",5,"2020-01-05")]
from datetime import timedelta

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType

my_schema = StructType([
    StructField("id", StringType()),
    StructField("value1", IntegerType()),
    StructField("date1", DateType())
])
spark = SparkSession.builder.master("local").appName("interview2").getOrCreate()

df1 = spark.read.option("sep", "  ").schema(my_schema).option("dateFormat", "yyyy-MM-dd").csv("Marvel+Graph2")
df1.show()


def my_def(col1, col2, col3):
    list1 = []
    for i in range(1, col2 + 1):
        x = (col1, i, col3 + timedelta(1))
        list1.append(x)
    print(list1)
    return list1


tuple_schema = ArrayType(StructType([
    StructField("id2", StringType()),
    StructField("value2", IntegerType()),
    StructField("date2", DateType())
]))

my_udf = f.udf(lambda x, y, z: my_def(x, y, z), tuple_schema)

df2 = df1.withColumn("new", my_udf(df1.id, df1.value1, df1.date1))
df2.select(f.explode(df2.new)).show()
