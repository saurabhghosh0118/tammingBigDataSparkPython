from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

spark = SparkSession.builder.master("local").appName("bad records").getOrCreate()

my_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("salary", IntegerType()),
    StructField("_corrupt_record", StringType())
])
#df1 = spark.read.option("mode", "DROPMALFORMED").schema("id Integer, name String, age Integer, salary Integer").csv("bad_records")
df1 = spark.read.schema(my_schema).csv("bad_records")
df1.show()