from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import to_date, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

conf = SparkConf().setAppName("Max Temperature").setMaster("local[3]")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


def myFields(data_passed):
    field = data_passed.split(",")
    zone_id = field[0]
    date = field[1]
    temp_type = field[2]
    temperature = round(float(field[3]) * 0.1 * (9.0 / 5.0) + 32.0, 2)
    return zone_id, date, temp_type, temperature


my_schema = StructType([
    StructField("zone_id", StringType()),
    StructField("date", StringType()),
    StructField("temp_type", StringType()),
    StructField("temperature", FloatType())
])


#By rdd+df
# rdd1 = sc.textFile(r"D:\pythonProject\tammingBigDataSparkPython\1800.csv")
# rdd2 = rdd1.map(lambda x: myFields(x))
# df2 = spark.createDataFrame(rdd2, my_schema)
# df2.show(5)
# rdd3 = df2.filter("temp_type = 'TMAX'").select("zone_id","temperature").rdd\
#         .reduceByKey(lambda x,y: max(x,y))
# df3 = spark.createDataFrame(rdd3)
# df3.show(5)
# df3 = df2.withColumn("date",to_date('date','yyyyMMdd'))
# df4 = df3.filter("temp_type = 'TMAX'").agg(f.max("temperature"))
# df4.show(5)


# # only Dataframe
# def dataSplit(value):
#     temp = value.split(",")
#     return temp[0]
#
#
# myDataSplit = udf(dataSplit)
odf1 = spark.read.schema(my_schema).csv(r"D:\pythonProject\tammingBigDataSparkPython\1800.csv")
odf1.show(5)
odf2 = odf1.withColumn("temperature",f.round(f.col("temperature") * 0.1 * (9.0 / 5.0) + 32.0, 2))
odf2.write.csv(r"D:\df_output")
odf2.show(5)
odf3 = odf2.filter(odf1.temp_type == "TMAX")
odf3.show(5)
odf4 = odf3.groupBy(odf3.zone_id).max("temperature")
odf4.show(5)