from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("join").master("local").getOrCreate()

data1 = ((1,"saurabh","780777"),(2,"snehal","787878787"),(3,"sameer","87686756"),
        (4,"bob","986798789"),(5,"sam","8798"))

df1 = spark.createDataFrame(data1, schema="id Int, name String, phone String")
df1.show(5)
df1.createOrReplaceTempView("df1")

data2 = [(1,"saurabh","Platinum"),(2,"snehal","Gold"), (3,"sameer","Silver"),
         (4,"Ram","Ultimate")]
df2 = spark.createDataFrame(data2, schema="id Int, name String, privilage String")
df2.show(5)
df2.createOrReplaceTempView("df2")

#inner join
# df3 = df1.join(df2,df1.id == df2.id,"inner")
# df3.show()
# df4 = df1.join(df2,df1.name == df2.name,"inner")
# df4.show()

#left
# df5 = df1.join(df2,df1.name == df2.name,"left")
# df5.show()
#left semi
# df6 = df1.join(df2,df1.name == df2.name,"leftsemi")
# df6.show()
# df7 = spark.sql("select df1.id, df1.name, df1.phone from df1 left join df2 on df1.name = df2.name where df2.id is not null")
# df7.show()

#left anti-----------------------------------------
# df6 = df1.join(df2,df1.name == df2.name,"leftanti")
# df6.show()
# df7 = spark.sql("select df1.id, df1.name, df1.phone from df1 left join df2 on df1.name = df2.name where df2.id is null")
# df7.show()

#self join------------------------------------------
data3 = ((1,"saurabh",0),(2,"snehal",1),(3,"sameer",1),
        (4,"bob",1),(5,"sam",2))

df8 = spark.createDataFrame(data3, schema="id Int, name String, refrer Int")
df8.show(5)
df8.createOrReplaceTempView("df8")
df9 = spark.sql("select tab2.name, tab1.name from df8 as tab1 join df8 as tab2 on tab1.id = tab2.refrer")
df9.show()