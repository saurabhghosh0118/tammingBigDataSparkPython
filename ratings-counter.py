from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as f
import collections

from pyspark.sql import SparkSession


ratingRecord = collections.namedtuple("ratingRecord", ["id1", "id2", "rating", "mob"])
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


lines = sc.textFile("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")

#lines = spark.sparkContext.textFile("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
ratings = lines.map(lambda x: x.split())
ratingsWithSchema = ratings.map(lambda x: ratingRecord(int(x[0]), int(x[1]), int(x[2]), x[3]))
df1 = spark.createDataFrame(ratingsWithSchema)
#df1.show(10)
rdd5 = df1.select('rating').rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
df5 = spark.createDataFrame(rdd5).toDF("Rating","Total Count")
df5.show(5)

#df3 = df1.groupBy('rating').agg(f.sum('rating'),f.count('rating'), f.max('rating'))
#df3.show()
#df1.createOrReplaceTempView("ratingView")
#df2 = spark.sql("SELECT rating, COUNT(*) FROM ratingView Group By rating HAVING COUNT(*) > 25000")

# df4 = df1.filter(f.expr('rating > 3')).groupBy('rating').agg(f.count('rating'))
# df4.show()


#df2.show()



#ORIGNAL
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf=conf)
#lines = sc.textFile("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
# ratings = lines.map(lambda x: x.split()[2])
# result = ratings.countByValue()
#
# sortedResults = collections.OrderedDict(sorted(result.items()))
# for key, value in sortedResults.items():
#     print("%s %i" % (key, value))
