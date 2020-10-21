from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///D:/pythonProject/tammingBigDataSparkPython/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))


movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)

results = movies.collect()

for result in results:
    print (result)