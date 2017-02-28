"""
Spark script to count hashtags comming from a twitter broker

In this instance I convert to a DataFrame later, perfoming most of the transformations
on RDD's manually it allows you to maintain more control but Spark cannot help
you in terms of optimistaion... e.g. if you do a reduceByKey before a filter!
anagoly: why write unportable assembler when you can let the C compiler do it just as good!
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from collections import namedtuple


def getSparkSessionInstance(sparkConf):
	if ('sparkSessionSingletonInstance' not in globals()):
		globals()['sparkSessionSingletonInstance'] = SparkSession\
			.builder\
			.config(conf=sparkConf)\
			.getOrCreate()
	return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":
	sc = SparkContext(appName="RDDtoDFSQLHashtagCounter")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 1)

	fields = ("tag", "count" )
	Tweet = namedtuple("Tweet", fields )

	# Create a socket stream on and filter to only get hashtags
	lines = ssc.socketTextStream("toby-linux", 5555)#.window(20)

	# Spark cannot help you here! It does what you tell it...
	# So keep it limited and use DF's!!
	words = (
		lines.flatMap(lambda line: line.split(" "))
			.filter(lambda word: word.startswith("#"))
			.map(lambda word: (word.lower(), 1))
			.reduceByKey(lambda a, b: a + b)
			.map(lambda record: Tweet(tag=record[0], count=record[1]))
	)

	# Convert RDDs of the words DStream to DataFrame and run SQL query
	def process(time, rdd):
		print("========= %s =========" % str(time))

		try:
			# Get the singleton instance of SparkSession
			spark = getSparkSessionInstance(rdd.context.getConf())

			# Convert RDD[String] to RDD[Row] to DataFrame
			# we want to convert RDD[Tweet] to RDD[Row]
			# this is where we loose (would duh python..) type safety!
			rowRdd = rdd.map(lambda w: Row(tag=w[0], count=w[1]))
			wordsDataFrame = spark.createDataFrame(rowRdd)

			# Creates a temporary view using the DataFrame.
			wordsDataFrame.createOrReplaceTempView("hashtags")

			# Do word count on table using SQL and print it
			wordCountsDataFrame = \
				spark.sql("select tag, count from hashtags")
			wordCountsDataFrame.show()
		except:
			pass

words.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
