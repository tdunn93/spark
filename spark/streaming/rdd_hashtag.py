"""
Spark script to count hashtags comming from a twitter broker

In this instance more transofrmations are performed manually
on RDD's before converting to DF.

Using purely RDD's Spark cannot help you in terms of optimistaion... e.g. if
you do a reduceByKey before a filter! Anagoly: why write unportable assembler
when you can let the C compiler optimise stuff just as good!
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from collections import namedtuple


def get_spark_session_instance(sparkConf):
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

	fields = ("tag", "count")
	Tweet = namedtuple("Tweet", fields )

	# Create a socket stream on and filter to only get hashtags
	lines = ssc.socketTextStream("toby-linux", 5555)

	# Converting this to a DF will let the cataylst optimser sort out any 
	# inefficient ordering here...
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
			spark = get_spark_session_instance(rdd.context.getConf())

			# Convert RDD[String] to RDD[Row] to DataFrame
			# we want to convert RDD[Tweet] to RDD[Row]
			# this is where we loose (would duh python..) type safety!
			row_rdd = rdd.map(lambda w: Row(tag=w[0], count=w[1]))
			tweets_data_frame = spark.createDataFrame(row_rdd)

			# Creates a temporary view using the DataFrame.
			tweets_data_frame.createOrReplaceTempView("hashtags")

			# Do word count on table using SQL and print it
			hashtag_count_df = \
				spark.sql("select tag, count from hashtags")
			hashtag_count_df.show()
		except:
			pass

words.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
