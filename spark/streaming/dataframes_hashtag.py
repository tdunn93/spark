"""
Spark script to count hashtags comming from a twitter broker

In this instance I let the SQL catalyst optimiser handle more of the work
by converting to a DataFrame sooner, these were previously carried out manually
by lambdas on RDD's which is notoriously slow in python (DF's more of a level
playing field between lanugaes
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
	sc = SparkContext(appName="DataFramesSQLHashtagCounter")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 1)

	# Create a socket stream on and filter to only get hashtags
	lines = ssc.socketTextStream("toby-linux", 5555)#.window(20)

	words = (
		lines.flatMap(lambda line: line.split(" "))
			.filter(lambda w: w.startswith("#"))
	)

	# Convert RDDs of the words DStream to DataFrame and run SQL query
	def process(time, rdd):
		print("========= %s =========" % str(time))

		try:
			# Get the singleton instance of SparkSession for each RDD
			spark = getSparkSessionInstance(rdd.context.getConf())

			# Convert RDD[String] to RDD[Row] to DataFrame
			rowRdd = rdd.map(lambda w: Row(word=w))
			wordsDataFrame = spark.createDataFrame(rowRdd)

			# Creates a temporary view using the DataFrame.
			wordsDataFrame.createOrReplaceTempView("words")

			# Do word count on table using SQL and print it
			wordCountsDataFrame = \
				spark.sql("""
					select
						word,
						count(*) as total
					from
						words
					group by
						word
				""")
			wordCountsDataFrame.show()
		except:
			pass

words.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
