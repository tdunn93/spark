"""
Spark script to count hashtags comming from a twitter broker

In this instance the SQL catalyst optimiser optimises the transformations
when we convert to the RDD to a DataFrame. These were previously carried out manually
by lambdas on RDD's (This is notoriously slow in python, DF's give more of a level
playing field in terms of performance between lanugaes)
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def get_spark_session_instance(sparkConf):
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
	lines = ssc.socketTextStream("toby-linux", 5555)

	words = (
		lines.flatMap(lambda line: line.split(" "))
			.filter(lambda w: w.startswith("#"))
	)

	# Convert RDDs of the words DStream to DataFrame and run SQL query
	def process(time, rdd):
		print("========= %s =========" % str(time))

		try:
			# Get the singleton instance of SparkSession for each RDD
			spark = get_spark_session_instance(rdd.context.getConf())

			# Convert RDD[String] to RDD[Row] to DataFrame
			row_rdd = rdd.map(lambda w: Row(word=w))
			tweets_data_frame = spark.createDataFrame(row_rdd)

			# Creates a temporary view using the DataFrame.
			tweets_data_frame.createOrReplaceTempView("words")

			# Do word count on table using SQL and print it
			hashtag_count_df = \
				spark.sql("""
					select
						word,
						count(*) as total
					from
						words
					group by
						word
				""")
			hashtag_count_df.show()
		except:
			pass

words.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
