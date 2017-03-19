"""
The entry point for the spark jobs, spark context is created here

Performs sentiment analysis using Naive Bayes on an incomming stream of
tweets in real time, model trained using the sentiment140 tweets dataset
"""
import json
from pyspark.sql import SparkSession, Row

from jobs.stream import DataStream
from jobs.naive_bayes import NaiveBayesModel, process

from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
	StructType,
	IntegerType,
	LongType,
	StringType,
)


if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("TwitterSentimentAnalysis")\
	.getOrCreate()

	sc = spark.sparkContext
	sc.setLogLevel("ERROR")

	stream = DataStream(sc, spark)

	fields = (
		StructType().add("label", IntegerType(), True)
			.add("id", LongType(), True)
			.add("date", StringType(), True)
			.add("query", StringType(), True)
			.add("user", StringType(), True)
			.add("text", StringType(), True)
	)

	#no need to cache this as we only use it once...
	training_data_frame = \
		spark.read.load(
			"/home/toby/dev/spark/training_data/training.1600000.processed.noemoticon.csv",
			format='csv',
			schema=fields
		)

	test_data_frame = \
		spark.read.load(
			"/home/toby/dev/spark/training_data/test_data.csv",
			format='csv',
			schema=fields
		)

	#initialise and train the model
	naive_bayes = NaiveBayesModel(training_data_frame)

	#print the accuracy
	naive_bayes.calculate_accuracy(test_data_frame)
	model = naive_bayes.get_model()
	lines = stream.get_text_stream()

	words = lines.flatMap(lambda x: x.split('\n'))
	words.foreachRDD(process(spark, model))

	stream.start()
