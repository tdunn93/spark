"""
Twitter sentiment analyser, trained using the sentiment140 tweets dataset
"""
from pyspark import SparkContext

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
	RegexTokenizer,
	StopWordsRemover,
	HashingTF,
)
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
	StructType,
	IntegerType,
	LongType,
	StringType,
)

if __name__ == "__main__":
	sc = SparkContext()
	spark = SparkSession\
	.builder\
	.appName("TwitterSentimentAnalysis")\
	.getOrCreate()

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

	negative_string = (
		sc.parallelize(["im so pissed off this is obviously not positive"])
		.map(lambda x: x.split(' '))
		.map(lambda y: Row(y))
		.toDF(schema=StructType().add("text", StringType(), True))
	)

	positive_string = (
		sc.parallelize(["im so happy with my results"])
		.map(lambda x: x.split(' '))
		.map(lambda y: Row(y))
		.toDF(schema=StructType().add("text", StringType(), True))
	)

	test_data_frame.cache()

	regex_tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
	remover = StopWordsRemover(inputCol=regex_tokenizer.getOutputCol(), outputCol="filtered")
	hashing_tf = HashingTF(inputCol=remover.getOutputCol(), outputCol="features")

	#Column names "features" and "labels" are defaults in the ml NB API
	#so no need to specify columns to run model on
	naive_bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")

	#Can just pipeline the DF, no need to turn into labelled point!
	pipeline = Pipeline(stages=[regex_tokenizer, remover, hashing_tf, naive_bayes])

	#form the model
	model = pipeline.fit(training_data_frame)

	#run the prediciton
	predictions = model.transform(test_data_frame)

	#work out the accuracy
	evaluator = MulticlassClassificationEvaluator(
		labelCol="label", predictionCol="prediction",
		metricName="accuracy"
	)

	accuracy = evaluator.evaluate(predictions)
	print("Model accuracy: %s" % accuracy)

	#sample prediction
	model.transform(negative_string).select("filtered", "prediction").show(truncate=False)
	model.transform(positive_string).select("filtered", "prediction").show(truncate=False)

	spark.stop()
