from pyspark.sql import Row
from pyspark.sql.types import (
	StructType,
	StringType,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
	RegexTokenizer,
	StopWordsRemover,
	HashingTF,
)
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


class NaiveBayesModel:
"""
Creates a Naive Bayes model using pipelines
"""
	def __init__(self, training_data):
		self.training_data = training_data

		self.regex_tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
		self.remover = StopWordsRemover(inputCol=self.regex_tokenizer.getOutputCol(), outputCol="filtered")
		self.hashing_tf = HashingTF(inputCol=self.remover.getOutputCol(), outputCol="features")

		#column names "features" and "labels" are defaults in the spark ml NB API
		#so no need to specify columns to run model on
		self.naive_bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")

		self.model = (
			Pipeline(stages=[
				self.regex_tokenizer,
				self.remover,
				self.hashing_tf,
				self.naive_bayes
			])
			.fit(training_data)
		)

	def get_model(self):
		return self.model

	def calculate_accuracy(self, test_data):
		predictions = self.model.transform(test_data)

		evaluator = MulticlassClassificationEvaluator(
			labelCol="label", predictionCol="prediction",
			metricName="accuracy"
		)

		accuracy = evaluator.evaluate(predictions)
		print("Model accuracy: %s" % accuracy)


def process(spark, model):
	"""
	takes a model and performs a transformation on the incomming rdd on it
	"""
	def _process(time, rdd):
		print("========= %s =========" % str(time))

		try:
			#convert RDD[String] to RDD[Row] to DataFrame
			words_data_frame = spark.createDataFrame(
				rdd.map(lambda y: Row(text=y))
			)

			model.transform(words_data_frame).select(
				"words",
				"prediction"
				).show(truncate=False)

		except:
			pass

	return _process
