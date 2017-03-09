from pyspark.ml.feature import (
	Tokenizer,
	RegexTokenizer,
	StopWordsRemover,
	HashingTF,
)
from pyspark.ml import Pipeline

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
	StructType,
	IntegerType,
	StringType,
	DateType,
)

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("TokenizerExample")\
	.getOrCreate()

	fields = (
		StructType().add("label", IntegerType(), True)
			.add("id", IntegerType(), True)
			.add("date", StringType(), True)
			.add("query", StringType(), True)
			.add("user", StringType(), True)
			.add("text", StringType(), True)
	)

	input_data_frame = \
		spark.read.load(
			"/home/toby/dev/spark/training_data/training.1600000.processed.noemoticon.csv/",
			format='csv',
			schema=fields
		)

	tweets_data_frame = input_data_frame.select("label", "text")
	tweets_data_frame.cache()

	splits = tweets_data_frame.randomSplit([0.8, 0.2], 1234)
	train = splits[0]
	test = splits[1]

	regex_tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
	remover = StopWordsRemover(inputCol=regex_tokenizer.getOutputCol(), outputCol="filtered")
	hashing_tf = HashingTF(inputCol=remover.getOutputCol(), outputCol="features")

	# features=features label=label are defaulted in the NB API...
	# so no need to specify columns
	# can just pipeline the DF, no need to turn into labelled point!
	naive_bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")
	pipeline = Pipeline(stages=[regex_tokenizer, remover, hashing_tf, naive_bayes])

	out = pipeline.fit(train).transform(test)#.createOrReplaceTempView("nb") #select("label", "filtered", "prediction").show(truncate=False)
	#spark.sql("SELECT label, filtered, prediction from nb").show(truncate=False)
	#use these to show flow individually...
	evaluator = MulticlassClassificationEvaluator(
		labelCol="label", predictionCol="prediction",
		metricName="accuracy"
	)

	accuracy = evaluator.evaluate(out)
	print(accuracy)
#	regexTokenized = regex_tokenizer.transform(train)
#	remover = remover.transform(regexTokenized)
#	hashingtf = hashing_tf.transform(remover).select("label", "features").show(truncate=False)

#	spark.stop()
