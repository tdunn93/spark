"""
	Encapsulate a spark DStream
"""
from pyspark.streaming import StreamingContext
from pyspark.sql import Row


class DataStream:
	def __init__(self, sc, spark_session, port=5555):
		self.spark = spark_session
		self.ssc = StreamingContext(sc, 1)

		#create a socket stream on port 5555
		self.lines = self.ssc.socketTextStream("toby-linux", port)

	def get_text_stream(self):
		return self.lines

	def start(self):
		self.ssc.start()
		self.ssc.awaitTermination()
