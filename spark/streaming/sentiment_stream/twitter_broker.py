"""
Twitter broker as pyspark does not have in built twitter support...
"""
import socket
import sys
import json

from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''


class TweetsListener(StreamListener):

	def __init__(self, csocket):
		self.client_socket = csocket

	def on_data(self, data):
		try:
			print data
			self.client_socket.send(data)
			return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True

	def on_error(self, status):
		print(status)
		return True

def sendData(c_socket):
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	twitter_stream = Stream(auth, TweetsListener(c_socket))
	twitter_stream.filter(track=['brexit'])

if __name__ == "__main__":
	s = socket.socket()         #create a socket object
	host = "toby-linux"         #get local machine name
	port = int(sys.argv[1])     #reserve a port for your service.
	s.bind((host, port))        #bind to the port

	print("Listening on port: %s" % str(port))

	s.listen(5)                 #now wait for client connection.
	c, addr = s.accept()        #establish connection with client.

	print("Received request from: " + str(addr))

	sendData(c)
