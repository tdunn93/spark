import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = 'KSzFTRL8VHTHK3C0bZLdjlsw2'
consumer_secret = 'h1EaYUaRfS2rbdJnCdBqRWOY64kNFYbGM6TZu0x7D7cWoKAHRo'
access_token = '331544155-yBNZkRXGZeyNeBFZz67SIC8paPL0D4BOpFk2x8bV'
access_secret = 'M79hmuTKn94Yha0nlYAWFa1vGuHK7Xz6OLjK9G6OMpgbP'

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
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
  twitter_stream.filter(track=['trump'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "toby-linux"      # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
