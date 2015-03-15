from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = "156509990-2OENzM4s6crQlUI9V6geS54tGEN0pEQVQ7Pvb2Xr"
access_token_secret = "szk6SWcmyRSSW5ECQRKyFyFkERj62cU1WRdydPSdMP8EU"
consumer_key = "FvHpVtiweeZ1Db4Q89AtXYmHE"
consumer_secret = "vMZ0Ek28yv3jcnjVh4GDwMvAwPbyuT0VsA5qJNAYxDzP8iirnX"

#This is a basic listener that just prints received tweets stdout
class StdOutListener(StreamListener):

	def on_data(self, data):
		print data
		return True

	def on_error(self, status):
		print status


if __name__ == "__main__":

	#This handles Twitter authentification and the connection to Twitter Streaming API
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)

	# This line filter Twitter Streams to capture data by the keywords
	stream.filter(languages='en')