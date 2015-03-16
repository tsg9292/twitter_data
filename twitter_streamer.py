from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json

#Variables that contains the user credentials to access Twitter API
access_token = "156509990-2OENzM4s6crQlUI9V6geS54tGEN0pEQVQ7Pvb2Xr"
access_token_secret = "szk6SWcmyRSSW5ECQRKyFyFkERj62cU1WRdydPSdMP8EU"
consumer_key = "FvHpVtiweeZ1Db4Q89AtXYmHE"
consumer_secret = "vMZ0Ek28yv3jcnjVh4GDwMvAwPbyuT0VsA5qJNAYxDzP8iirnX"

language_dict={}

#This is a basic listener that just prints received tweets stdout
class StdOutListener(StreamListener):

	def on_data(self, data):
		new_data = {}
		json_data = json.loads(data)
		if 'geo' not in data:
			return True
		if json_data['coordinates']:
			if json_data['lang'] == 'en':
				#TODO fix quotes here. Need to all be double quotes except for in the text field
				new_data = "{'created_at':"
				new_data = new_data + json.dumps(json_data['created_at'],separators=(',',':')) + ','
				new_data = new_data + "'id':" + json.dumps(json_data['id'],separators=(',',':')) + ','
				new_data = new_data + "'text':" + json.dumps(json_data['text'],separators=(',',':')) + ','
				new_data = new_data + "'coordinates':" + json.dumps(json_data['coordinates'],separators=(',',':')) + ','
				new_data = new_data + "'place':" + json.dumps(json_data['place'],separators=(',',':')) + ','
				new_data = new_data + "'entities':" + json.dumps(json_data['entities'],separators=(',',':')) + ','
				new_data = new_data + "'user':{'id':" + json.dumps(json_data['user']['id'],separators=(',',':')) + '}'
				new_data = new_data + "}"

		print
		print new_data
		#if new_data != {}:
		#		print json.loads(new_data)

		return True

	def on_error(self, status_code):
		if status_code == 406:
			print "Warning: unsupported keyword, returning..."
		else:
			print status_code
		return False

if __name__ == "__main__":

	#This handles Twitter authentification and the connection to Twitter Streaming API
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)

	# This line filter Twitter Streams to capture data by the keywords
	stream.filter(locations=[-126.00,25.00,-50.00,50.00])
	