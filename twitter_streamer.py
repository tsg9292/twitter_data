from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import os
from sys import stdout

#Variables that contains the user credentials to access Twitter API
access_token = "156509990-2OENzM4s6crQlUI9V6geS54tGEN0pEQVQ7Pvb2Xr"
access_token_secret = "szk6SWcmyRSSW5ECQRKyFyFkERj62cU1WRdydPSdMP8EU"
consumer_key = "FvHpVtiweeZ1Db4Q89AtXYmHE"
consumer_secret = "vMZ0Ek28yv3jcnjVh4GDwMvAwPbyuT0VsA5qJNAYxDzP8iirnX"

#This is a basic listener that just prints received tweets stdout
class StdOutListener(StreamListener):

	def on_data(self, data):
		if os.path.isfile('middle_east_meta'):
			with open('middle_east_meta') as f:
				line = f.read()
				tweet_count,fsize = line.split(',')
				tweet_count = int(tweet_count)
				fsize = int(fsize)
		else:
			tweet_count = 0
			fsize = 0

		new_data = {}
		json_data = json.loads(data)
		if 'geo' not in data:
			return True
		if json_data['coordinates']:
			if json_data['lang'] == 'en':
				#TODO fix quotes here. Need to all be double quotes except for in the text field
				new_data = "{\"created_at\":" \
					+ json.dumps(json_data['created_at'],separators=(',',':')) + ',' \
					+ "\"id\":" + json.dumps(json_data['id'],separators=(',',':')) + ',' \
					+ "\"text\":" + json.dumps(json_data['text'],separators=(',',':')) + ',' \
					+ "\"coordinates\":" + json.dumps(json_data['coordinates'],separators=(',',':')) + ',' \
					+ "\"place\":" + json.dumps(json_data['place'],separators=(',',':')) + ',' \
					+ "\"entities\":" + json.dumps(json_data['entities'],separators=(',',':')) + ',' \
					+ "\"user\":{\"id\":" + json.dumps(json_data['user']['id'],separators=(',',':')) + '}' \
				+ "}"

		if new_data != {}:
			f2 = open('middle_east_tweets','a')
			if fsize%1000 <= 25 and fsize%1000 >=1:
				output='===== Number of Tweets: {0}  File size: {1} ====='.format(tweet_count, fsize)
				stdout.write("\r%s"%output)
				stdout.flush()

			# if the file size if bigger than ~15Gb, exit
			if fsize >= 16106127360:
				return False
		
			f2.write(new_data+'\n')
			tweet_count = tweet_count + 1
			
			f2.close()
			fsize = os.path.getsize('middle_east_tweets')
			with open('middle_east_meta','w') as f1:
					f1.write('{0},{1}\n'.format(tweet_count,fsize))
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
	#				[min(long),min(lat),max(long),max(lat)]
	stream.filter(locations=[-126.00,25.00,-50.00,50.00])
	