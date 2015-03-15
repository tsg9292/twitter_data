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
		json_data = json.loads(data)
		if 'geo' not in data:
			return True
		if json_data['geo']:
			f = open('geo_lang.csv','a')
			lon, lat = json_data['geo']['coordinates']
			lang = json_data['lang']
			string = '{0},{1},{2}\n'.format(lon,lat,lang)
			f.write(string)
			f.close()

			if lang in language_dict:
				language_dict[lang] = language_dict[lang]+1
			else:
				language_dict[lang] = 1

		f = open('language_stats.txt','w')
		f.write(str(language_dict))
		f.close()
		
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
	#stream.sample(languages=['es'])
	stream.filter(locations=[-126.00,25.00,-50.00,50.00])
	