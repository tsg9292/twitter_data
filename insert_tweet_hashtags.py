import psycopg2
import json
import os
from sys import stdout
import datetime

def insert_tweet_hashtags():
	conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
	cur = conn.cursor()
	
	cur.execute("SELECT * FROM hashtags;")
	output = cur.fetchall()
	dictionary = {}
	for i in range(len(output)):
		dictionary[output[i][1]] = output[i][0]

	print "Size of dict: {}".format(len(dictionary))

	with open('json_tweets') as f:
		iterator = 1
		iterator2 = 0
		for line in f:
			json_obj = json.loads(line)
			tweet_id = json_obj['id']	

			hashtags = json_obj['entities']['hashtags']
			if hashtags != []: # hashtags exist for the tweet
				for tag in hashtags:
					hashtag = tag['text']
					hashtag = repr(hashtag).strip('u')
					try:
						hashtag_id = dictionary[hashtag.strip("/'")]
					except KeyError:
						print "KeyError: {}".format(hashtag)
						cur.execute("SELECT MAX(hashtag_id) from hashtags;")
						hashtag_id = cur.fetchall()[0][0]+1
						cur.execute("INSERT INTO hashtags (hashtag_id, hashtag) values (%i, %s)"% (hashtag_id, hashtag))
						dictionary[hashtag] = hashtag_id


					sql = "INSERT INTO tweet_hashtags (tweet_id, hashtag_id) VALUES ({0}, {1})".format(
									tweet_id,
									hashtag_id)
					try:
						cur.execute(sql)
					except psycopg2.IntegrityError:
						conn.rollback()
					else:
			 			conn.commit()
						iterator2 = iterator2+1

			if (iterator %1000  == 0):
				output = ''
				if iterator < 1000:
					output = '==== Number of Tweets processed: {0}  Hashtags inserted: {1} ===='.format(iterator, iterator2)
				if iterator >= 1000 and iterator < 1000000:
					output = '==== Number of Tweets processed: {0} thousand  Hashtags inserted: {1} ===='.format(iterator/1000, iterator2)
				if iterator >= 1000000:
					num = "{0:.2f}".format(float(iterator)/1000000)
					output = '=== Number of Tweets processed: {0} million  Hashtags inserted: {1} ===='.format(num, iterator2)
				stdout.write("\r%s"%output)
				stdout.flush()
			iterator = iterator+1					

	cur.close()
	conn.close()
	print 
	print 'tweet_hashtags table inserted successfully!'
	
if __name__ == '__main__':
	insert_tweet_hashtags()

