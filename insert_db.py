import psycopg2
import json
import os
from sys import stdout

def insert_tweets():
	conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
	cur = conn.cursor()
	with open('json_tweets') as f:
		iterator = 1
		for line in f:
			json_obj = json.loads(line)
			
			tweet_id = json_obj['id']
			
			date = json_obj['created_at']
			date = repr(date).strip('u')

			tweet_text = json_obj['text']
			tweet_text = repr(tweet_text).strip('u').strip('/"').strip("/'")
			tweet_text = tweet_text.replace("'","''")
			tweet_text = "'"+tweet_text+"'"

			coord_type = json_obj['coordinates']['type']
			coord_type = repr(coord_type).strip('u')

			coords = json_obj['coordinates']['coordinates']
			lat,lon = coords[1], coords[0]


			name, place_type, country, place_id = '', '', '', ''
			if json_obj['place'] != None:
				name = json_obj['place']['full_name']
				place_type = json_obj['place']['place_type']
				country = json_obj['place']['country']
				place_id = json_obj['place']['id']

				place_type = repr(place_type).strip('u')
				country = repr(country).strip('u')
				place_id = repr(place_id).strip('u')

				name = repr(name).strip('u').strip('/"').strip("/'")
				name = name.replace("'","''")
				name = "'"+name+"'"

			user_id = json_obj['user']['id']
			user_id = repr(user_id).strip('u')

			if (iterator %1000  == 0):
				output = ''
				if iterator < 1000:
					output = '==== Number of Tweets inserted into the database: {} ===='.format(iterator)
				if iterator >= 1000 and iterator < 1000000:
					output = '==== Number of Tweets inserted into the database: {} thousand ===='.format(iterator/1000)
				if iterator >= 1000000:
					num = "{0:.2f}".format(float(iterator)/1000000)
					output = '=== Number of Tweets inserted into the database: {} million ===='.format(num)
				stdout.write("\r%s"%output)
				stdout.flush()
			iterator = iterator+1	

			sql = "INSERT INTO tweets (tweet_id, created_at, message, coord_type, latitude, longitude, place_name, place_type, country, place_id, user_id) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10});".format(
								tweet_id, 
								date, 
								tweet_text,
								coord_type,
								lat,
								lon,
								name,
								place_type,
								country,
								place_id,
								user_id)
			if name == '':
				sql = "INSERT INTO tweets (tweet_id, created_at, message, coord_type, latitude, longitude, user_id) VALUES ({0},{1},{2},{3},{4},{5},{6});".format(
									tweet_id,
									date,
									tweet_text,
									coord_type,
									lat,
									lon,
									user_id)
			try:
				cur.execute(sql)
			except psycopg2.IntegrityError:
				conn.rollback()
			else:
			 conn.commit()


	cur.execute("SELECT COUNT(*) FROM tweets;")

	print cur.fetchall()

	conn.commit()
	print
	print 'Database injection successful'

	cur.close()
	conn.close()

def insert_hashtags():
	conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
	cur = conn.cursor()
	with open('json_tweets') as f:
		iterator = 1
		for line in f:
			json_obj = json.loads(line)
			hashtags = json_obj['entities']['hashtags']
			if hashtags != []:
				for tag in hashtags:
					hashtag = tag['text']
					hashtag = repr(hashtag).strip('u')
					cur.execute("SELECT hashtag_id from hashtags where hashtag = %s;"% hashtag)

					output = cur.fetchall()
					if output == []:
						cur.execute("SELECT MAX(hashtag_ID) from hashtags;")
						hashtag_id = cur.fetchall()[0][0]+1
						print 'no hashtag in db... inserting hashtag_id: ' + str(hashtag_id)
						sql = "INSERT INTO hashtags (hashtag_id, hashtag) VALUES ({0}, {1});".format(
													hashtag_id,
													hashtag)
						cur.execute(sql)
						conn.commit()
					

			if (iterator %1000  == 0):
				
				output = ''
				if iterator < 1000:
					output = '==== Number of Tweets inserted into the database: {} ===='.format(iterator)
				if iterator >= 1000 and iterator < 1000000:
					output = '==== Number of Tweets inserted into the database: {} thousand ===='.format(iterator/1000)
				if iterator >= 1000000:
					num = "{0:.2f}".format(float(iterator)/1000000)
					output = '=== Number of Tweets inserted into the database: {} million ===='.format(num)
				stdout.write("\r%s"%output)
				stdout.flush()
			
			iterator = iterator+1		
	cur.close()
	conn.close()

	print
	print 'Hashtag injection successful!'

def insert_tweet_hashtags():
	conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
	cur = conn.cursor()
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
					cur.execute("SELECT hashtag_id from hashtags where hashtag = %s;"% hashtag)
					hashtag_id = cur.fetchall[0][0]

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
	#insert_tweets()
	insert_hashtags()
	#insert_tweet_hashtags()