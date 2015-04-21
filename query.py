import psycopg2
import json
import os
from sys import stdout

if __name__ == '__main__':
	conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
	cur = conn.cursor()
	
	one=0
	two=0
	three=0
	four=0
	five=0
	six=0
	seven=0
	eight=0
	nine=0
	ten=0
	for i in range(1,2):
		cur.execute("SELECT COUNT(*) from tweet_hashtags where hashtag_id = %i" % i)

	conn.commit()
	print

	cur.close()
	conn.close()