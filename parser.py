import json
import os
import time

def find_coordinates():
	start=time.time()
	iter_time = 5
	with open('json_tweets1') as f:
		for line in f:
			json_obj = json.loads(line)
			lat,lon = json_obj['coordinates']['coordinates']
			f = open('gps.csv','a')
			f.write('{0},{1}\n'.format(lat,lon))
			f.close()
			stop = time.time()
			if stop-start > iter_time:
				iter_time = iter_time+5
				print('.'),

def main():
	f = open('gps.csv','w')
	f.write('lat,lon\n')
	f.close()
	find_coordinates()



if __name__ == '__main__':
	main()