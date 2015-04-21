import psycopg2
import json
import os
from sys import stdout
import datetime
import numpy as np
from sklearn.cluster import MeanShift, estimate_bandwidth
from sklearn.datasets.samples_generator import make_blobs

conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
cur = conn.cursor()

#cur.execute("SELECT latitude, longitude FROM tweets LIMIT 200;")
cur.execute("SELECT t.latitude, t.longitude " +
			"FROM hashtags h, tweet_hashtags th, tweets t " +
			"WHERE t.tweet_id = th.tweet_id and " +
			      "h.hashtag_id = th.hashtag_id and " +
			      "lower(h.hashtag) = 'isis';");
output = cur.fetchall()

X = np.array([[1.,1.]])

iterator = 1
for lat, lon in output:
	if (20 < lat < 55) and (-130 < lon < -45):
		X=np.insert(X, 0, [float(lon), float(lat)], axis=0)
	else:
		iterator = iterator+1
		continue
	output = '=== Number of Tweets processed: {0} ==='.format(iterator)
	stdout.write("\r%s"%output)
	stdout.flush()
	iterator = iterator+1

X=np.delete(X,len(X)-1,0)

cur.close()
conn.close()

print
print "Calculating clusters now..."

bandwidth = estimate_bandwidth(X, quantile=0.15)

ms = MeanShift(bandwidth, bin_seeding=True)
ms.fit(X)
labels = ms.labels_
cluster_centers = ms.cluster_centers_

labels_unique = np.unique(labels)
n_clusters_ = len(labels_unique)

print len(cluster_centers)

print("Number of estimated clusters: %d"%n_clusters_)

###############################################################################
# Plot result
import matplotlib.pyplot as plt
from itertools import cycle

plt.figure(1)
plt.clf()

colors = cycle('bgrcmykbgrcmykbgrcmykbgrcmyk')
for k, col in zip(range(n_clusters_), colors):
    my_members = labels == k
    cluster_center = cluster_centers[k]
    plt.plot(X[my_members, 0], X[my_members, 1], col + '.')
    plt.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
             markeredgecolor='k', markersize=14)
plt.title('Estimated number of clusters: %d' % n_clusters_)
plt.show()
