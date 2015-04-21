import psycopg2
import matplotlib.pyplot as plt
from matplotlib.dates import AutoDateFormatter, AutoDateLocator, DateFormatter
from datetime import datetime, timedelta
import pytz
import time

time1 = time.time()
conn = psycopg2.connect("dbname='twitter' user='tsg9292' host='localhost' password='post'")
cur = conn.cursor()

cur.execute("select distinct on (created_at) created_at from \
			(select created_at from tweets order by tweet_id) subq;")

output = cur.fetchall()
#print output

cur.close()
conn.close()
time2 = time.time()
print "Time to complete database query: {}".format(time2-time1)

local_tz = pytz.timezone('US/Mountain')

date_array = [datetime.strptime(string[0], "%a %b %d %H:%M:%S +0000 %Y")\
	.replace(tzinfo=pytz.utc).astimezone(local_tz) for string in output]



print "Number of unique timestamps: {}".format(len(date_array))

y = [1 for i in date_array]

time3 = time.time()
print "Time to complete array formatting: {}".format(time3-time2)

minutes = AutoDateLocator()
minutesFmt = DateFormatter("%m/%d %I:%M:%S%p", tz=local_tz)

fig, ax = plt.subplots()
plt.title("Timestamps of Twitter data collected")

ax.plot(date_array, y, 'k|')
ax.xaxis.set_major_locator(minutes)
ax.xaxis.set_major_formatter(minutesFmt)
ax.yaxis.set_ticks([0,1,2])
ax.grid(True)


#ax.autoscale_view()
fig.autofmt_xdate()

time4 = time.time()
print "Time to create the plot: {}".format(time4-time3)

print "Total runtime: {}".format(time4-time1)
print

plt.show()
 