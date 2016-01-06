import datetime
import sys

from gnocchi import carbonara

points = int(sys.argv[1])

ts = carbonara.TimeSerie(timestamps=map(datetime.datetime.fromtimestamp, xrange(points)), values=xrange(points))

agg = carbonara.AggregatedTimeSerie(sampling=1)

agg.update(ts)
print("%.2f Kb" % (len(agg.serialize()) / 1024.0))
print("Theory: %.2f Kb" % (points * 16 / 1024.0))
