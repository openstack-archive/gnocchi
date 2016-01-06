from gnocchi  import carbonara
import datetime

sampling = 1
points = 100000
ts = carbonara.TimeSerie(
    timestamps=map(datetime.datetime.utcfromtimestamp, xrange(points)),
    values=xrange(points))
agg = carbonara.AggregatedTimeSerie(sampling=sampling)
agg.update(ts)

grouped_points = list(agg.split())
