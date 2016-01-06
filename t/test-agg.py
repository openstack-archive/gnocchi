import datetime

from gnocchi import carbonara

points = 10

ts = carbonara.TimeSerie.from_data(
    timestamps=map(datetime.datetime.fromtimestamp, xrange(points)),
    values=xrange(points))

ts2 = carbonara.TimeSerie.from_data(
    timestamps=map(datetime.datetime.fromtimestamp, xrange(points, points * 2)),
    values=xrange(points, points * 2))

x = ts.ts + ts2.ts
