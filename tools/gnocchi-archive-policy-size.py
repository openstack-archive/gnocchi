#!/usr/bin/env python
import sys

from gnocchi import utils


WORST_CASE_BYTES_PER_POINT = 8.04


if (len(sys.argv) - 1) % 2 != 0:
    print("Usage: %s <granularity> <timespan> ... <granularity> <timespan>"
          % sys.argv[0])
    sys.exit(1)


def sizeof_fmt(num, suffix='B'):
    for unit in ('','Ki','Mi','Gi','Ti','Pi','Ei','Zi'):
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


size = 0
for g, t in utils.grouper(sys.argv[1:], 2):
    granularity = utils.to_timespan(g)
    timespan = utils.to_timespan(t)
    points = timespan.total_seconds() / granularity.total_seconds()
    cursize = points * WORST_CASE_BYTES_PER_POINT
    size += cursize
    print("%s over %s = %d points = %s" % (g, t, points,sizeof_fmt(cursize)))

print("Total: " + sizeof_fmt(size))
