#!/usr/bin/env python
#
# Copyright 2012 New Dream Network, LLC (DreamHost)
#
# Author: Doug Hellmann <doug.hellmann@dreamhost.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Command line tool for creating test data for Gnocchi.

Usage:

Generate testing data for e.g. for default time span

source .tox/py27/bin/activate
./tools/make_test_data.py --user %userid --project %projectid
"""
from __future__ import print_function

import argparse
import datetime
import logging
import random
import sys
import time
import uuid

from oslo.config import cfg
from oslo.utils import timeutils

from ceilometer.publisher import utils
from ceilometer import sample
from gnocchi.ceilometer import dispatcher


def dump_data(log_file, timedata):
    with open(log_file, 'w') as f:
        for timelog in timedata:
            f.write("%s\n" % " ".join(str(t) for t in timelog))


RESOURCE_METADATA = {'instance': {'host': 'localhost',
                                  'image_ref_url': 'image_ref',
                                  'display_name': 'gnocchi_test_instance',
                                  'flavor': {'id': "42"}}}


def prepare_resource_metadata(resource_type='instance'):
    return RESOURCE_METADATA.get(resource_type, {})


def timeit(func, *args, **kwargs):
    start = time.time()
    func(*args, **kwargs)
    stop = time.time()
    return stop - start


def make_test_data(conn, name, meter_type, unit, volume, random_min,
                   random_max, user_id, project_id, start,
                   end, interval, log_file,
                   source='artificial',
                   batch_size=1, resource_count=10,):
    # Compute start and end timestamps for the new data.
    if isinstance(start, datetime.datetime):
        timestamp = start
    else:
        timestamp = timeutils.parse_strtime(start)

    if not isinstance(end, datetime.datetime):
        end = timeutils.parse_strtime(end)

    increment = datetime.timedelta(minutes=interval)

    # Generate events
    batch_num = 0
    total_volume = volume
    batch = []
    resource_metadata = prepare_resource_metadata(name)

    resources = [uuid.uuid4().hex for _ in xrange(resource_count)]
    logdata = []

    while timestamp <= end:
        if random_min >= 0 and random_max >= 0:
            # If there is a random element defined, we will add it to
            # user given volume.
            if isinstance(random_min, int) and isinstance(random_max, int):
                total_volume += random.randint(random_min, random_max)
            else:
                total_volume += random.uniform(random_min, random_max)

        resource = resources[random.randint(0, len(resources) - 1)]
        smpl = sample.Sample(name=name,
                             type=meter_type,
                             unit=unit,
                             volume=total_volume,
                             user_id=user_id,
                             project_id=project_id,
                             resource_id=resource,
                             timestamp=timestamp,
                             resource_metadata=resource_metadata,
                             source=source)
        data = utils.meter_message_from_counter(
            smpl, cfg.CONF.publisher.metering_secret)
        if len(batch) >= batch_size:
            deltatime = timeit(conn.record_metering_data, batch)
            logdata.append((batch_num, deltatime, len(batch)))
            batch_num += 1
            print("({batch_num} with {samples_count} "
                  "was written by {time}".format(batch_num=batch_num,
                                                 samples_count=len(batch),
                                                 time=deltatime))
            batch = []

        data['timestamp'] = data['timestamp'].isoformat()
        batch.append(data)

        timestamp = timestamp + increment
        if meter_type == 'gauge' or meter_type == 'delta':
            # For delta and gauge, we don't want to increase the value
            # in time by random element. So we always set it back to
            # volume.
            total_volume = volume
    dump_data(log_file, logdata)
    print('Added %d new samples for meter %s.' % (batch_num, name))


def main():
    parser = argparse.ArgumentParser(
        description='generate metering data',
    )
    parser.add_argument(
        '--interval',
        default=10,
        type=int,
        help='The period between events, in minutes.',
    )
    parser.add_argument(
        '--start',
        default=31,
        type=int,
        help='The number of days in the past to start timestamps.',
    )
    parser.add_argument(
        '--end',
        default=2,
        type=int,
        help='The number of days into the future to continue timestamps.',
    )
    parser.add_argument(
        '--type',
        choices=('gauge', 'cumulative'),
        default='gauge',
        help='Counter type.',
    )
    parser.add_argument(
        '--unit',
        default='instance',
        help='Counter unit.',
    )
    parser.add_argument(
        '--random_min',
        help='The random min border of amount for added to given volume.',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--random_max',
        help='The random max border of amount for added to given volume.',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--batch_size',
        help='Samples in batch.',
        type=int,
        default=100,
    )
    parser.add_argument(
        '--resource_count',
        help='The resources count',
        type=int,
        default=20
    )
    parser.add_argument(
        '--counter',
        default='instance',
        help='The counter name for the meter data.',
    )
    parser.add_argument(
        '--volume',
        help='The amount to attach to the meter.',
        type=int,
        default=1,
    )
    parser.add_argument(
        '--log-file',
        dest='log_file',
        help='File to logging time',
        default='/tmp/gnocchi_dispatcher.log',
    )
    parser.add_argument(
        '--gnocchi-url',
        dest='url',
        help='Gnocchi api',
        default="http://localhost:8041",
    )
    parser.add_argument(
        '--project-id',
        help='Project id of owner.',
        required=True,
    )
    parser.add_argument(
        '--user-id',
        help='User id of owner.',
        required=True,
    )

    args = parser.parse_args()

    cfg.CONF([], project='gnocchi')
    cfg.CONF.dispatcher_gnocchi.url = args.url



    # Set up logging to use the console
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    root_logger = logging.getLogger('')
    root_logger.addHandler(console)
    root_logger.setLevel(logging.DEBUG)

    # Connect to the Gnocchi dispatcher
    conn = dispatcher.GnocchiDispatcher(cfg.CONF)

    # Compute the correct time span
    start = datetime.datetime.utcnow() - datetime.timedelta(days=args.start)
    end = datetime.datetime.utcnow() + datetime.timedelta(days=args.end)
    make_test_data(conn=conn,
                   name=args.counter,
                   meter_type=args.type,
                   unit=args.unit,
                   volume=args.volume,
                   random_min=args.random_min,
                   random_max=args.random_max,
                   user_id=args.user_id,
                   project_id=args.project_id,
                   start=start,
                   end=end,
                   interval=args.interval,
                   source='artificial',
                   resource_count=args.resource_count,
                   batch_size=args.batch_size,
                   log_file=args.log_file)

    return 0


if __name__ == '__main__':
    main()
