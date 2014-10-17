# Copyright (c) 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import datetime
import json
import random
import requests
import time

from keystoneclient.v2_0 import client as keystone_client
import six

LOG_DATA = {}
ENTITIES = []


def dump_data(dir):
    for name, data in LOG_DATA.items():
        with open("%s/gnocchi_%s.log" % (dir, name), 'w') as f:
            f.write("Index\tTime\tCount\n")
            for meter in data:
                f.write("%s\n" % "\t".join("%.2f" % m for m in meter))
    LOG_DATA.clear()


def append_log_data(log_name, index, time, count):
    if log_name not in LOG_DATA:
        LOG_DATA[log_name] = []
    if count:
        LOG_DATA[log_name].append([index, time, count])
        print(("{name} #{index} processed "
               "{count} objects by {time} sec").format(name=log_name,
                                                       index=index,
                                                       count=count,
                                                       time=time))


class TimedCalling(object):
    def __init__(self, log_name):
        self.log_name = log_name

    def __call__(self, func):
        def wrapper(index, *args, **kwargs):
            start_ts = time.time()
            count = func(index, *args, **kwargs)
            end_ts = time.time()
            append_log_data(self.log_name, index, end_ts - start_ts, count)
            return count

        return wrapper


@TimedCalling('entities_post')
def _write_entity(index, keystone, url, archive_policy):
    entity_url = url + "/v1/entity"
    data = json.dumps({"archive_policy": archive_policy})
    headers = None
    if keystone:
        headers = {'X-Auth-Token': keystone.auth_token}
    resp = requests.post(entity_url,
                         data=data,
                         headers=headers)
    try:
        if resp.content:
            response_content = json.loads(resp.content)
            ENTITIES.append(response_content["id"])
        else:
            return 0
    except Exception:
        print("Failed creating entity #%d" % index)
        return 0
    else:
        return 1


@TimedCalling('measures_post')
def _write_measures(index, keystone, url, entity, timestamp, interval,
                    batch_size=100):
    measures_url = url + "/v1/entity/%s/measures" % entity
    data = []
    for i in six.range(batch_size):
        timestamp += interval
        data.append({'timestamp': timestamp.isoformat(),
                     'value': 100})
    headers = None
    if keystone:
        headers = {'X-Auth-Token': keystone.auth_token}
    resp = requests.post(measures_url,
                         data=json.dumps(data),
                         headers=headers)
    if resp.status_code / 100 != 2:
        print('Failed in writing measures batch #%s' % index)
        return 0
    return batch_size


@TimedCalling('measures_get')
def _get_measures(index, keystone, url, entity):
    measures_url = url + "/v1/entity/%s/measures" % entity
    headers = None
    if keystone:
        headers = {'X-Auth-Token': keystone.auth_token}
    resp = requests.get(measures_url,
                        headers=headers)
    try:
        return len(json.loads(resp.content))
    except Exception:
        print('Failed GET request to measures #s' % index)
        print(resp.status_code)
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity-count",
                        help=('Number of entities to be created. '
                              'Entities are created one by one.'),
                        default=100,
                        type=int)
    parser.add_argument("--measure-count",
                        help='Number of measures batches to be sent.',
                        default=100,
                        type=int)
    parser.add_argument("--gnocchi-url",
                        help='Gnocchi API URL to use.',
                        default="http://localhost:8041")
    parser.add_argument("--archive-policy",
                        help='Archive policy to use.',
                        default="low")
    parser.add_argument("--os-username",
                        dest='username',
                        help='User name to use for OpenStack service access.',
                        default="admin")
    parser.add_argument("--os-tenant-name",
                        dest='tenant_name',
                        help=('Tenant name to use for '
                              'OpenStack service access.'),
                        default="admin")
    parser.add_argument("--os-password",
                        dest='password',
                        help='Password to use for OpenStack service access.',
                        default="nova")
    parser.add_argument("--os-auth-url",
                        dest='auth_url',
                        help='Auth URL to use for OpenStack service access.',
                        default="http://localhost:5000/v2.0")
    parser.add_argument("--result-dir",
                        help='Directory to write results to.',
                        dest='dir',
                        default="/tmp/")
    parser.add_argument("--need-authenticate",
                        dest='need_auth',
                        help=('Boolean option that defines if we need to '
                              'authenticate.'),
                        default=True,
                        type=bool)
    parser.add_argument("--batch-size",
                        dest='batch_size',
                        help='Number of measurements in the batch.',
                        default=100, )
    args = parser.parse_args()

    try:
        keystone = None
        if args.need_auth:
            keystone = keystone_client.Client(username=args.username,
                                              password=args.password,
                                              tenant_name=args.tenant_name,
                                              auth_url=args.auth_url,
                                              force_new_token=True)
            # Delete _auth_token and use auth_ref further
            del keystone.auth_token

        for index in six.range(args.entity_count):
            _write_entity(index, keystone, args.gnocchi_url,
                          args.archive_policy)

        start_timestamp = datetime.datetime.utcnow()
        interval = datetime.timedelta(minutes=1)
        for index in six.range(args.measure_count):
            entity = ENTITIES[random.randint(0, len(ENTITIES) - 1)]
            _write_measures(index, keystone, args.gnocchi_url,
                            entity, start_timestamp, interval, args.batch_size)
            _get_measures(index, keystone, args.gnocchi_url, entity)
    finally:
        dump_data(args.dir)


if __name__ == '__main__':
    main()
