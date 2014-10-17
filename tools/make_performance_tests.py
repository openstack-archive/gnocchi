# Copyright (c) 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
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

LOG_DATA = {}
ENTITIES = []


def dump_data(dir):
    for name, data in LOG_DATA.items():
        with open("%s/gnocchi_%s.log" % (dir, name), 'w') as f:
            for meter in data:
                f.write("%s\n" % " ".join("%.2f" % m for m in meter))
    LOG_DATA.clear()


def append_log_data(log_name, index, time, count):
    if log_name not in LOG_DATA:
        LOG_DATA[log_name] = []
    if count != 0 and count is not None:
        LOG_DATA[log_name].append([index, time, count, time / count])


class TimedCalling():
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


@TimedCalling('entities')
def _write_entity(index, token, url, archive_policy):
    entity_url = url + "/v1/entity"
    data = json.dumps({"archive_policy": archive_policy})
    headers = None
    if token:
        headers = {'X-Auth-Token': token}
    resp = requests.post(entity_url,
                         data=data,
                         headers=headers)
    try:
        if resp.content:
            response_content = json.loads(resp.content)
            ENTITIES.append(response_content["id"])
    except Exception:
        print 'Failed in writing %s entity' % index
        return 0
    else:
        print '%s entity has been written'
        return 1


@TimedCalling('measures_post')
def _write_measures(index, token, url, entity, timestamp, interval, size=100):
    measures_url = url + "/v1/entity/%s/measures" % entity
    data = []
    for i in range(size):
        timestamp += interval
        data.append({'timestamp': timestamp.isoformat(),
                     'value': 100})
    headers = None
    if token:
        headers = {'X-Auth-Token': token}
    resp = requests.post(measures_url,
                         data=json.dumps(data),
                         headers=headers)
    if resp.status_code / 200 != 2:
        print 'Failed in writing %s measures batch' % index
        return 0
    print '%s measures batch has been written' % index
    return size


@TimedCalling('measures_get')
def _get_measures(index, token, url, entity):
    measures_url = url + "/v1/entity/%s/measures" % entity
    headers = None
    if token:
        headers = {'X-Auth-Token': token}
    resp = requests.get(measures_url,
                        headers=headers)
    try:
        print '%s measures GET' % index
        return len(json.loads(resp.content))
    except Exception:
        print '%s measures GET failed' % index
        return 0


class TokenManager(object):
    def __init__(self, user, password, tenant, auth_url, need_auth=True):
        self.need_auth = need_auth
        self.user = user
        self.password = password
        self.tenant = tenant
        self.auth_url = auth_url
        self.keystone_client = None

    @property
    def token(self):
        if not self.need_auth:
            return None
        if not keystone_client:
            self.keystone_client = self._get_client()
        try:
            self.keystone_client.authenticate()
        except Exception:
            self.keystone_client = self._get_client()
        return self.keystone_client.auth_token

    def _get_client(self):
        client = keystone_client.Client(
            username=self.user,
            password=self.password,
            tenant_name=self.tenant,
            auth_url=self.auth_url)
        return client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity-count",
                        default=100,
                        type=int)
    parser.add_argument("--measure-count",
                        default=100,
                        type=int)
    parser.add_argument("--gnocchi-url",
                        default="http://localhost:8041")
    parser.add_argument("--archive-policy",
                        default="low")
    parser.add_argument("--os-username",
                        dest='username',
                        default="admin")
    parser.add_argument("--os-tenant-name",
                        dest='tenant_name',
                        default="admin")
    parser.add_argument("--os-password",
                        dest='password',
                        default="nova")
    parser.add_argument("--os-auth-url",
                        dest='auth_url',
                        default="http://localhost:5000/v2.0")
    parser.add_argument("--result-dir",
                        dest='dir',
                        default="/tmp/")
    parser.add_argument("--need-authenticate",
                        dest='need_auth',
                        default=True,
                        type=bool)
    args = parser.parse_args()

    try:
        token_manager = TokenManager(args.username, args.password,
                                     args.tenant_name, args.auth_url,
                                     args.need_auth)
        for index in range(args.entity_count):
            _write_entity(index, token_manager.token, args.gnocchi_url,
                          args.archive_policy)

        start_timestamp = datetime.datetime.utcnow()
        interval = datetime.timedelta(minutes=1)
        for index in range(args.measure_count):
            entity = ENTITIES[random.randint(0, len(ENTITIES) - 1)]
            _write_measures(index, token_manager.token, args.gnocchi_url,
                            entity, start_timestamp, interval)
            _get_measures(index, token_manager.token, args.gnocchi_url, entity)
    except Exception:
        raise
    finally:
        dump_data(args.dir)


if __name__ == '__main__':
    main()
