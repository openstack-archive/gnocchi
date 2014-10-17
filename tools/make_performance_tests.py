import argparse
import datetime
import json
import random
import requests
import threading
import time

from keystoneclient.openstack.common.apiclient import exceptions
from keystoneclient.v2_0 import client as keystone_client

LOG_DATA = {}
ENTITIES = []
RUNNING = True

def dump_data(sleeptime=600):
    while RUNNING:
        time.sleep(sleeptime)
        for name, data in LOG_DATA.items():
            with open("/tmp/gnocchi_%s.log" % name, 'a') as f:
                for meter in data:
                    f.write("%s\n" % " ".join("%.2f" % m for m in meter))
        LOG_DATA.clear()


def append_log_data(log_name, index, time, count):
    if log_name not in LOG_DATA:
        LOG_DATA[log_name] = []
    if count != 0 and count is not None:
        LOG_DATA[log_name].append([index, time, count, time/count])


class TimedCalling():
    def __init__(self, log_name):
        self.log_name = log_name

    def __call__(self, func):
        def wrapper(index, *args, **kwargs):
            start_ts = time.time()
            count = func(index, *args, **kwargs)
            end_ts = time.time()
            append_log_data(self.log_name, index, end_ts-start_ts, count)
            return count
        return wrapper


@TimedCalling('entities')
def _write_entity(index, token, url, archive_policy):
    entity_url = url + "/v1/entity"
    data = json.dumps({"archive_policy": archive_policy})
    resp = requests.post(entity_url,
                         data=data,
                         headers={'X-Auth-Token': token})
    try:
        if resp.content:
            response_content = json.loads(resp.content)
            ENTITIES.append(response_content["id"])
    except Exception:
        return 0
    else:
        return 1


@TimedCalling('measures_post')
def _write_measures(index, token, url, entity, timestamp, interval, size=100):
    measures_url = url + "/v1/entity/%s/measures" % entity
    data = []
    for i in range(size):
        timestamp += interval
        data.append({'timestamp': timestamp.isoformat(),
                     'value': 100})
    resp = requests.post(measures_url,
                         data=json.dumps(data),
                         headers={'X-Auth-Token': token})
    if resp.status_code / 200 != 2:
        return 0
    return size


@TimedCalling('measures_get')
def _get_measures(index, token, url, entity):
    measures_url = url + "/v1/entity/%s/measures" % entity
    resp = requests.get(measures_url,
                        headers={'X-Auth-Token': token})
    try:
        return len(json.loads(resp.content))
    except Exception:
        return 0


class TokenManager(object):
    def __init__(self, user, password, tenant, auth_url):
        self.user = user
        self.password = password
        self.tenant = tenant
        self.auth_url = auth_url
        self._token = None

    @property
    def token(self):
        try:
            keystone_client.Client(token=self._token,
                                   auth_url=self.auth_url)
        except exceptions.Unauthorized:
            self._token = self._get_token()
        except exceptions.AuthorizationFailure:
            self._token = self._get_token()
        return self._token

    def _get_token(self):
        client = keystone_client.Client(
            username=self.user,
            password=self.password,
            tenant_name=self.tenant,
            auth_url=self.auth_url)
        return client.auth_token


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
    args = parser.parse_args()

    thread = threading.Thread(target=dump_data)
    thread.start()
    token = TokenManager(args.username, args.password,
                         args.tenant_name, args.auth_url)
    for index in range(args.entity_count):
        _write_entity(index, token.token, args.gnocchi_url,
                      args.archive_policy)
        print "Added %s entity" % index

    start_timestamp = datetime.datetime.utcnow()
    interval = datetime.timedelta(minutes=1)
    for index in range(args.measure_count):
        entity = ENTITIES[random.randint(0, len(ENTITIES) - 1)]
        _write_measures(index, token.token, args.gnocchi_url, entity,
                        start_timestamp, interval)
        print "Added %s measure batches" % index
        _get_measures(index, token.token, args.gnocchi_url, entity)
        print "Get %s measures batch" % index
    RUNNING = False

if __name__ == '__main__':
    main()
